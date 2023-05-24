//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-kafka-gsoc open source project
//
// Copyright (c) 2022 Apple Inc. and the swift-kafka-gsoc project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of swift-kafka-gsoc project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Crdkafka
import Dispatch
import Logging
import NIOCore

// TODO: deduplicate
/// `NIOAsyncSequenceProducerDelegate` implementation handling backpressure for ``KafkaProducer``.
private struct AcknowledgedMessagesAsyncSequenceDelegate: NIOAsyncSequenceProducerDelegate {
    let produceMoreClosure: @Sendable () -> Void
    let didTerminateClosure: @Sendable () -> Void

    func produceMore() {
        produceMoreClosure()
    }

    func didTerminate() {
        didTerminateClosure()
    }
}

/// `AsyncSequence` implementation for handling messages acknowledged by the Kafka cluster (``KafkaAcknowledgedMessage``).
public struct AcknowledgedMessagesAsyncSequence: AsyncSequence {
    public typealias Element = Result<KafkaAcknowledgedMessage, KafkaAcknowledgedMessageError>
    typealias HighLowWatermark = NIOAsyncSequenceProducerBackPressureStrategies.HighLowWatermark
    fileprivate let wrappedSequence: NIOAsyncSequenceProducer<Element, HighLowWatermark, AcknowledgedMessagesAsyncSequenceDelegate>

    /// `AsynceIteratorProtocol` implementation for handling messages received from the Kafka cluster (``KafkaConsumerMessage``).
    public struct ConsumerMessagesAsyncIterator: AsyncIteratorProtocol {
        fileprivate let wrappedIterator: NIOAsyncSequenceProducer<
            Element,
            HighLowWatermark,
            AcknowledgedMessagesAsyncSequenceDelegate
        >.AsyncIterator

        public mutating func next() async -> Element? {
            await self.wrappedIterator.next()
        }
    }

    public func makeAsyncIterator() -> ConsumerMessagesAsyncIterator {
        return ConsumerMessagesAsyncIterator(wrappedIterator: self.wrappedSequence.makeAsyncIterator())
    }
}

/// Send messages to the Kafka cluster.
/// Please make sure to explicitly call ``shutdownGracefully(timeout:)`` when the ``KafkaProducer`` is not used anymore.
/// - Note: When messages get published to a non-existent topic, a new topic is created using the ``KafkaTopicConfig``
/// configuration object (only works if server has `auto.create.topics.enable` property set).
public final class KafkaProducer {
    /// States that the ``KafkaProducer`` can have.
    private enum State {
        /// The ``KafkaProducer`` has started and is ready to use.
        case started
        /// ``KafkaProducer/shutdownGracefully()`` has been invoked and the ``KafkaProducer``
        /// is in the process of receiving all outstanding acknowlegements and shutting down.
        case shuttingDown
        /// The ``KafkaProducer`` has been shut down and cannot be used anymore.
        case shutDown
    }

    /// State of the ``KafkaProducer``.
    private var state: State

    /// Counter that is used to assign each message a unique ID.
    /// Every time a new message is sent to the Kafka cluster, the counter is increased by one.
    private var messageIDCounter: UInt = 0
    /// The ``KafkaTopicConfig`` used for newly created topics.
    private let topicConfig: KafkaTopicConfig
    /// A logger.
    private let logger: Logger
    /// Dictionary containing all topic names with their respective `rd_kafka_topic_t` pointer.
    private var topicHandles: [String: OpaquePointer]

    // We use implicitly unwrapped optionals here as these properties need to access self upon initialization
    /// Used for handling the connection to the Kafka cluster.
    private var client: KafkaClient!

    /// Serial queue used to run all blocking operations. Additionally ensures that no data races occur.
    private let serialQueue: DispatchQueue

    // We use implicitly unwrapped optionals here as these properties need to access self upon initialization
    /// Type of the values returned by the ``messages`` sequence.
    private var acknowledgementsSource: NIOAsyncSequenceProducer<
        AcknowledgedMessagesAsyncSequence.Element,
        AcknowledgedMessagesAsyncSequence.HighLowWatermark,
        AcknowledgedMessagesAsyncSequenceDelegate
    >.Source!
    /// `AsyncSequence` that returns all ``KafkaAcknowledgedMessage`` objects that the producer receives.
    public private(set) var acknowledgements: AcknowledgedMessagesAsyncSequence!

    /// Initialize a new ``KafkaProducer``.
    /// - Parameter config: The ``KafkaConfig`` for configuring the ``KafkaProducer``.
    /// - Parameter topicConfig: The ``KafkaTopicConfig`` used for newly created topics.
    /// - Parameter logger: A logger.
    /// - Throws: A ``KafkaError`` if the received message is an error message or malformed.
    public init(
        config: KafkaConfig = KafkaConfig(),
        topicConfig: KafkaTopicConfig = KafkaTopicConfig(),
        logger: Logger
    ) throws {
        self.topicConfig = topicConfig
        self.logger = logger
        self.topicHandles = [:]
        self.state = .started

        self.serialQueue = DispatchQueue(label: "swift-kafka.producer.serial")

        // (NIOAsyncSequenceProducer.makeSequence Documentation Excerpt)
        // This method returns a struct containing a NIOAsyncSequenceProducer.Source and a NIOAsyncSequenceProducer.
        // The source MUST be held by the caller and used to signal new elements or finish.
        // The sequence MUST be passed to the actual consumer and MUST NOT be held by the caller.
        // This is due to the fact that deiniting the sequence is used as part of a trigger to
        // terminate the underlying source.
        let backpressureStrategy = ConsumerMessagesAsyncSequence.HighLowWatermark(
            lowWatermark: 5,
            highWatermark: 10
        )
        let acknowledgementsSequenceDelegate = AcknowledgedMessagesAsyncSequenceDelegate { [weak self] in
            self?.produceMore()
        } didTerminateClosure: { [weak self] in
//            self?.close()
            print("TODO: implement")
        }
        let acknowledgementsSourceAndSequence = NIOAsyncSequenceProducer.makeSequence(
            elementType: AcknowledgedMessagesAsyncSequence.Element.self,
            backPressureStrategy: backpressureStrategy,
            delegate: acknowledgementsSequenceDelegate
        )
        self.acknowledgementsSource = acknowledgementsSourceAndSequence.source
        self.acknowledgements = AcknowledgedMessagesAsyncSequence(
            wrappedSequence: acknowledgementsSourceAndSequence.sequence
        )

        var config = config
        config.setDeliveryReportCallback(callback: self.deliveryReportCallback)

        self.client = try KafkaClient(type: .producer, config: config, logger: self.logger)
    }

    // MARK: - Initialiser with new config

    public convenience init(
        config: ProducerConfig = ProducerConfig(),
        topicConfig: TopicConfig = TopicConfig(),
        logger: Logger
    ) throws {
        try self.init(
            config: KafkaConfig(producerConfig: config),
            topicConfig: KafkaTopicConfig(topicConfig: topicConfig),
            logger: logger
        )
    }

    /// Method to shutdown the ``KafkaProducer``.
    ///
    /// This method flushes any buffered messages and waits until a callback is received for all of them.
    /// Afterwards, it shuts down the connection to Kafka and cleans any remaining state up.
    /// - Parameter timeout: Maximum amount of milliseconds this method waits for any outstanding messages to be sent.
    public func shutdownGracefully(timeout: Int32 = 10000) async {
        await withCheckedContinuation { continuation in
            self.serialQueue.async {
                switch self.state {
                case .started:
                    self.state = .shuttingDown
                    self._shutDownGracefully(timeout: timeout, continuation: continuation)
                case .shuttingDown, .shutDown:
                    continuation.resume()
                }
            }
        }
    }

    private func _shutDownGracefully(timeout: Int32, continuation: CheckedContinuation<Void, Never>) {
        dispatchPrecondition(condition: .onQueue(self.serialQueue))
        assert(self.state == .shuttingDown)

        // Wait 10 seconds for outstanding messages to be sent and callbacks to be called
        self.client.withKafkaHandlePointer { handle in
            rd_kafka_flush(handle, timeout)
            continuation.resume()
        }

        for (_, topicHandle) in self.topicHandles {
            rd_kafka_topic_destroy(topicHandle)
        }

        self.state = .shutDown
    }

    /// Send messages to the Kafka cluster asynchronously, aka "fire and forget".
    /// This function is non-blocking.
    /// - Parameter message: The ``KafkaProducerMessage`` that is sent to the KafkaCluster.
    /// - Returns: Unique message identifier matching the `id` property of the corresponding ``KafkaAcknowledgedMessage``
    /// - Throws: A ``KafkaError`` if sending the message failed.
    @discardableResult
    public func sendAsync(_ message: KafkaProducerMessage) async throws -> UInt {
        try await withCheckedThrowingContinuation { continuation in
            self.serialQueue.async {
                switch self.state {
                case .started:
                    self._sendAsync(message, continuation: continuation)
                case .shuttingDown, .shutDown:
                    continuation.resume(throwing: KafkaError.connectionClosed(reason: "Tried to produce a message with a closed producer"))
                }
            }
        }
    }

    private func _sendAsync(_ message: KafkaProducerMessage, continuation: CheckedContinuation<UInt, Error>) {
        dispatchPrecondition(condition: .onQueue(self.serialQueue))
        assert(self.state == .started)

        let topicHandle = self.createTopicHandleIfNeeded(topic: message.topic)

        let keyBytes: [UInt8]?
        if var key = message.key {
            keyBytes = key.readBytes(length: key.readableBytes)
        } else {
            keyBytes = nil
        }

        self.messageIDCounter += 1

        let responseCode = message.value.withUnsafeReadableBytes { valueBuffer in

            // Pass message over to librdkafka where it will be queued and sent to the Kafka Cluster.
            // Returns 0 on success, error code otherwise.
            return rd_kafka_produce(
                topicHandle,
                message.partition.rawValue,
                RD_KAFKA_MSG_F_COPY,
                UnsafeMutableRawPointer(mutating: valueBuffer.baseAddress),
                valueBuffer.count,
                keyBytes,
                keyBytes?.count ?? 0,
                UnsafeMutableRawPointer(bitPattern: self.messageIDCounter)
            )
        }

        guard responseCode == 0 else {
            continuation.resume(throwing: KafkaError.rdKafkaError(wrapping: rd_kafka_last_error()))
            return
        }

        continuation.resume(returning: self.messageIDCounter)
    }

    // TODO: move
    // TODO: get threading right?
    // TODO: why does this crash when not using the serial queue? -> stack overflow? (not the website)
    fileprivate func produceMore() {
        self.serialQueue.async {
            var result: Int32 = 0
            self.client.withKafkaHandlePointer { handle in
                result = rd_kafka_poll(handle, 0)
            }
            if result == 0 {
                self.produceMore()
            }
        }
    }

    // Closure that is executed when a message has been acknowledged by Kafka
    // TODO: capturing self fine here?
    private lazy var deliveryReportCallback: (UnsafePointer<rd_kafka_message_t>?) -> Void = { [self] messagePointer in
        guard let messagePointer = messagePointer else {
            self.logger.error("Could not resolve acknowledged message")
            return
        }

        let messageID = UInt(bitPattern: messagePointer.pointee._private)

        do {
            let message = try KafkaAcknowledgedMessage(messagePointer: messagePointer, id: messageID)

            // TODO: deduplicate
            let yieldResult = self.acknowledgementsSource.yield(.success(message))
            switch yieldResult {
            case .produceMore:
                self.produceMore()
            case .dropped, .stopProducing:
                return
            }

        } catch {
            guard let error = error as? KafkaAcknowledgedMessageError else {
                fatalError("Caught error that is not of type \(KafkaAcknowledgedMessageError.self)")
            }

            let yieldResult = self.acknowledgementsSource.yield(.failure(error))
            switch yieldResult {
            case .produceMore:
                self.produceMore()
            case .dropped, .stopProducing:
                return
            }
        }

        // The messagePointer is automatically destroyed by librdkafka
        // For safety reasons, we only use it inside of this closure
    }

    /// Check `topicHandles` for a handle matching the topic name and create a new handle if needed.
    /// - Parameter topic: The name of the topic that is addressed.
    private func createTopicHandleIfNeeded(topic: String) -> OpaquePointer? {
        dispatchPrecondition(condition: .onQueue(self.serialQueue))

        if let handle = self.topicHandles[topic] {
            return handle
        } else {
            let newHandle = self.client.withKafkaHandlePointer { handle in
                self.topicConfig.withDuplicatePointer { duplicatePointer in
                    // Duplicate because rd_kafka_topic_new deallocates config object
                    rd_kafka_topic_new(
                        handle,
                        topic,
                        duplicatePointer
                    )
                }
            }
            if newHandle != nil {
                self.topicHandles[topic] = newHandle
            }
            return newHandle
        }
    }
}
