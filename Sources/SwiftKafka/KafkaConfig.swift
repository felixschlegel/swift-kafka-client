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

/// Used to configure producers and consumers.
/// `KafkaConfig` is a `struct` that points to a configuration in memory.
/// Once a property of the `KafkaConfig` is changed, a duplicate in-memory config is created using the
/// copy-on-write mechanism.
/// For more information on how to configure Kafka, see
/// [all available configurations](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md).
public struct KafkaConfig: Hashable, Equatable {
    private final class _Internal: Hashable, Equatable {
        /// Pointer to the `rd_kafka_conf_t` object managed by `librdkafka`
        private(set) var pointer: OpaquePointer

        /// Initialize internal `KafkaConfig` object with default configuration
        init() {
            self.pointer = rd_kafka_conf_new()
        }

        /// Initialize internal `KafkaConfig` object through a given `rd_kafka_conf_t` pointer
        init(pointer: OpaquePointer) {
            self.pointer = pointer
        }

        deinit {
            rd_kafka_conf_destroy(pointer)
        }

        func value(forKey key: String) -> String? {
            let value = UnsafeMutablePointer<CChar>.allocate(capacity: KafkaClient.stringSize)
            defer { value.deallocate() }

            var valueSize = KafkaClient.stringSize
            let configResult = rd_kafka_conf_get(
                pointer,
                key,
                value,
                &valueSize
            )

            if configResult == RD_KAFKA_CONF_OK {
                return String(cString: value)
            }
            return nil
        }

        func set(_ value: String, forKey key: String) throws {
            let errorChars = UnsafeMutablePointer<CChar>.allocate(capacity: KafkaClient.stringSize)
            defer { errorChars.deallocate() }

            let configResult = rd_kafka_conf_set(
                pointer,
                key,
                value,
                errorChars,
                KafkaClient.stringSize
            )

            if configResult != RD_KAFKA_CONF_OK {
                let errorString = String(cString: errorChars)
                throw KafkaError(description: errorString)
            }
        }

        func createDuplicate() -> _Internal {
            let duplicatePointer: OpaquePointer = rd_kafka_conf_dup(self.pointer)
            return .init(pointer: duplicatePointer)
        }

        // MARK: Hashable

        func hash(into hasher: inout Hasher) {
            hasher.combine(self.pointer)
        }

        // MARK: Equatable

        static func == (lhs: _Internal, rhs: _Internal) -> Bool {
            return lhs.pointer == rhs.pointer
        }
    }

    private var _internal: _Internal

    public init() {
        self._internal = .init()
    }

    var pointer: OpaquePointer {
        return self._internal.pointer
    }

    /// Retrieve value of configuration property for `key`
    public func value(forKey key: String) -> String? {
        return self._internal.value(forKey: key)
    }

    /// Set configuration `value` for `key`
    public mutating func set(_ value: String, forKey key: String) throws {
        // Copy-on-write mechanism
        if !isKnownUniquelyReferenced(&(self._internal)) {
            self._internal = self._internal.createDuplicate()
        }

        try self._internal.set(value, forKey: key)
    }
}
