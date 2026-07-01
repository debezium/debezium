/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.kafka;

/**
 * Set of interfaces which mark implementations as Kafka-based and allows access to underlying Kafka instances.
 *
 * @author Debezium Authors
 */
public interface KafkaConnectStorageAdapter {

    interface OffsetStorageReader {
        /**
         * Get the underlying Kafka OffsetStorageReader.
         *
         * @return the wrapped Kafka reader
         */
        org.apache.kafka.connect.storage.OffsetStorageReader getDelegate();
    }

    interface OffsetStorageWriter {
        /**
         * Get the underlying Kafka OffsetStorageWriter.
         *
         * @return the wrapped Kafka writer
         */
        org.apache.kafka.connect.storage.OffsetStorageWriter getDelegate();
    }

    interface OffsetBackingStore {
        /**
         * Get the underlying Kafka OffsetBackingStore.
         *
         * @return the wrapped Kafka store
         */
        org.apache.kafka.connect.storage.OffsetBackingStore getDelegate();
    }

    interface HeaderConverter {
        /**
         * Get the underlying Kafka HeaderConverter.
         *
         * @return the wrapped Kafka header converter
         */
        org.apache.kafka.connect.storage.HeaderConverter getDelegate();
    }

    interface Converter {
        /**
         * Get the underlying Kafka Converter.
         *
         * @return the wrapped Kafka converter
         */
        org.apache.kafka.connect.storage.Converter getDelegate();
    }
}
