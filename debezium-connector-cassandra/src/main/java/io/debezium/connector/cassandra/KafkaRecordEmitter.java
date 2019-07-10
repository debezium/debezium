/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * This emitter is responsible for emitting records to Kafka broker and managing offsets post send.
 */
public class KafkaRecordEmitter implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaRecordEmitter.class);

    private final KafkaProducer<GenericRecord, GenericRecord> producer;
    private final CassandraTopicSelector topicSelector;
    private final OffsetWriter offsetWriter;
    private final OffsetFlushPolicy offsetFlushPolicy;
    private final Map<Record, Future<RecordMetadata>> futures = new LinkedHashMap<>();
    private final Object lock = new Object();
    private long timeOfLastFlush;
    private long emitCount = 0;

    public KafkaRecordEmitter(String kafkaTopicPrefix, Properties kafkaProperties, OffsetWriter offsetWriter, Duration offsetFlushIntervalMs, long maxOffsetFlushSize) {
        this.producer = new KafkaProducer<>(kafkaProperties);
        this.topicSelector = CassandraTopicSelector.defaultSelector(kafkaTopicPrefix);
        this.offsetWriter = offsetWriter;
        this.offsetFlushPolicy = offsetFlushIntervalMs.isZero() ? OffsetFlushPolicy.always() : OffsetFlushPolicy.periodic(offsetFlushIntervalMs, maxOffsetFlushSize);
    }

    public void emit(Record record) {
        synchronized (lock) {
            ProducerRecord<GenericRecord, GenericRecord> producerRecord = toProducerRecord(record);
            Future<RecordMetadata> future = producer.send(producerRecord);
            futures.put(record, future);
            maybeFlushAndMarkOffset();
        }
    }

    private ProducerRecord<GenericRecord, GenericRecord> toProducerRecord(Record record) {
        String topic = topicSelector.topicNameFor(record.getSource().keyspaceTable);
        return new ProducerRecord<>(topic, record.buildKey(), record.buildValue());
    }

    private void maybeFlushAndMarkOffset() {
        long now = System.currentTimeMillis();
        long timeSinceLastFlush = now - timeOfLastFlush;
        if (offsetFlushPolicy.shouldFlush(Duration.ofMillis(timeSinceLastFlush), futures.size())) {
            flushAndMarkOffset();
            timeOfLastFlush = now;
        }
    }

    private void flushAndMarkOffset() {
        futures.entrySet().stream().filter(this::flush).filter(this::hasOffset).forEach(this::markOffset);
        offsetWriter.flush();
        futures.clear();
    }

    private boolean flush(Map.Entry<Record, Future<RecordMetadata>> recordEntry) {
        try {
            recordEntry.getValue().get(); // wait
            if (++emitCount % 10_000 == 0) {
                LOGGER.info("Emitted {} records to Kafka Broker", emitCount);
                emitCount = 0;
            }
            return true;
        } catch (ExecutionException | InterruptedException e) {
            LOGGER.error("Failed to emit record {}", recordEntry.getKey(), e);
            return false;
        }
    }

    private boolean hasOffset(Map.Entry<Record, Future<RecordMetadata>> recordEntry) {
        return recordEntry.getKey().shouldMarkOffset();
    }

    private void markOffset(Map.Entry<Record, Future<RecordMetadata>> recordEntry) {
        SourceInfo source = recordEntry.getKey().getSource();
        String sourceTable = source.keyspaceTable.name();
        String sourceOffset = source.offsetPosition.serialize();
        boolean isSnapshot = source.snapshot;
        offsetWriter.markOffset(sourceTable, sourceOffset, isSnapshot);
        if (isSnapshot) {
            LOGGER.info("Mark snapshot offset for table '{}'", sourceTable);
        }
    }

    public void close() {
        producer.close();
    }
}
