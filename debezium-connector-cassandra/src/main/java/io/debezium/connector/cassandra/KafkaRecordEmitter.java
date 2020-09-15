/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.schema.TopicSelector;

/**
 * This emitter is responsible for emitting records to Kafka broker and managing offsets post send.
 */
public class KafkaRecordEmitter implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaRecordEmitter.class);

    private final KafkaProducer<byte[], byte[]> producer;
    private final TopicSelector<KeyspaceTable> topicSelector;
    private final OffsetWriter offsetWriter;
    private final OffsetFlushPolicy offsetFlushPolicy;
    private final Map<Record, Future<RecordMetadata>> futures = new LinkedHashMap<>();
    private final Object lock = new Object();
    private long timeOfLastFlush;
    private long emitCount = 0;
    private Converter keyConverter;
    private Converter valueConverter;

    public KafkaRecordEmitter(String kafkaTopicPrefix, String heartbeatPrefix, Properties kafkaProperties,
                              OffsetWriter offsetWriter, Duration offsetFlushIntervalMs, long maxOffsetFlushSize,
                              Converter keyConverter, Converter valueConverter) {
        this.producer = new KafkaProducer<>(kafkaProperties);
        this.topicSelector = CassandraTopicSelector.defaultSelector(kafkaTopicPrefix, heartbeatPrefix);
        this.offsetWriter = offsetWriter;
        this.offsetFlushPolicy = offsetFlushIntervalMs.isZero() ? OffsetFlushPolicy.always() : OffsetFlushPolicy.periodic(offsetFlushIntervalMs, maxOffsetFlushSize);
        this.keyConverter = keyConverter;
        this.valueConverter = valueConverter;
    }

    public void emit(Record record) {
        try {
            synchronized (lock) {
                ProducerRecord<byte[], byte[]> producerRecord = toProducerRecord(record);
                LOGGER.debug("Sending the record '{}'", record.toString());
                Future<RecordMetadata> future = producer.send(producerRecord);
                LOGGER.debug("The record '{}' has been sent", record.toString());
                futures.put(record, future);
                maybeFlushAndMarkOffset();
            }
        }
        catch (Exception e) {
            throw new DebeziumException(String.format("Failed to send record %s", record.toString()), e);
        }
    }

    private ProducerRecord<byte[], byte[]> toProducerRecord(Record record) {
        String topic = topicSelector.topicNameFor(record.getSource().keyspaceTable);
        byte[] serializedKey = keyConverter.fromConnectData(topic, record.getKeySchema(), record.buildKey());
        byte[] serializedValue = valueConverter.fromConnectData(topic, record.getValueSchema(), record.buildValue());
        return new ProducerRecord<>(topic, serializedKey, serializedValue);
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
        }
        catch (ExecutionException | InterruptedException e) {
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
