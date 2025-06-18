/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.cockroachdb;

import java.time.Instant;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceResult;
import io.debezium.pipeline.source.spi.ChangeEventSourceContext;
import io.debezium.pipeline.source.spi.EventDispatcher;

/**
 * Polls the Kafka topic written to by CockroachDB's CHANGEFEED with enriched envelope.
 * Converts each changefeed message into Debezium {@link org.apache.kafka.connect.source.SourceRecord}s and dispatches it.
 *
 * @author Virag Tripathi
 */
public class CockroachDBChangefeedClient implements ChangeEventSource<CockroachDBPartition, CockroachDBOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(CockroachDBChangefeedClient.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final CockroachDBConnectorConfig config;
    private final CockroachDBSourceRecordMapper recordMapper;
    private final CockroachDBSourceInfoStructMaker sourceInfoStructMaker;

    public CockroachDBChangefeedClient(CockroachDBConnectorConfig config) {
        this.config = config;
        this.recordMapper = new CockroachDBSourceRecordMapper(config);
        this.sourceInfoStructMaker = new CockroachDBSourceInfoStructMaker(config);
    }

    @Override
    public ChangeEventSourceResult execute(
                                           ChangeEventSourceContext context,
                                           CockroachDBPartition partition,
                                           CockroachDBOffsetContext offsetContext,
                                           EventDispatcher<CockroachDBPartition, ?> dispatcher)
            throws InterruptedException {

        String topic = config.getChangefeedTopic();
        LOGGER.info("Subscribing to CockroachDB changefeed topic '{}'", topic);

        try (KafkaConsumer<String, String> consumer = createKafkaConsumer()) {
            consumer.subscribe(Collections.singletonList(topic));

            while (context.isRunning()) {
                ConsumerRecords<String, String> records = consumer.poll(config.getPollInterval());
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        JsonNode root = MAPPER.readTree(record.value());

                        JsonNode keyNode = root.get("key");
                        JsonNode afterNode = root.get("after");
                        JsonNode beforeNode = root.get("before");
                        String updated = root.path("updated").asText(null);

                        // Optional enriched properties
                        JsonNode sourceNode = root.path("source");
                        String hlc = sourceNode.path("ts_hlc").asText(null);
                        Long tsNs = sourceNode.has("ts_ns") ? sourceNode.get("ts_ns").asLong() : null;

                        Envelope.Operation op = beforeNode != null && afterNode == null
                                ? Envelope.Operation.DELETE
                                : Envelope.Operation.UPDATE;

                        // Offset timestamp
                        Instant resolvedTs = updated != null ? Instant.parse(updated) : Instant.now();

                        // Build updated offset context
                        offsetContext = new CockroachDBOffsetContext(config, resolvedTs, hlc, tsNs);

                        if (afterNode == null && beforeNode == null) {
                            // Resolved timestamp / heartbeat message
                            dispatcher.dispatchHeartbeatEvent(
                                    partition,
                                    offsetContext,
                                    recordMapper.mapResolvedTimestamp(partition, offsetContext));
                            continue;
                        }

                        dispatcher.dispatchDataChangeEvent(
                                partition,
                                recordMapper.mapChange(
                                        partition,
                                        offsetContext,
                                        topic,
                                        null,
                                        keyNode.toString(),
                                        null,
                                        afterNode != null ? afterNode.toString() : beforeNode.toString(),
                                        op,
                                        sourceInfoStructMaker.sourceInfo(offsetContext)));
                    }
                    catch (Exception ex) {
                        LOGGER.warn("Failed to parse changefeed record: {}", record.value(), ex);
                    }
                }
            }

        }
        catch (Exception e) {
            LOGGER.error("Error while polling CockroachDB changefeed topic", e);
        }

        return ChangeEventSourceResult.completed();
    }

    private KafkaConsumer<String, String> createKafkaConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", config.getKafkaBootstrapServers());
        props.put("group.id", config.getLogicalName() + "-cockroachdb-consumer");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("auto.offset.reset", "earliest");
        return new KafkaConsumer<>(props);
    }
}
