/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.Dependent;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.inject.Named;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.CustomConsumerBuilder;

/**
 * An implementation of the {@link DebeziumEngine.ChangeConsumer} interface that publishes change event messages to Kafka.
 */
@Named("kafka")
@Dependent
public class KafkaChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaChangeConsumer.class);

    private static final String PROP_PREFIX = "debezium.sink.kafka.";
    private static final String PROP_BOOTSTRAP_SERVERS = PROP_PREFIX + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;

    private KafkaProducer<Object, Object> producer;

    @Inject
    @CustomConsumerBuilder
    Instance<KafkaProducer<Object, Object>> customKafkaProducer;

    @PostConstruct
    void start() {
        if (customKafkaProducer.isResolvable()) {
            producer = customKafkaProducer.get();
            LOGGER.info("Obtained custom configured KafkaProducer '{}'", producer);
            return;
        }

        final Config config = ConfigProvider.getConfig();

        final Properties props = new Properties();
        props.put(PROP_BOOTSTRAP_SERVERS, config.getValue(PROP_BOOTSTRAP_SERVERS, String.class));
        producer = new KafkaProducer<>(props);
        LOGGER.info("consumer started...");
    }

    @PreDestroy
    void stop() {
        LOGGER.info("consumer destroyed...");
        if (producer != null) {
            try {
                producer.close(Duration.ofSeconds(5));
            }
            catch (Throwable t) {
                LOGGER.warn("Could not close producer", t);
            }
        }
    }

    @Override
    public void handleBatch(final List<ChangeEvent<Object, Object>> records, final RecordCommitter<ChangeEvent<Object, Object>> committer) throws InterruptedException {
        final List<Future<RecordMetadata>> futures = new ArrayList<>();
        for (ChangeEvent<Object, Object> record : records) {
            try {
                // TODO: change log level to trace
                LOGGER.info("Received event '{}'", record);
                futures.add(producer.send(new ProducerRecord<>(record.destination(), record.key(), record.value())));
                committer.markProcessed(record);
            }
            catch (Exception e) {
                throw new DebeziumException(e);
            }
        }

        final List<Long> offsets = new ArrayList<>();
        for (Future<RecordMetadata> future : futures) {
            try {
                RecordMetadata meta = future.get();
                offsets.add(meta.offset());
            }
            catch (InterruptedException | ExecutionException e) {
                throw new DebeziumException(e);
            }
        }
        LOGGER.trace("Sent messages with offsets: {}", offsets);
        committer.markBatchFinished();
    }
}
