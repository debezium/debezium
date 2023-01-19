/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.kafka;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.Dependent;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.inject.Named;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.engine.Header;
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
    private static final String PROP_PREFIX_PRODUCER = PROP_PREFIX + "producer.";

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
        producer = new KafkaProducer<>(getConfigSubset(config, PROP_PREFIX_PRODUCER));
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
    public void handleBatch(final List<ChangeEvent<Object, Object>> records,
                            final RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(records.size());
        for (ChangeEvent<Object, Object> record : records) {
            try {
                LOGGER.trace("Received event '{}'", record);

                Headers headers = convertHeaders(record);

                producer.send(new ProducerRecord<>(record.destination(), null, null, record.key(), record.value(), headers), (metadata, exception) -> {
                    if (exception != null) {
                        LOGGER.error("Failed to send record to {}:", record.destination(), exception);
                        throw new DebeziumException(exception);
                    }
                    else {
                        LOGGER.trace("Sent message with offset: {}", metadata.offset());
                        latch.countDown();
                    }
                });
                committer.markProcessed(record);
            }
            catch (Exception e) {
                throw new DebeziumException(e);
            }
        }

        latch.await();
        committer.markBatchFinished();
    }

    private Headers convertHeaders(ChangeEvent<Object, Object> record) {
        List<Header<Object>> headers = record.headers();
        Headers kafkaHeaders = new RecordHeaders();
        for (Header<Object> header : headers) {
            kafkaHeaders.add(header.getKey(), getBytes(header.getValue()));
        }
        return kafkaHeaders;
    }
}
