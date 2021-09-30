/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.kinesis;

import java.net.URI;
import java.util.List;
import java.util.Optional;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.Dependent;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.inject.Named;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.CustomConsumerBuilder;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.KinesisClientBuilder;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;

/**
 * Implementation of the consumer that delivers the messages into Amazon Kinesis destination.
 *
 * @author Jiri Pechanec
 *
 */
@Named("kinesis")
@Dependent
public class KinesisChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KinesisChangeConsumer.class);

    private static final String PROP_PREFIX = "debezium.sink.kinesis.";
    private static final String PROP_REGION_NAME = PROP_PREFIX + "region";
    private static final String PROP_ENDPOINT_NAME = PROP_PREFIX + "endpoint";

    private String region;
    private Optional<String> endpointOverride;

    @ConfigProperty(name = PROP_PREFIX + "credentials.profile", defaultValue = "default")
    String credentialsProfile;

    @ConfigProperty(name = PROP_PREFIX + "null.key", defaultValue = "default")
    String nullKey;

    private KinesisClient client = null;

    @Inject
    @CustomConsumerBuilder
    Instance<KinesisClient> customClient;

    @PostConstruct
    void connect() {
        if (customClient.isResolvable()) {
            client = customClient.get();
            LOGGER.info("Obtained custom configured KinesisClient '{}'", client);
            return;
        }

        final Config config = ConfigProvider.getConfig();
        region = config.getValue(PROP_REGION_NAME, String.class);
        endpointOverride = config.getOptionalValue(PROP_ENDPOINT_NAME, String.class);
        final KinesisClientBuilder builder = KinesisClient.builder()
                .region(Region.of(region))
                .credentialsProvider(ProfileCredentialsProvider.create(credentialsProfile));
        endpointOverride.ifPresent(endpoint -> builder.endpointOverride(URI.create(endpoint)));
        client = builder.build();
        LOGGER.info("Using default KinesisClient '{}'", client);
    }

    @PreDestroy
    void close() {
        try {
            client.close();
        }
        catch (Exception e) {
            LOGGER.warn("Exception while closing Kinesis client: {}", e);
        }
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records, RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        for (ChangeEvent<Object, Object> record : records) {
            LOGGER.trace("Received event '{}'", record);
            Object rv = record.value();
            if (rv == null) {
                rv = "";
            }

            final PutRecordRequest putRecord = PutRecordRequest.builder()
                    .partitionKey((record.key() != null) ? getString(record.key()) : nullKey)
                    .streamName(streamNameMapper.map(record.destination()))
                    .data(SdkBytes.fromByteArray(getBytes(rv)))
                    .build();
            client.putRecord(putRecord);
            committer.markProcessed(record);
        }
        committer.markBatchFinished();
    }
}
