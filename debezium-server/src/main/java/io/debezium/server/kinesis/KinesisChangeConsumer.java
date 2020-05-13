/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.kinesis;

import java.util.List;

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

import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.server.CustomConsumerBuilder;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;

/**
 * Implementation of the consumer that delivers the messages into Amazon Kinesis destination.
 *
 * @author Jiri Pechanec
 *
 */
@Named("kinesis")
@Dependent
public class KinesisChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KinesisChangeConsumer.class);

    private static final String PROP_PREFIX = "debezium.sink.kinesis.";
    private static final String PROP_REGION_NAME = PROP_PREFIX + "region";

    private String region;

    @ConfigProperty(name = PROP_PREFIX + "credentials.profile", defaultValue = "default")
    String credentialsProfile;

    @ConfigProperty(name = PROP_PREFIX + "null.key", defaultValue = "default")
    String nullKey;

    private KinesisClient client = null;
    private StreamNameMapper streamNameMapper = (x) -> x;

    @Inject
    @CustomConsumerBuilder
    Instance<KinesisClient> customClient;

    @Inject
    Instance<StreamNameMapper> customStreamNameMapper;

    @PostConstruct
    void connect() {
        if (customStreamNameMapper.isResolvable()) {
            streamNameMapper = customStreamNameMapper.get();
        }
        LOGGER.info("Using '{}' stream name mapper", streamNameMapper);
        if (customClient.isResolvable()) {
            client = customClient.get();
            LOGGER.info("Obtained custom configured KinesisClient '{}'", client);
            return;
        }

        final Config config = ConfigProvider.getConfig();
        region = config.getValue(PROP_REGION_NAME, String.class);
        client = KinesisClient.builder()
                .region(Region.of(region))
                .credentialsProvider(ProfileCredentialsProvider.create(credentialsProfile))
                .build();
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

    private byte[] getByte(Object object) {
        if (object instanceof byte[]) {
            return (byte[]) object;
        }
        else if (object instanceof String) {
            return ((String) object).getBytes();
        }
        throw new DebeziumException(unsupportedTypeMessage(object));
    }

    private String getString(Object object) {
        if (object instanceof String) {
            return (String) object;
        }
        throw new DebeziumException(unsupportedTypeMessage(object));
    }

    public String unsupportedTypeMessage(Object object) {
        final String type = (object == null) ? "null" : object.getClass().getName();
        return "Unexpected data type '" + type + "'";
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records, RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        for (ChangeEvent<Object, Object> record : records) {
            LOGGER.trace("Received event '{}'", record);
            final PutRecordRequest putRecord = PutRecordRequest.builder()
                    .partitionKey((record.key() != null) ? getString(record.key()) : nullKey)
                    .streamName(streamNameMapper.map(record.destination()))
                    .data(SdkBytes.fromByteArray(getByte(record.value())))
                    .build();
            client.putRecord(putRecord);
            committer.markProcessed(record);
        }
        committer.markBatchFinished();
    }
}
