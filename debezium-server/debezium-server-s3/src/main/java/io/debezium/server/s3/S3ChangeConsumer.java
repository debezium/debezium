/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.s3;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.Dependent;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.inject.Named;

import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.CustomConsumerBuilder;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
@Named("s3")
@Dependent
public class S3ChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3ChangeConsumer.class);
    private static final String PROP_PREFIX = "debezium.sink.s3.";
    private static final String PROP_REGION_NAME = PROP_PREFIX + "region";
    private static final String PROP_BUCKET_NAME = PROP_PREFIX + "bucket.name";
    final String valueFormat = ConfigProvider.getConfig().getOptionalValue("debezium.format.value", String.class).orElse(Json.class.getSimpleName().toLowerCase());

    final String credentialsProfile = ConfigProvider.getConfig().getOptionalValue(PROP_PREFIX + "credentials.profile", String.class).orElse("default");
    final String endpointOverride = ConfigProvider.getConfig().getOptionalValue("debezium.sink.s3.endpointoverride", String.class).orElse("false");
    final Boolean useInstanceProfile = ConfigProvider.getConfig().getOptionalValue("debezium.sink.s3.credentials.useinstancecred", Boolean.class).orElse(false);

    @Inject
    @CustomConsumerBuilder
    Instance<S3Client> customClient;
    @Inject
    Instance<ObjectKeyMapper> customObjectKeyMapper;
    @ConfigProperty(name = PROP_BUCKET_NAME, defaultValue = "My-S3-Bucket")
    String bucket;
    S3Client s3client;
    @ConfigProperty(name = PROP_REGION_NAME, defaultValue = "eu-central-1")
    String region;

    private ObjectKeyMapper objectKeyMapper;

    @PostConstruct
    void connect() throws URISyntaxException {
        if (customObjectKeyMapper.isResolvable()) {
            objectKeyMapper = customObjectKeyMapper.get();
        }
        else {
            objectKeyMapper = new DefaultObjectKeyMapper();
        }
        LOGGER.info("Using '{}' stream name mapper", objectKeyMapper);
        if (customClient.isResolvable()) {
            s3client = customClient.get();
            LOGGER.info("Obtained custom configured S3Client '{}'", s3client);
            return;
        }

        AwsCredentialsProvider credProvider;
        if (useInstanceProfile) {
            LOGGER.info("Using Instance Profile Credentials For S3");
            credProvider = InstanceProfileCredentialsProvider.create();
        }
        else {
            credProvider = ProfileCredentialsProvider.create(credentialsProfile);
        }

        S3ClientBuilder clientBuilder = S3Client.builder()
                .region(Region.of(region))
                .credentialsProvider(credProvider);
        // used for testing, using minio
        if (!endpointOverride.trim().toLowerCase().equals("false")) {
            clientBuilder.endpointOverride(new URI(endpointOverride));
        }
        s3client = clientBuilder.build();
        LOGGER.info("Using default S3Client '{}'", s3client);
    }

    @PreDestroy
    void close() {
        try {
            s3client.close();
        }
        catch (Exception e) {
            LOGGER.error("Exception while closing S3 client: ", e);
        }
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records, DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        LocalDateTime batchTime = LocalDateTime.now();
        for (ChangeEvent<Object, Object> record : records) {
            final PutObjectRequest putRecord = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(objectKeyMapper.map(record.destination(), batchTime, UUID.randomUUID().toString()))
                    .build();
            s3client.putObject(putRecord, RequestBody.fromBytes(getBytes(record.value())));
            committer.markProcessed(record);
        }
        committer.markBatchFinished();
    }
}
