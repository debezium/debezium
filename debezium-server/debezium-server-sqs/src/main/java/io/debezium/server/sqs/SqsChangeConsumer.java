/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.sqs;

import java.net.URI;
import java.util.List;
import java.util.Optional;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.Dependent;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.inject.Named;

import org.apache.commons.lang3.StringUtils;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.CustomConsumerBuilder;

import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.SqsClientBuilder;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;

/**
 * Implementation of the consumer that delivers the messages into Amazon SQS destination
 *
 */
@Named("sqs")
@Dependent
public class SqsChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqsChangeConsumer.class);

    private static final String PROP_PREFIX = "debezium.sink.sqs.";
    private static final String PROP_REGION_NAME = PROP_PREFIX + "region";
    private static final String PROP_ENDPOINT_NAME = PROP_PREFIX + "endpoint";
    private static final String ANON_CREDENTIALS_PROVIDER = "AnonymousCredentialsProvider";

    private String region;
    private Optional<String> endpointOverride;

    @ConfigProperty(name = PROP_PREFIX + "credentials.profile", defaultValue = ANON_CREDENTIALS_PROVIDER)
    String credentialsProfile;

    @ConfigProperty(name = PROP_PREFIX + "prefix")
    Optional<String> queuePrefix;

    @ConfigProperty(name = PROP_PREFIX + "json.fields.extract")
    Optional<String> fieldToExtract;

    @ConfigProperty(name = PROP_PREFIX + "json.fields.route")
    Optional<String> routingField;

    @ConfigProperty(name = "debezium.format.key", defaultValue = "json")
    String keyFormat;

    @ConfigProperty(name = "debezium.format.value", defaultValue = "json")
    String valueFormat;

    private SqsClient client = null;

    @Inject
    @CustomConsumerBuilder
    Instance<SqsClient> customClient;

    @PostConstruct
    void connect() {
        LOGGER.debug("SQS connect");

        if (customClient.isResolvable()) {
            client = customClient.get();
            LOGGER.info("Using custom SqsClient '{}'", client);
            return;
        }

        final Config config = ConfigProvider.getConfig();
        region = config.getValue(PROP_REGION_NAME, String.class);
        endpointOverride = config.getOptionalValue(PROP_ENDPOINT_NAME, String.class);

        AwsCredentialsProvider credentialsProvider = (credentialsProfile.equalsIgnoreCase(ANON_CREDENTIALS_PROVIDER)) ? AnonymousCredentialsProvider.create()
                : ProfileCredentialsProvider.create(credentialsProfile);

        final SqsClientBuilder builder = SqsClient.builder()
                .region(Region.of(region))
                .credentialsProvider(credentialsProvider);

        endpointOverride.ifPresent(endpoint -> builder.endpointOverride(URI.create(endpoint)));
        client = builder.build();

        LOGGER.info("Using default SqsClient '{}'", client);
    }

    @PreDestroy
    void close() {
        LOGGER.info("SQS close");

        try {
            client.close();
        }
        catch (Exception e) {
            LOGGER.warn("Exception while closing Kinesis client", e);
        }
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> events, RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        for (ChangeEvent<Object, Object> event : events) {
            LOGGER.trace("Received event '{}'", event);
            handleEvent(event);
            committer.markProcessed(event);
        }
        committer.markBatchFinished();
    }

    private void handleEvent(ChangeEvent<Object, Object> event) {
        String message = (event.value() != null) ? event.value().toString() : "";

        if (StringUtils.isNotEmpty(message)) {
            final DocumentContext messageContext = getMessageContext(message);

            if (messageContext != null && fieldToExtract.isPresent()) {
                message = messageContext.read("$." + fieldToExtract.get());
            }

            final String queueName = getQueueName(event.destination(), messageContext);
            send(queuePrefix.orElse("") + queueName, message);
        }
    }

    private DocumentContext getMessageContext(String message) {
        if (valueFormat.equalsIgnoreCase("json")) {
            return JsonPath.parse(message);
        }
        return null;
    }

    private String getQueueName(String destination, DocumentContext messageContext) {
        if (messageContext != null && routingField.isPresent()) {
            return messageContext.read("$." + routingField.get());
        }
        else {
            // This may be a bit brittle but not sure how to easily get just the table name,
            // it seems to be coming in as servername.schema.table.
            String queueName = destination.substring(destination.lastIndexOf('.') + 1);

            // If we can't get just the table name use the full destination which shouldn't be null
            if (StringUtils.isEmpty(queueName)) {
                queueName = destination;
            }

            return queueName;
        }
    }

    private void send(String queueName, String message) {
        if (StringUtils.isEmpty(queueName) || StringUtils.isEmpty(message)) {
            LOGGER.warn("Cannot send message due to missing required parameters queueName empty = '{}' message empty = '{}'",
                    StringUtils.isEmpty(queueName), StringUtils.isEmpty(message));
            return;
        }

        LOGGER.info("Sending message to queue '{}'", queueName);
        LOGGER.info("Message='{}'", message);
        try {
            GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                    .queueName(queueName)
                    .build();

            String queueUrl = client.getQueueUrl(getQueueRequest).queueUrl();
            SendMessageRequest sendMsgRequest = SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(message)
                    .build();

            LOGGER.info("Queue URL '{}'", queueUrl);
            client.sendMessage(sendMsgRequest);

        }
        catch (SqsException e) {
            LOGGER.warn("Failed to send message to queue {}", queueName, e);
        }
    }
}
