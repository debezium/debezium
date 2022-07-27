/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.http;

import static java.net.HttpURLConnection.HTTP_ACCEPTED;
import static java.net.HttpURLConnection.HTTP_NO_CONTENT;
import static java.net.HttpURLConnection.HTTP_OK;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.enterprise.context.Dependent;
import javax.inject.Named;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;

/**
 * Implementation of the consumer that delivers the messages to an HTTP Webhook destination.
 *
 * @author Chris Baumbauer
 */
@Named("http")
@Dependent
public class HttpChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpChangeConsumer.class);

    private static final String PROP_PREFIX = "debezium.sink.http.";
    private static final String PROP_WEBHOOK_URL = "url";
    private static final String PROP_CLIENT_TIMEOUT = "timeout.ms";
    private static final String PROP_RETRIES = "retries";
    private static final String PROP_RETRY_INTERVAL = "retry.interval.ms";

    private static final Long HTTP_TIMEOUT = Integer.toUnsignedLong(60000); // Default to 60s
    private static final int DEFAULT_RETRIES = 5;
    private static final Long RETRY_INTERVAL = Integer.toUnsignedLong(1_000); // Default to 1s

    private static Duration timeoutDuration;
    private static int retries;
    private static Duration retryInterval;

    private HttpClient client;
    private HttpRequest.Builder requestBuilder;

    // If this is running as a Knative object, then expect the sink URL to be located in `K_SINK`
    // as per https://knative.dev/development/eventing/custom-event-source/sinkbinding/
    @PostConstruct
    void connect() throws URISyntaxException {
        String sinkUrl;
        String contentType;

        client = HttpClient.newHttpClient();
        final Config config = ConfigProvider.getConfig();
        String sink = System.getenv("K_SINK");
        timeoutDuration = Duration.ofMillis(HTTP_TIMEOUT);
        retries = DEFAULT_RETRIES;
        retryInterval = Duration.ofMillis(RETRY_INTERVAL);

        if (sink != null) {
            sinkUrl = sink;
        }
        else {
            sinkUrl = config.getValue(PROP_PREFIX + PROP_WEBHOOK_URL, String.class);
        }

        config.getOptionalValue(PROP_PREFIX + PROP_CLIENT_TIMEOUT, String.class)
                .ifPresent(t -> timeoutDuration = Duration.ofMillis(Long.parseLong(t)));

        config.getOptionalValue(PROP_PREFIX + PROP_RETRIES, String.class)
                .ifPresent(n -> retries = Integer.parseInt(n));

        config.getOptionalValue(PROP_PREFIX + PROP_RETRY_INTERVAL, String.class)
                .ifPresent(t -> retryInterval = Duration.ofMillis(Long.parseLong(t)));

        switch (config.getValue("debezium.format.value", String.class)) {
            case "avro":
                contentType = "avro/bytes";
                break;
            case "cloudevents":
                contentType = "application/cloudevents+json";
                break;
            default:
                // Note: will default to JSON if it cannot be determined, but should not reach this point
                contentType = "application/json";
        }

        LOGGER.info("Using http content-type type {}", contentType);
        LOGGER.info("Using sink URL: {}", sinkUrl);
        requestBuilder = HttpRequest.newBuilder(new URI(sinkUrl)).timeout(timeoutDuration);
        requestBuilder.setHeader("content-type", contentType);
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records, DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        for (ChangeEvent<Object, Object> record : records) {
            LOGGER.trace("Received event '{}'", record);

            if (record.value() != null) {
                int attempts = 0;
                while (!recordSent(record)) {
                    attempts++;
                    if (attempts >= retries) {
                        throw new DebeziumException("Exceeded maximum number of attempts to publish event " + record);
                    }
                    Metronome.sleeper(retryInterval, Clock.SYSTEM).pause();
                }
                committer.markProcessed(record);
            }
        }

        committer.markBatchFinished();
    }

    private boolean recordSent(ChangeEvent<Object, Object> record) throws InterruptedException {
        boolean sent = false;
        String value = (String) record.value();
        HttpResponse r;

        HttpRequest request = requestBuilder.POST(HttpRequest.BodyPublishers.ofString(value)).build();

        try {
            r = client.send(request, HttpResponse.BodyHandlers.ofString());
        }
        catch (IOException ioe) {
            throw new InterruptedException(ioe.toString());
        }

        if ((r.statusCode() == HTTP_OK) || (r.statusCode() == HTTP_NO_CONTENT) || (r.statusCode() == HTTP_ACCEPTED)) {
            sent = true;
        }
        else {
            LOGGER.info("Failed to publish event: " + r.body());
        }

        return sent;
    }
}
