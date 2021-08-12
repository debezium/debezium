/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.nats_streaming;

import java.util.List;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.Dependent;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.inject.Named;

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
import io.nats.client.Connection;
import io.nats.client.ConnectionListener;
import io.nats.streaming.NatsStreaming;
import io.nats.streaming.Options;
import io.nats.streaming.StreamingConnection;

/**
 * Implementation of the consumer that delivers the messages into NATS Streaming subject.
 *
 * @author Thiago Avancini
 */
@Named("nats_streaming")
@Dependent
public class NatsStreamingChangeConsumer extends BaseChangeConsumer
        implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(NatsStreamingChangeConsumer.class);

    private static final String PROP_PREFIX = "debezium.sink.nats_streaming.";
    private static final String PROP_URL = PROP_PREFIX + "url";
    private static final String PROP_CLUSTER_ID = PROP_PREFIX + "cluster.id";
    private static final String PROP_CLIENT_ID = PROP_PREFIX + "client.id";

    private String url;
    private String clusterId;
    private String clientId;

    private StreamingConnection sc;

    @Inject
    @CustomConsumerBuilder
    Instance<StreamingConnection> customStreamingConnection;

    @PostConstruct
    void connect() {
        if (customStreamingConnection.isResolvable()) {
            sc = customStreamingConnection.get();
            LOGGER.info("Obtained custom configured StreamingConnection '{}'", sc);
            return;
        }

        // Read config
        final Config config = ConfigProvider.getConfig();
        url = config.getValue(PROP_URL, String.class);
        clusterId = config.getValue(PROP_CLUSTER_ID, String.class);
        clientId = config.getValue(PROP_CLIENT_ID, String.class);

        // Setup NATS Streaming connection
        Options stanOptions = new Options.Builder()
                .natsUrl(url)
                .connectionListener(new ConnectionListener() {
                    public void connectionEvent(Connection natsConnection, Events event) {
                        if (event == Events.DISCONNECTED) {
                            throw new DebeziumException("NATS Streaming disconnected.");
                        }
                    }
                })
                .connectionLostHandler((streamingConnection, e) -> {
                    throw new DebeziumException("NATS Streaming connection lost.");
                })
                .build();
        try {
            sc = NatsStreaming.connect(clusterId, clientId, stanOptions);
        }
        catch (Exception e) {
            throw new DebeziumException(e);
        }

        LOGGER.info("Using default StreamingConnection '{}'", sc);
    }

    @PreDestroy
    void close() {
        try {
            sc.close();
            LOGGER.info("NATS Streaming connection closed.");
        }
        catch (Exception e) {
            // Do nothing
        }
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records,
                            RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {

        for (ChangeEvent<Object, Object> record : records) {
            if (record.value() != null) {
                String subject = streamNameMapper.map(record.destination());
                byte[] recordBytes = getBytes(record.value());
                LOGGER.trace("Received event @ {} = '{}'", subject, getString(record.value()));

                try {
                    sc.publish(subject, recordBytes);
                }
                catch (Exception e) {
                    throw new DebeziumException(e);
                }
            }
            committer.markProcessed(record);
        }
        committer.markBatchFinished();
    }
}
