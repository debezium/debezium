/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.eventhubs;

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

import com.azure.core.amqp.exception.AmqpException;
import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventDataBatch;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubProducerClient;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.CustomConsumerBuilder;

/**
 * This sink adapter delivers the messages into Azure Event Hubs.
 *
 * @author Abhishek Gupta
 *
 */
@Named("eventhubs")
@Dependent
public class EventHubsChangeConsumer extends BaseChangeConsumer
        implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventHubsChangeConsumer.class);

    private static final String PROP_PREFIX = "debezium.sink.eventhubs.";
    private static final String PROP_CONNECTIONSTRING_NAME = PROP_PREFIX + "connectionstring";

    private String connectionString;
    private EventHubProducerClient producer = null;

    @Inject
    @CustomConsumerBuilder
    Instance<EventHubProducerClient> customProducer;

    @PostConstruct
    void connect() {
        if (customProducer.isResolvable()) {
            producer = customProducer.get();
            LOGGER.info("Obtained custom configured Event Hubs client '{}'", customProducer);
            return;
        }

        final Config config = ConfigProvider.getConfig();
        connectionString = config.getValue(PROP_CONNECTIONSTRING_NAME, String.class);

        producer = new EventHubClientBuilder().connectionString(connectionString).buildProducerClient();
        LOGGER.info("Using default Event Hubs client for namespace '{}'", producer.getFullyQualifiedNamespace());
    }

    @PreDestroy
    void close() {
        try {
            producer.close();
            LOGGER.info("Closed Event Hubs producer client");
        }
        catch (Exception e) {
            LOGGER.warn("Exception while closing Event Hubs producer: {}", e);
        }
    }

    /*
     * TODOs (1) add producer options (CreateBatchOptions) via config - max size,
     * partition id, partition key (2) handle DELETE events
     */

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records,
                            RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        LOGGER.info("Processing CDC events...");

        EventDataBatch batch = producer.createBatch();
        for (ChangeEvent<Object, Object> record : records) {
            LOGGER.info("Got record -- {}", (String) record.value());

            EventData eventData = null;

            if (record.value() instanceof String) {
                eventData = new EventData((String) record.value());
            }
            else if (record.value() instanceof byte[]) {
                eventData = new EventData(getBytes(record.value()));
            }
            try {
                if (!batch.tryAdd(eventData)) {
                    LOGGER.warn("event was too large to fit in the batch - {}", record);
                    continue;
                }
            }
            catch (IllegalArgumentException e) {
                LOGGER.warn("EventData was null - {}", e.getMessage());
                continue;
            }
            catch (AmqpException e) {
                LOGGER.warn("EventData is larger than the maximum size of the EventDataBatch - {}", e.getMessage());
                continue;
            }
            catch (Exception e) {
                LOGGER.warn("Failed to add EventData to batch {}", e.getMessage());
                continue;
            }

            // Event Hubs producer only supports "batch"ed sends. Each record is sent as a
            // separate batch which is then acknowledged/committed

            try {
                producer.send(batch);
                LOGGER.info("Sent record to Event Hubs...");
            }
            catch (Exception e) {
                LOGGER.warn("Failed to send record to Event Hubs {}", e.getMessage());
                // do not mark the record as processed it its not sent to Event Hubs
                continue;
            }
            committer.markProcessed(record);
            LOGGER.info("Record marked processed");
        }

        // TODO - check if this is required in addition to markProcessed for each
        // record?
        // committer.markBatchFinished();
        // LOGGER.info("Batch marked finished");
    }
}
