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
import com.azure.messaging.eventhubs.models.CreateBatchOptions;

import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.CustomConsumerBuilder;

/**
 * This sink adapter delivers change event messages to Azure Event Hubs
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
    private static final String PROP_CONNECTION_STRING_NAME = PROP_PREFIX + "connectionstring";
    private static final String PROP_EVENTHUB_NAME = PROP_PREFIX + "hubname";
    private static final String PROP_PARTITION_ID = PROP_PREFIX + "partitionid";
    private static final String PROP_PARTITION_KEY = PROP_PREFIX + "partitionkey";
    // maximum size for the batch of events (bytes)
    private static final String PROP_MAX_BATCH_SIZE = PROP_PREFIX + "maxbatchsize";

    private String connectionString;
    private String eventHubName;
    private String partitionID;
    private String partitionKey;
    private Integer maxBatchSize;

    // connection string format -
    // Endpoint=sb://<NAMESPACE>/;SharedAccessKeyName=<KEY_NAME>;SharedAccessKey=<ACCESS_KEY>;EntityPath=<HUB_NAME>
    private static final String CONNECTION_STRING_FORMAT = "%s;EntityPath=%s";

    private EventHubProducerClient producer = null;

    @Inject
    @CustomConsumerBuilder
    Instance<EventHubProducerClient> customProducer;

    @PostConstruct
    void connect() {
        if (customProducer.isResolvable()) {
            producer = customProducer.get();
            LOGGER.info("Obtained custom configured Event Hubs client for namespace '{}'",
                    customProducer.get().getFullyQualifiedNamespace());
            return;
        }

        final Config config = ConfigProvider.getConfig();
        connectionString = config.getValue(PROP_CONNECTION_STRING_NAME, String.class);
        eventHubName = config.getValue(PROP_EVENTHUB_NAME, String.class);

        // optional config
        partitionID = config.getOptionalValue(PROP_PARTITION_ID, String.class).orElse("");
        partitionKey = config.getOptionalValue(PROP_PARTITION_KEY, String.class).orElse("");
        maxBatchSize = config.getOptionalValue(PROP_MAX_BATCH_SIZE, Integer.class).orElse(0);

        String finalConnectionString = String.format(CONNECTION_STRING_FORMAT, connectionString, eventHubName);

        try {
            producer = new EventHubClientBuilder().connectionString(finalConnectionString).buildProducerClient();
        }
        catch (Exception e) {
            throw new DebeziumException(e);
        }

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

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records,
                            RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        LOGGER.trace("Event Hubs sink adapter processing change events");

        CreateBatchOptions op = new CreateBatchOptions().setPartitionId(partitionID);
        if (partitionKey != "") {
            op.setPartitionKey(partitionKey);
        }
        if (maxBatchSize.intValue() != 0) {
            op.setMaximumSizeInBytes(maxBatchSize);
        }

        for (int recordIndex = 0; recordIndex < records.size();) {
            int start = recordIndex;
            LOGGER.trace("Emitting events starting from index {}", start);

            EventDataBatch batch = producer.createBatch(op);

            // this loop adds as many records to the batch as possible
            for (; recordIndex < records.size(); recordIndex++) {
                ChangeEvent<Object, Object> record = records.get(recordIndex);
                LOGGER.trace("Received record '{}'", record.value());
                if (null == record.value()) {
                    continue;
                }

                EventData eventData = null;
                if (record.value() instanceof String) {
                    eventData = new EventData((String) record.value());
                }
                else if (record.value() instanceof byte[]) {
                    eventData = new EventData(getBytes(record.value()));
                }

                try {
                    if (!batch.tryAdd(eventData)) {
                        if (batch.getCount() == 0) {
                            // If we fail to add at least the very first event to the batch that is because
                            // the event's size exceeds the maxBatchSize in which case we cannot safely
                            // recover and dispatch the event, only option is to throw an exception.
                            throw new DebeziumException("Event data is too large to fit into batch");
                        }
                        // reached the maximum allowed size for the batch
                        LOGGER.trace("Maximum batch reached, dispatching {} events.", batch.getCount());
                        break;
                    }
                }
                catch (IllegalArgumentException e) {
                    // thrown by tryAdd if event data is null
                    throw new DebeziumException(e);
                }
                catch (AmqpException e) {
                    // tryAdd throws AmqpException if "eventData is larger than the maximum size of
                    // the EventDataBatch."
                    throw new DebeziumException("Event data was larger than the maximum size of the batch", e);
                }
                catch (Exception e) {
                    throw new DebeziumException(e);
                }
            }

            final int batchEventSize = batch.getCount();
            if (batchEventSize > 0) {
                try {
                    LOGGER.trace("Sending batch of {} events to Event Hubs", batchEventSize);
                    producer.send(batch);
                    LOGGER.trace("Sent record batch to Event Hubs");
                }
                catch (Exception e) {
                    throw new DebeziumException(e);
                }

                // this loop commits each record submitted in the event hubs batch
                LOGGER.trace("Marking records at index {} to {} as processed", start, recordIndex);
                for (int j = start; j < recordIndex; ++j) {
                    ChangeEvent<Object, Object> record = records.get(j);
                    try {
                        committer.markProcessed(record);
                        LOGGER.trace("Record marked processed");
                    }
                    catch (Exception e) {
                        throw new DebeziumException(e);
                    }
                }
            }
        }

        committer.markBatchFinished();
        LOGGER.trace("Batch marked finished");
    }
}
