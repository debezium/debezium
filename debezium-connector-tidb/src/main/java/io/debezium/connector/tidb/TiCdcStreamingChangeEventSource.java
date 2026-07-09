/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.tidb;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.tidb.ticdc.TiCdcEvent;
import io.debezium.connector.tidb.ticdc.TiCdcEventParser;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

/**
 * Streams change events from the Kafka topics written by a TiCDC changefeed running with
 * {@code protocol=debezium}.
 * <p>
 * This is the first phase of the TiDB connector agreed in DBZ-6269: TiCDC remains the component
 * that reads TiKV's event stream and resolves raw key-values into rows, while Debezium provides
 * connector lifecycle management, offset tracking and the standard event envelope. A later phase
 * may replace this source with a direct TiKV {@code EventFeed} client without affecting the rest
 * of the connector.
 * <p>
 * The consumer uses manual partition assignment; progress is tracked exclusively through the
 * Kafka Connect offsets of this connector, no consumer group offsets are committed.
 *
 * @author Aviral Srivastava
 */
public class TiCdcStreamingChangeEventSource implements StreamingChangeEventSource<TiDbPartition, TiDbOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TiCdcStreamingChangeEventSource.class);

    private final TiDbConnectorConfig connectorConfig;
    private final EventDispatcher<TiDbPartition, TableId> dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final TiDbSchema schema;

    private volatile TiDbOffsetContext effectiveOffsetContext;

    public TiCdcStreamingChangeEventSource(TiDbConnectorConfig connectorConfig,
                                           EventDispatcher<TiDbPartition, TableId> dispatcher,
                                           ErrorHandler errorHandler, Clock clock, TiDbSchema schema) {
        this.connectorConfig = connectorConfig;
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.schema = schema;
    }

    @Override
    public void init(TiDbOffsetContext offsetContext) {
        this.effectiveOffsetContext = offsetContext != null ? offsetContext : TiDbOffsetContext.empty(connectorConfig);
    }

    @Override
    public void execute(ChangeEventSourceContext context, TiDbPartition partition, TiDbOffsetContext offsetContext)
            throws InterruptedException {
        final TiDbOffsetContext effectiveOffset = this.effectiveOffsetContext;

        try (TiCdcEventParser parser = new TiCdcEventParser();
                Consumer<byte[], byte[]> consumer = createConsumer()) {

            seekToStartPosition(consumer, effectiveOffset);

            while (context.isRunning()) {
                final ConsumerRecords<byte[], byte[]> records = consumer.poll(connectorConfig.getTicdcPollTimeout());
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    if (!context.isRunning()) {
                        return;
                    }
                    processRecord(parser, partition, effectiveOffset, record);
                }
            }
        }
        catch (InterruptedException e) {
            throw e;
        }
        catch (Throwable t) {
            errorHandler.setProducerThrowable(t);
        }
    }

    private void processRecord(TiCdcEventParser parser, TiDbPartition partition, TiDbOffsetContext offsetContext,
                               ConsumerRecord<byte[], byte[]> record)
            throws InterruptedException {
        final TiCdcEvent event = parser.parse(record.topic(), record.key(), record.value());
        if (event == null) {
            // Still advance the stream position so non data change messages are not re-read on restart
            offsetContext.event(null, clock.currentTimeAsInstant(), offsetContext.getCommitTs(), null,
                    record.topic(), record.partition(), record.offset() + 1);
            return;
        }

        final TableId tableId = event.tableId();
        offsetContext.event(tableId, event.sourceTimestamp(), event.commitTs(), event.clusterId(),
                record.topic(), record.partition(), record.offset() + 1);
        schema.refresh(tableId, event.keySchema(), event.rowSchema());

        dispatcher.dispatchDataChangeEvent(partition, tableId,
                new TiDbChangeRecordEmitter(partition, offsetContext, clock, connectorConfig, event));
    }

    /**
     * Creates the Kafka consumer reading the TiCDC topics. Visible so that tests can substitute
     * a mock consumer.
     */
    protected Consumer<byte[], byte[]> createConsumer() {
        final Properties props = connectorConfig.getTicdcConsumerProperties();
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.putIfAbsent(ConsumerConfig.CLIENT_ID_CONFIG, "debezium-tidb-" + connectorConfig.getLogicalName());
        return new KafkaConsumer<>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    private void seekToStartPosition(Consumer<byte[], byte[]> consumer, TiDbOffsetContext offsetContext) {
        final List<TopicPartition> assignment = new ArrayList<>();
        for (String topic : connectorConfig.getTicdcTopics()) {
            final List<PartitionInfo> partitions = consumer.partitionsFor(topic);
            if (partitions == null || partitions.isEmpty()) {
                throw new DebeziumException("TiCDC topic '" + topic + "' does not exist or has no partitions. "
                        + "Verify that the TiCDC changefeed is running and writing to the configured topics");
            }
            for (PartitionInfo partition : partitions) {
                assignment.add(new TopicPartition(partition.topic(), partition.partition()));
            }
        }
        consumer.assign(assignment);

        final List<TopicPartition> withoutStoredOffset = new ArrayList<>();
        for (TopicPartition topicPartition : assignment) {
            offsetContext.nextOffsetFor(topicPartition.topic(), topicPartition.partition())
                    .ifPresentOrElse(
                            nextOffset -> consumer.seek(topicPartition, nextOffset),
                            () -> withoutStoredOffset.add(topicPartition));
        }
        if (!withoutStoredOffset.isEmpty()) {
            if (connectorConfig.getTicdcInitialOffset() == TiDbConnectorConfig.InitialOffset.LATEST) {
                consumer.seekToEnd(withoutStoredOffset);
            }
            else {
                consumer.seekToBeginning(withoutStoredOffset);
            }
        }
        LOGGER.info("Consuming TiCDC stream from {} topic partition(s): {}", assignment.size(), assignment);
    }

    @Override
    public TiDbOffsetContext getOffsetContext() {
        return effectiveOffsetContext;
    }
}
