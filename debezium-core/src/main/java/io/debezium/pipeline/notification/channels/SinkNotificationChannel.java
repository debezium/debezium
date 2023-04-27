/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.notification.channels;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.SnapshotRecord;
import io.debezium.function.BlockingConsumer;
import io.debezium.pipeline.notification.Notification;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.spi.Partition;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.schema.SchemaFactory;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.schema.DataCollectionId;

public class SinkNotificationChannel implements NotificationChannel, ConnectChannel {

    private static final Logger LOGGER = LoggerFactory.getLogger(SinkNotificationChannel.class);

    public static final Field NOTIFICATION_TOPIC = Field.create(CommonConnectorConfig.NOTIFICATION_CONFIGURATION_FIELD_PREFIX_STRING + "sink.topic.name")
            .withDisplayName("Notification topic name")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("The name of the topic for the notifications. This is required in case 'sink' is in the list of enabled channels")
            .withValidation(SinkNotificationChannel::validateNotificationTopicName);

    public static final String CHANNEL_NAME = "sink";
    private BlockingConsumer<SourceRecord> consumer;
    private Schema keySchema;
    private Schema valueSchema;
    private String topicName;

    private static int validateNotificationTopicName(Configuration config, Field field, Field.ValidationOutput problems) {

        String topicName = config.getString(field);
        boolean isKafkaChannelEnabled = config.getList(CommonConnectorConfig.NOTIFICATION_ENABLED_CHANNELS).contains(CHANNEL_NAME);
        int count = 0;
        if (isKafkaChannelEnabled && topicName == null) {
            problems.accept(field, topicName, "Notification topic name must be provided when kafka notification channel is enabled");
            ++count;
        }

        return count;
    }

    @Override
    public void init(CommonConnectorConfig config) {

        this.topicName = config.getNotificationTopic();
    }

    @Override
    public void initConnectChannel(SchemaFactory schemaFactory, BlockingConsumer<SourceRecord> consumer) {

        this.consumer = consumer;
        this.keySchema = schemaFactory.notificationKeySchema(SchemaNameAdjuster.NO_OP);
        this.valueSchema = schemaFactory.notificationValueSchema(SchemaNameAdjuster.NO_OP);
    }

    @Override
    public String name() {
        return CHANNEL_NAME;
    }

    @Override
    public void send(Notification notification) {

        send(notification, Offsets.of(Collections.singletonMap(Map::of, new EmptyOffsetContext())));
    }

    @Override
    public <P extends Partition, O extends OffsetContext> void send(Notification notification, Offsets<P, O> offsets) {

        Struct keyValue = new Struct(keySchema).put(Notification.ID_KEY, notification.getId());
        Struct value = new Struct(valueSchema)
                .put(Notification.ID_KEY, notification.getId())
                .put(Notification.TYPE, notification.getType())
                .put(Notification.AGGREGATE_TYPE, notification.getAggregateType())
                .put(Notification.ADDITIONAL_DATA, notification.getAdditionalData());

        // When connector is started for the first time and SnapshotMode.NEVER
        Map<String, ?> offset = offsets.getTheOnlyOffset() == null ? Map.of() : offsets.getTheOnlyOffset().getOffset();

        SourceRecord sourceRecord = new SourceRecord(offsets.getTheOnlyPartition().getSourcePartition(), offset, topicName,
                keySchema, keyValue,
                valueSchema, value);

        try {
            consumer.accept(sourceRecord); // This sends the record to the ChangeEventQueue
        }
        catch (InterruptedException e) {
            LOGGER.error("Notification {} not sent due to interrupt", notification);
        }
    }

    @Override
    public void close() {
    }

    private static class EmptyOffsetContext implements OffsetContext {

        @Override
        public Map<String, ?> getOffset() {
            return Map.of();
        }

        @Override
        public Schema getSourceInfoSchema() {
            return null;
        }

        @Override
        public Struct getSourceInfo() {
            return null;
        }

        @Override
        public boolean isSnapshotRunning() {
            return false;
        }

        @Override
        public void markSnapshotRecord(SnapshotRecord record) {

        }

        @Override
        public void preSnapshotStart() {

        }

        @Override
        public void preSnapshotCompletion() {

        }

        @Override
        public void postSnapshotCompletion() {

        }

        @Override
        public void event(DataCollectionId collectionId, Instant timestamp) {

        }

        @Override
        public TransactionContext getTransactionContext() {
            return null;
        }
    }
}
