/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.notification;

import java.util.List;
import java.util.function.Predicate;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.function.BlockingConsumer;
import io.debezium.pipeline.notification.channels.ConnectChannel;
import io.debezium.pipeline.notification.channels.NotificationChannel;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.spi.Partition;
import io.debezium.schema.SchemaFactory;

import static io.debezium.function.Predicates.not;

/**
 * This service can be used to send notification to available and enabled channels
 */
public class NotificationService<P extends Partition, O extends OffsetContext> {

    private final List<NotificationChannel> notificationChannels;
    private final List<String> enabledChannels;

    private final IncrementalSnapshotNotificationService<P, O> incrementalSnapshotNotificationService;

    public NotificationService(List<NotificationChannel> notificationChannels,
                               CommonConnectorConfig config,
                               SchemaFactory schemaFactory, BlockingConsumer<SourceRecord> consumer) {

        this.notificationChannels = notificationChannels;

        this.enabledChannels = config.getEnabledNotificationChannels();

        this.notificationChannels.stream()
                .filter(isEnabled())
                .forEach(channel -> channel.init(config));

        this.notificationChannels.stream()
                .filter(isConnectChannel())
                .forEach(channel -> ((ConnectChannel) channel).initConnectChannel(schemaFactory, consumer));

        incrementalSnapshotNotificationService = new IncrementalSnapshotNotificationService<>(this);
    }

    /**
     * This method permits to just send a notification.
     * For the channels that implements For {@link ConnectChannel} an empty Partition and Offset will be sent with the {@link SourceRecord}
     * @param notification the notification to send
     */
    public void notify(Notification notification) {

        notificationChannels.stream()
                .filter(isEnabled())
                .forEach(channel -> channel.send(notification));
    }

    /**
     * This method permits to send a notification together with offsets.
     * This make sense only for channels that implements For {@link ConnectChannel}
     * A notification is sent also to non {@link ConnectChannel}
     * @param notification the notification to send
     * @param offsets the offset to send together with Kafka {@link SourceRecord}
     */
    public void notify(Notification notification, Offsets<P, ? extends OffsetContext> offsets) {

        this.notificationChannels.stream()
                .filter(isEnabled())
                .filter(not(isConnectChannel()))
                .forEach(channel -> channel.send(notification));

        this.notificationChannels.stream()
                .filter(isEnabled())
                .filter(isConnectChannel())
                .forEach(channel -> ((ConnectChannel) channel).send(notification, offsets));
    }

    public IncrementalSnapshotNotificationService<P, O> incrementalSnapshotNotificationService() {
        return incrementalSnapshotNotificationService;
    }

    private Predicate<? super NotificationChannel> isEnabled() {
        return channel -> enabledChannels.contains(channel.name());
    }

    private Predicate<? super NotificationChannel> isConnectChannel() {
        return channel -> channel instanceof ConnectChannel;
    }
}
