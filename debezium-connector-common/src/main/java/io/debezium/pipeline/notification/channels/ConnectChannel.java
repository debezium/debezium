/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.notification.channels;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.function.BlockingConsumer;
import io.debezium.pipeline.notification.Notification;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.spi.Partition;
import io.debezium.schema.SchemaFactory;

/**
 * A special channel that is strictly connected to Kafka Connect API
 */
public interface ConnectChannel {

    void initConnectChannel(SchemaFactory schemaFactory, BlockingConsumer<SourceRecord> consumer);

    <P extends Partition, O extends OffsetContext> void send(Notification notification, Offsets<P, O> offsets);
}
