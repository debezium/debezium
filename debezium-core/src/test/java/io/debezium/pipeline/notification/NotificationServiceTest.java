/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.notification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.pipeline.notification.channels.LogNotificationChannel;
import io.debezium.pipeline.notification.channels.SinkNotificationChannel;
import io.debezium.schema.SchemaFactory;

@RunWith(MockitoJUnitRunner.class)
public class NotificationServiceTest {

    public static final String NOTIFICATION_ID = UUID.fromString("a5dc3ab8-933d-4aae-a994-2e5f9d47acd2").toString();
    private final SourceRecord expectedRecord = buildRecord();
    private boolean isConsumerCalled = false;

    @Mock
    private CommonConnectorConfig connectorConfig;

    private SourceRecord buildRecord() {

        Schema keySchema = SchemaBuilder.struct()
                .name("io.debezium.connector.common.NotificationKey")
                .field("id", SchemaBuilder.STRING_SCHEMA)
                .build();

        Schema valueSchema = SchemaBuilder.struct()
                .name("io.debezium.connector.common.Notification")
                .field("id", SchemaBuilder.STRING_SCHEMA)
                .field("type", SchemaBuilder.STRING_SCHEMA)
                .field("aggregate_type", SchemaBuilder.STRING_SCHEMA)
                .field("additional_data", SchemaBuilder.map(SchemaBuilder.STRING_SCHEMA, SchemaBuilder.STRING_SCHEMA))
                .build();

        Struct key = new Struct(keySchema).put("id", NOTIFICATION_ID);
        Struct value = new Struct(valueSchema)
                .put("id", NOTIFICATION_ID)
                .put("type", "Test")
                .put("aggregate_type", "Test")
                .put("additional_data", Map.of("k1", "v1"));

        return new SourceRecord(Map.of(), Map.of(), "notificationTopic", null, keySchema, key, valueSchema, value);
    }

    @Test
    public void testNotificationWithLogNotificationChannel() {

        when(connectorConfig.getEnabledNotificationChannels()).thenReturn(List.of("log"));

        LogInterceptor logInterceptor = new LogInterceptor(LogNotificationChannel.class);

        NotificationService<?, ?> notificationService = new NotificationService<>(List.of(new LogNotificationChannel()), connectorConfig,
                new SchemaFactory(),
                this::consume);

        notificationService.notify(Notification.Builder.builder()
                .withId(NOTIFICATION_ID)
                .withType("Test")
                .withAggregateType("Test")
                .withAdditionalData(Map.of("Key1", "Value1"))
                .build());

        assertThat(logInterceptor.containsMessage("[Notification Service]  {aggregateType='Test', type='Test', additionalData={Key1=Value1}}")).isTrue();
    }

    @Test
    public void notificationSentOnKafkaChannelWillBeCorrectlyProcessed() {

        when(connectorConfig.getNotificationTopic()).thenReturn("io.debezium.notification");
        when(connectorConfig.getEnabledNotificationChannels()).thenReturn(List.of("sink"));

        NotificationService<?, ?> notificationService = new NotificationService<>(List.of(new SinkNotificationChannel()), connectorConfig,
                new SchemaFactory(), this::consume);

        notificationService.notify(Notification.Builder.builder()
                .withId(NOTIFICATION_ID)
                .withType("Test")
                .withAggregateType("Test")
                .withAdditionalData(Map.of("k1", "v1"))
                .build());

        assertThat(isConsumerCalled).isTrue();
    }

    private void consume(SourceRecord sourceRecord) {
        Struct value = (Struct) sourceRecord.value();
        Struct expectedValue = (Struct) expectedRecord.value();
        assertThat(value.toString()).isEqualTo(expectedValue.toString());
        Struct key = (Struct) sourceRecord.key();
        Struct expectedKey = (Struct) expectedRecord.key();
        assertThat(key.toString()).isEqualTo(expectedKey.toString());
        assertThat(sourceRecord.topic()).isEqualTo("io.debezium.notification");
        isConsumerCalled = true;
    }
}
