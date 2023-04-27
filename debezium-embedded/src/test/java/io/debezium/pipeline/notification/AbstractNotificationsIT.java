/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.notification;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.pipeline.notification.channels.SinkNotificationChannel;

public abstract class AbstractNotificationsIT<T extends SourceConnector> extends AbstractConnectorTest {

    protected abstract Class<T> connectorClass();

    protected abstract Configuration.Builder config();

    protected void startConnector(Function<Configuration.Builder, Configuration.Builder> custConfig) {

        final Configuration config = custConfig.apply(config()).build();

        start(connectorClass(), config);
    }

    protected abstract String snapshotStatusResult();

    @Test
    public void notificationCorrectlySentOnItsTopic() throws InterruptedException {
        // Testing.Print.enable();

        startConnector(config -> config
                .with(SinkNotificationChannel.NOTIFICATION_TOPIC, "io.debezium.notification")
                .with(CommonConnectorConfig.NOTIFICATION_ENABLED_CHANNELS, "sink"));

        assertConnectorIsRunning();

        waitForAvailableRecords(100, TimeUnit.MILLISECONDS);

        final SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.allRecordsInOrder()).hasSize(1);
        SourceRecord sourceRecord = records.allRecordsInOrder().get(0);
        Assertions.assertThat(sourceRecord.topic()).isEqualTo("io.debezium.notification");
        Assertions.assertThat(((Struct) sourceRecord.value()).getString("aggregate_type")).isEqualTo("Initial Snapshot");
        Assertions.assertThat(((Struct) sourceRecord.value()).getString("type")).isEqualTo("Status " + snapshotStatusResult());
    }

    @Test
    public void notificationNotSentIfNoChannelIsConfigured() {
        // Testing.Print.enable();

        startConnector(config -> config.with(SinkNotificationChannel.NOTIFICATION_TOPIC, "io.debezium.notification"));
        assertConnectorIsRunning();

        waitForAvailableRecords(100, TimeUnit.MILLISECONDS);

        // there shouldn't be any snapshot records
        assertNoRecordsToConsume();
    }

    @Test
    public void reportErrorWhenSinkChannelIsEnabledAndNoTopicConfigurationProvided() {
        // Testing.Print.enable();

        LogInterceptor logInterceptor = new LogInterceptor("io.debezium.connector");
        startConnector(config -> config
                .with(CommonConnectorConfig.NOTIFICATION_ENABLED_CHANNELS, "sink"));

        Assertions.assertThat(logInterceptor.containsErrorMessage(
                "Connector configuration is not valid. The 'notification.sink.topic.name' value is invalid: Notification topic name must be provided when kafka notification channel is enabled"))
                .isTrue();
    }
}
