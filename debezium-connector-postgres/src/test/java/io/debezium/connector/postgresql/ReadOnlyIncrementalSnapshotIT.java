/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static io.debezium.junit.EqualityCheck.LESS_THAN;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.junit.SkipWhenDatabaseVersion;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.pipeline.signal.actions.AbstractSnapshotSignal;
import io.debezium.pipeline.signal.actions.snapshotting.ExecuteSnapshot;
import io.debezium.pipeline.signal.actions.snapshotting.StopSnapshot;
import io.debezium.pipeline.signal.channels.KafkaSignalChannel;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.util.Strings;

@SkipWhenDatabaseVersion(check = LESS_THAN, major = 13, minor = 0, reason = "Function pg_current_snapshot() not supported until PostgreSQL 13")
public class ReadOnlyIncrementalSnapshotIT extends IncrementalSnapshotIT {

    private final LogInterceptor executeSignalInterceptor = new LogInterceptor(ExecuteSnapshot.class);
    private final LogInterceptor stopSignalInterceptor = new LogInterceptor(StopSnapshot.class);

    @Override
    protected Configuration.Builder config() {
        return TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.NO_DATA.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                .with(PostgresConnectorConfig.INCREMENTAL_SNAPSHOT_CHUNK_SIZE, 10)
                .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, "s1")
                .with(CommonConnectorConfig.SIGNAL_ENABLED_CHANNELS, "kafka")
                .with(PostgresConnectorConfig.READ_ONLY_CONNECTION, true)
                .with(KafkaSignalChannel.BOOTSTRAP_SERVERS, kafka.brokerList())
                .with(KafkaSignalChannel.SIGNAL_TOPIC, getSignalsTopic())
                .with(CommonConnectorConfig.SIGNAL_POLL_INTERVAL_MS, 5)
                .with(RelationalDatabaseConnectorConfig.MSG_KEY_COLUMNS, "s1.a42:pk1,pk2,pk3,pk4")
                // DBZ-4272 required to allow dropping columns just before an incremental snapshot
                .with("database.autosave", "conservative");
    }

    @Override
    protected Configuration.Builder mutableConfig(boolean signalTableOnly, boolean storeOnlyCapturedDdl) {
        final String tableIncludeList;
        if (signalTableOnly) {
            tableIncludeList = "s1.b";
        }
        else {
            tableIncludeList = "s1.a,s1.b";
        }
        return TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.NO_DATA.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                .with(CommonConnectorConfig.SIGNAL_POLL_INTERVAL_MS, 5)
                .with(PostgresConnectorConfig.INCREMENTAL_SNAPSHOT_CHUNK_SIZE, 10)
                .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, "s1")
                .with(CommonConnectorConfig.SIGNAL_ENABLED_CHANNELS, "kafka")
                .with(PostgresConnectorConfig.READ_ONLY_CONNECTION, true)
                .with(KafkaSignalChannel.BOOTSTRAP_SERVERS, kafka.brokerList())
                .with(KafkaSignalChannel.SIGNAL_TOPIC, getSignalsTopic())
                .with(RelationalDatabaseConnectorConfig.MSG_KEY_COLUMNS, "s1.a42:pk1,pk2,pk3,pk4")
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, tableIncludeList)
                // DBZ-4272 required to allow dropping columns just before an incremental snapshot
                .with("database.autosave", "conservative");
    }

    @Override
    protected void sendAdHocSnapshotSignal(String... dataCollectionIds) {

        String signalValue = String.format(
                "{\"id\":\"ad-hoc\", \"type\":\"execute-snapshot\",\"data\": {\"data-collections\": [\"%s\"], \"type\": \"INCREMENTAL\"}}",
                String.join("\",\"", dataCollectionIds));
        try {
            sendKafkaSignal(signalValue);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to send signal", e);
        }
    }

    @Override
    protected void sendAdHocSnapshotStopSignal(String... dataCollectionIds) {

        String signalValue = "{\"type\":\"stop-snapshot\",\"data\": {\"type\": \"INCREMENTAL\"}}";

        if (dataCollectionIds.length > 0) {

            signalValue = String.format(
                    "{\"type\":\"stop-snapshot\",\"data\": {\"data-collections\": [\"%s\"], \"type\": \"INCREMENTAL\"}}",
                    String.join(",", dataCollectionIds));
        }

        try {
            sendKafkaSignal(signalValue);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to send signal", e);
        }
    }

    @Override
    protected void sendPauseSignal() {
        try {
            sendKafkaSignal("{\"type\":\"pause-snapshot\",\"data\": {\"type\": \"INCREMENTAL\"}}");
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to send resume signal", e);
        }
    }

    @Override
    protected void sendResumeSignal() {
        try {
            sendKafkaSignal("{\"type\":\"resume-snapshot\",\"data\": {\"type\": \"INCREMENTAL\"}}");
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to send pause signal", e);
        }
    }

    @Override
    protected void sendAdHocSnapshotSignalWithAdditionalConditionWithSurrogateKey(String additionalCondition, String surrogateKey,
                                                                                  AbstractSnapshotSignal.SnapshotType snapshotType,
                                                                                  String... dataCollectionIds) {
        final String dataCollectionIdsList = Arrays.stream(dataCollectionIds)
                .map(x -> '"' + x + '"')
                .collect(Collectors.joining(", "));

        String signalValue;
        if (!Strings.isNullOrEmpty(additionalCondition) && !Strings.isNullOrEmpty(surrogateKey)) {
            signalValue = String.format(
                    "{\"type\":\"execute-snapshot\",\"data\": {\"type\": \"%s\",\"data-collections\": [%s], \"additional-conditions\": [%s], \"surrogate-key\": %s}}",
                    snapshotType.toString(), dataCollectionIdsList, additionalCondition, surrogateKey);
        }
        else if (!Strings.isNullOrEmpty(additionalCondition)) {
            signalValue = String.format(
                    "{\"type\":\"execute-snapshot\",\"data\": {\"type\": \"%s\",\"data-collections\": [%s], \"additional-conditions\": [%s]}}",
                    snapshotType.toString(), dataCollectionIdsList, additionalCondition);
        }
        else if (!Strings.isNullOrEmpty(surrogateKey)) {
            signalValue = String.format(
                    "{\"type\":\"execute-snapshot\",\"data\": {\"type\": \"%s\",\"data-collections\": [%s], \"surrogate-key\": %s}} `",
                    snapshotType.toString(), dataCollectionIdsList, surrogateKey);
        }
        else {
            signalValue = String.format(
                    "{\"type\":\"execute-snapshot\",\"data\": {\"type\": \"%s\",\"data-collections\": [%s]}}",
                    snapshotType.toString(), dataCollectionIdsList);
        }

        try {
            logger.info("Sending signal with message {}", signalValue);
            sendKafkaSignal(signalValue);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to send signal", e);
        }
    }

    @Override
    protected void sendAdHocSnapshotSignalWithAdditionalConditionsWithSurrogateKey(Map<String, String> additionalConditions, String surrogateKey,
                                                                                   AbstractSnapshotSignal.SnapshotType snapshotType,
                                                                                   String... dataCollectionIds) {

        sendAdHocSnapshotSignalWithAdditionalConditionWithSurrogateKey(buildAdditionalConditions(additionalConditions), surrogateKey, snapshotType, dataCollectionIds);
    }

    @Override
    protected Callable<Boolean> executeSignalWaiter() {
        return () -> executeSignalInterceptor.containsMessage("Requested 'INCREMENTAL' snapshot of data collections");
    }

    @Override
    protected Callable<Boolean> stopSignalWaiter() {
        return () -> stopSignalInterceptor.containsMessage("Requested stop of snapshot");
    }

    @Test
    @Override
    public void insertInsertWatermarkingStrategy() throws Exception {
        // test has not to be executed on read only
    }

    @Test
    @Override
    public void insertDeleteWatermarkingStrategy() throws Exception {
        // test has not to be executed on read only
    }

    @Override
    protected void assertExpectedRecordsEnumPk(List<String> enumValues) throws InterruptedException {

        waitForAvailableRecords(5000, TimeUnit.SECONDS);

        final var records = consumeRecordsByTopic(enumValues.size()).allRecordsInOrder();
        for (int i = 0; i < enumValues.size(); i++) {
            var record = records.get(i);
            assertThat(((Struct) record.key()).getString("pk")).isEqualTo(enumValues.get(i));
            assertThat(((Struct) record.value()).getStruct("after").getInt32("aa")).isEqualTo(i);
        }
    }

    @Override
    protected Function<Configuration.Builder, Configuration.Builder> additionalConfiguration() {
        return x -> x.with(CommonConnectorConfig.INCREMENTAL_SNAPSHOT_CHUNK_SIZE, 1)
                .with(Heartbeat.HEARTBEAT_INTERVAL_PROPERTY_NAME, 5000);
    }
}
