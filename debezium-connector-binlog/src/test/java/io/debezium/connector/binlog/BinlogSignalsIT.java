/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.source.SourceConnector;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.engine.DebeziumEngine;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.kafka.KafkaCluster;
import io.debezium.pipeline.signal.actions.snapshotting.ExecuteSnapshot;
import io.debezium.pipeline.signal.channels.KafkaSignalChannel;
import io.debezium.util.Collect;

/**
 * @author Chris Cranford
 */
public abstract class BinlogSignalsIT<C extends SourceConnector> extends AbstractBinlogConnectorIT<C> {
    protected static final String SERVER_NAME = "is_test";
    protected static final Path SCHEMA_HISTORY_PATH = Files.createTestingPath("file-schema-history-is.txt")
            .toAbsolutePath();
    protected final UniqueDatabase DATABASE = TestHelper.getUniqueDatabase(SERVER_NAME, "incremental_snapshot-test").withDbHistoryPath(SCHEMA_HISTORY_PATH);
    protected static KafkaCluster kafka;

    @BeforeClass
    public static void startKafka() throws Exception {
        File dataDir = Files.createTestingDirectory("signal_cluster");
        Files.delete(dataDir);
        kafka = new KafkaCluster().usingDirectory(dataDir)
                .deleteDataPriorToStartup(true)
                .deleteDataUponShutdown(true)
                .addBrokers(1)
                .withKafkaConfiguration(Collect.propertiesOf(
                        "auto.create.topics.enable", "true",
                        "zookeeper.session.timeout.ms", "20000"))
                .startup();

    }

    @AfterClass
    public static void stopKafka() {
        if (kafka != null) {
            kafka.shutdown();
        }
    }

    @Before
    public void before() throws SQLException {
        stopConnector();
        DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        Files.delete(SCHEMA_HISTORY_PATH);
    }

    @After
    public void after() {
        try {
            stopConnector();
        }
        finally {
            Files.delete(SCHEMA_HISTORY_PATH);
        }
    }

    protected Configuration.Builder config() {
        return DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.INCLUDE_SQL_QUERY, true)
                .with(BinlogConnectorConfig.USER, "mysqluser")
                .with(BinlogConnectorConfig.PASSWORD, "mysqlpw")
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.NO_DATA.getValue())
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(BinlogConnectorConfig.SIGNAL_DATA_COLLECTION, DATABASE.qualifiedTableName("debezium_signal"))
                .with(CommonConnectorConfig.SIGNAL_POLL_INTERVAL_MS, 1)
                .with(BinlogConnectorConfig.INCREMENTAL_SNAPSHOT_CHUNK_SIZE, 10)
                .with(BinlogConnectorConfig.INCREMENTAL_SNAPSHOT_ALLOW_SCHEMA_CHANGES, true)
                .with(CommonConnectorConfig.SCHEMA_NAME_ADJUSTMENT_MODE, CommonConnectorConfig.SchemaNameAdjustmentMode.AVRO);
    }

    @Test
    public void givenOffsetCommitDisabledAndASignalSentWithConnectorRunning_whenConnectorComesBackUp_thenAllSignalsAreCorrectlyProcessed()
            throws ExecutionException, InterruptedException {

        final String signalTopic = "signals_topic-1";
        final LogInterceptor logInterceptor = new LogInterceptor(ExecuteSnapshot.class);
        startConnector(x -> x.with(CommonConnectorConfig.SIGNAL_ENABLED_CHANNELS, "source,kafka")
                .with(KafkaSignalChannel.SIGNAL_TOPIC, signalTopic)
                .with(KafkaSignalChannel.BOOTSTRAP_SERVERS, kafka.brokerList()));
        assertConnectorIsRunning();
        sendExecuteSnapshotKafkaSignal("b", signalTopic);
        waitForAvailableRecords(1000, TimeUnit.MILLISECONDS);
        Thread.sleep(5000);

        final SourceRecords records = consumeRecordsByTopic(2);
        assertThat(records.allRecordsInOrder()).hasSize(2);
        assertThat(logInterceptor.containsMessage("Requested 'INCREMENTAL' snapshot of data collections '[b]'")).isTrue();
    }

    @Test
    public void givenOffsetCommitEnabledAndASignalSentWithConnectorRunning_whenConnectorComesBackUp_thenAllSignalsAreCorrectlyProcessed()
            throws ExecutionException, InterruptedException {

        final String signalTopic = "signals_topic-3";
        final LogInterceptor logInterceptor = new LogInterceptor(ExecuteSnapshot.class);
        startConnector(x -> x.with(CommonConnectorConfig.SIGNAL_ENABLED_CHANNELS, "source,kafka")
                .with(KafkaSignalChannel.SIGNAL_TOPIC, signalTopic)
                .with(KafkaSignalChannel.BOOTSTRAP_SERVERS, kafka.brokerList()));
        assertConnectorIsRunning();
        sendExecuteSnapshotKafkaSignal("b", signalTopic);
        waitForAvailableRecords(1000, TimeUnit.MILLISECONDS);

        assertThat(logInterceptor.containsMessage("Requested 'INCREMENTAL' snapshot of data collections '[b]'")).isTrue();
    }

    @Test
    public void givenOffsetCommitEnabledAndMultipleSignalsSentWithConnectorRunning_whenConnectorComesBackUp_thenAllSignalsAreCorrectlyProcessed()
            throws ExecutionException, InterruptedException {

        final String signalTopic = "signals_topic-4";
        final LogInterceptor logInterceptor = new LogInterceptor(ExecuteSnapshot.class);
        sendExecuteSnapshotKafkaSignal("b", signalTopic);
        startConnector(x -> x.with(CommonConnectorConfig.SIGNAL_ENABLED_CHANNELS, "source,kafka")
                .with(KafkaSignalChannel.SIGNAL_TOPIC, signalTopic)
                .with(KafkaSignalChannel.BOOTSTRAP_SERVERS, kafka.brokerList()));
        assertConnectorIsRunning();
        waitForAvailableRecords(1000, TimeUnit.MILLISECONDS);

        stopConnector();

        sendExecuteSnapshotKafkaSignal("c", signalTopic);
        startConnector(x -> x.with(CommonConnectorConfig.SIGNAL_ENABLED_CHANNELS, "source,kafka")
                .with(KafkaSignalChannel.SIGNAL_TOPIC, signalTopic)
                .with(KafkaSignalChannel.BOOTSTRAP_SERVERS, kafka.brokerList()));

        assertConnectorIsRunning();
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName());
        waitForAvailableRecords(1000, TimeUnit.MILLISECONDS);

        assertThat(logInterceptor.getLogEntriesThatContainsMessage("Requested 'INCREMENTAL' snapshot of data collections '[b]'")).hasSize(1);
        assertThat(logInterceptor.getLogEntriesThatContainsMessage("Requested 'INCREMENTAL' snapshot of data collections '[c]'")).hasSize(1);
    }

    @Test
    public void givenOffsetCommitEnabledAndASignalSentWithConnectorNotRunning_whenConnectorComesBackUp_thenAllSignalsAreCorrectlyProcessed()
            throws ExecutionException, InterruptedException {

        final String signalTopic = "signals_topic-5";
        final LogInterceptor logInterceptor = new LogInterceptor(ExecuteSnapshot.class);
        sendExecuteSnapshotKafkaSignal("b", signalTopic);
        startConnector(x -> x.with(CommonConnectorConfig.SIGNAL_ENABLED_CHANNELS, "source,kafka")
                .with(KafkaSignalChannel.SIGNAL_TOPIC, signalTopic)
                .with(KafkaSignalChannel.BOOTSTRAP_SERVERS, kafka.brokerList()));
        assertConnectorIsRunning();

        assertThat(logInterceptor.containsMessage("Requested 'INCREMENTAL' snapshot of data collections '[b]'")).isTrue();
    }

    protected void startConnector(Function<Configuration.Builder, Configuration.Builder> custConfig) {
        startConnector(custConfig, loggingCompletion());
    }

    protected void startConnector(Function<Configuration.Builder, Configuration.Builder> custConfig,
                                  DebeziumEngine.CompletionCallback callback) {
        final Configuration config = custConfig.apply(config()).build();
        start(getConnectorClass(), config, callback);
        assertConnectorIsRunning();

        waitForAvailableRecords(5, TimeUnit.SECONDS);
    }

    protected void sendExecuteSnapshotKafkaSignal(final String table, String signalTopic) throws ExecutionException, InterruptedException {
        String signalValue = String.format(
                "{\"type\":\"execute-snapshot\",\"data\": {\"data-collections\": [\"%s\"], \"type\": \"INCREMENTAL\"}}",
                table);
        sendKafkaSignal(signalValue, signalTopic);
    }

    protected void sendKafkaSignal(String signalValue, String signalTopic) throws ExecutionException, InterruptedException {
        final ProducerRecord<String, String> executeSnapshotSignal = new ProducerRecord<>(signalTopic, 0, SERVER_NAME, signalValue);

        final Configuration signalProducerConfig = Configuration.create()
                .withDefault(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.brokerList())
                .withDefault(ProducerConfig.CLIENT_ID_CONFIG, "signals")
                .withDefault(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                .withDefault(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                .build();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(signalProducerConfig.asProperties())) {
            producer.send(executeSnapshotSignal).get();
        }
    }

}
