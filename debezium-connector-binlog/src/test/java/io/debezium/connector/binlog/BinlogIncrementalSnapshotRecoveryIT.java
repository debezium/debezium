/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.sql.SQLException;
import java.sql.Types;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.binlog.jdbc.BinlogConnectorConnection;
import io.debezium.connector.binlog.util.BinlogTestConnection;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.common.DebeziumHeaderProducer;
import io.debezium.converters.custom.CustomConverterServiceProvider;
import io.debezium.doc.FixFor;
import io.debezium.document.DocumentReader;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.signal.SignalRecord;
import io.debezium.pipeline.signal.actions.snapshotting.CloseIncrementalSnapshotWindow;
import io.debezium.pipeline.signal.actions.snapshotting.OpenIncrementalSnapshotWindow;
import io.debezium.pipeline.signal.actions.snapshotting.ResumeIncrementalSnapshot;
import io.debezium.pipeline.signal.channels.SourceSignalChannel;
import io.debezium.pipeline.source.snapshot.incremental.AbstractIncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaFactory;
import io.debezium.util.Clock;
import io.debezium.util.LoggingContext;

/**
 * Exercises schema-cache races deterministically with real JDBC queries and snapshot records.
 * Watermark callbacks and the timing of cached schema updates are controlled by the test.
 */
@Timeout(30)
public abstract class BinlogIncrementalSnapshotRecoveryIT<C extends SourceConnector, P extends BinlogPartition, O extends BinlogOffsetContext<?>>
        implements BinlogConnectorTest<C> {

    protected final UniqueDatabase database = TestHelper.getUniqueDatabase("recovery", "incremental_recovery");
    protected BinlogConnectorConfig config;
    protected BinlogConnectorConnection jdbc;
    protected BinlogDatabaseSchema<P, O, ?, ?> schema;
    protected P partition;
    protected O offset;
    protected EventDispatcher<P, TableId> dispatcher;
    protected NotificationService<P, O> notifications;
    private BinlogTestConnection observer;
    private ChangeEventQueue<DataChangeEvent> queue;
    private AbstractIncrementalSnapshotChangeEventSource<P, TableId> source;
    private IncrementalSnapshotContext<TableId> context;
    private boolean readOnly;
    private SignalProcessor<P, O> signalProcessor;
    private SourceSignalChannel signalChannel;
    private ErrorHandler errorHandler;

    protected abstract BinlogConnectorConfig createConfig(Configuration configuration);

    protected abstract BinlogConnectorConnection createConnection(Configuration configuration);

    protected abstract CdcSourceTaskContext<?> createTaskContext(Configuration configuration);

    protected abstract BinlogDatabaseSchema<P, O, ?, ?> createSchema(CdcSourceTaskContext<?> taskContext);

    protected abstract P createPartition();

    protected abstract O createOffset();

    protected abstract AbstractIncrementalSnapshotChangeEventSource<P, TableId> createReadOnlySource();

    @BeforeEach
    void createTables() throws SQLException {
        database.create();
        observer = getTestDatabaseConnection(database.getDatabaseName());
        observer.execute("CREATE TABLE a(pk INT PRIMARY KEY, aa INT, c INT)",
                "INSERT INTO a VALUES (1,10,101),(2,20,102),(3,30,103),(4,40,104),(5,50,105)",
                "CREATE TABLE b(pk INT PRIMARY KEY, aa INT, c INT)",
                "INSERT INTO b VALUES (11,110,111),(12,120,112),(13,130,113)",
                "CREATE TABLE signal_table(id VARCHAR(100) PRIMARY KEY, type VARCHAR(100), data VARCHAR(2000))",
                "CREATE TABLE ticks(id INT AUTO_INCREMENT PRIMARY KEY)",
                "SET SESSION lock_wait_timeout=2");
    }

    @AfterEach
    void closeResources() throws Exception {
        try {
            if (signalProcessor != null) {
                signalProcessor.stop();
            }
            if (jdbc != null) {
                jdbc.close();
            }
        }
        finally {
            if (dispatcher != null) {
                dispatcher.close();
            }
            if (queue != null) {
                queue.close();
            }
            if (schema != null) {
                schema.close();
            }
            if (observer != null) {
                observer.close();
            }
        }
    }

    @ParameterizedTest
    @FixFor("debezium/dbz#2604")
    @ValueSource(strings = { "read-only", "insert_insert", "insert_delete" })
    void shouldRecoverFirstChunkAndCompleteNextTable(String mode) throws Exception {
        initialize(mode, true);
        schema.refresh(table("a", false));
        context.maximumKey(new Object[]{ 5 });
        seedVerifiedSchema();

        source.init(partition, offset);

        assertRetryPreservesTableAndReleasesLock();
        schema.refresh(table("a", true));
        assertAllRows(completeSnapshot());
    }

    @ParameterizedTest
    @FixFor("debezium/dbz#2604")
    @ValueSource(strings = { "read-only", "insert_insert", "insert_delete" })
    void shouldRecoverMaximumKeyQuery(String mode) throws Exception {
        initialize(mode, true);
        schema.refresh(table("a", false));
        seedVerifiedSchema();

        source.init(partition, offset);

        assertThat(context.maximumKey()).isEmpty();
        assertRetryPreservesTableAndReleasesLock();
        schema.refresh(table("a", true));
        assertAllRows(completeSnapshot());
    }

    @ParameterizedTest
    @FixFor("debezium/dbz#2604")
    @ValueSource(strings = { "read-only", "insert_insert", "insert_delete" })
    void shouldRetryFromLastEmittedKey(String mode) throws Exception {
        initialize(mode, true);
        // Model a resumed snapshot whose first chunk (keys 1 and 2) was already emitted.
        context.sendEvent(new Object[]{ 2 });
        context.nextChunkPosition(new Object[]{ 2 });
        context.maximumKey(new Object[]{ 5 });
        schema.refresh(table("a", false));
        seedVerifiedSchema();

        source.init(partition, offset);

        assertRetryPreservesTableAndReleasesLock();
        assertThat(context.chunkEndPosititon()).containsExactly(2);
        schema.refresh(table("a", true));
        final var records = completeSnapshot();
        assertRows(records, List.of(3, 4, 5, 11, 12, 13));
    }

    @ParameterizedTest
    @FixFor("debezium/dbz#2604")
    @ValueSource(strings = { "read-only", "insert_insert", "insert_delete" })
    void shouldPreserveNormalSnapshotBehavior(String mode) throws Exception {
        initialize(mode, true);
        source.init(partition, offset);
        assertAllRows(completeSnapshot());
    }

    @ParameterizedTest
    @FixFor("debezium/dbz#2604")
    @ValueSource(strings = { "read-only", "insert_insert", "insert_delete" })
    void shouldReleaseFailedReadTransactionWhenRecoveryIsDisabled(String mode) throws Exception {
        initialize(mode, false);
        schema.refresh(table("a", false));
        context.maximumKey(new Object[]{ 5 });

        source.init(partition, offset);

        assertThat(context.currentDataCollectionId().getId()).isEqualTo(tableId("b"));
        assertThat(queue.poll()).isEmpty();
        assertDdlIsNotBlocked();
    }

    @ParameterizedTest
    @FixFor("debezium/dbz#2604")
    @ValueSource(strings = { "read-only", "insert_insert", "insert_delete" })
    void shouldKeepRetryingPersistentMismatchWithoutSkippingTables(String mode) throws Exception {
        initialize(mode, true);
        schema.refresh(table("a", false));
        context.maximumKey(new Object[]{ 5 });
        seedVerifiedSchema();
        source.init(partition, offset);

        for (int i = 0; i < 5; i++) {
            advanceWatermark();
            assertRetryPreservesTableAndReleasesLock();
            assertThat(errorHandler.getProducerThrowable()).isNull();
        }

        schema.refresh(table("a", true));
        assertAllRows(completeSnapshot());
    }

    @ParameterizedTest
    @FixFor("debezium/dbz#2604")
    @ValueSource(strings = { "insert_insert", "insert_delete" })
    void shouldReportRecoveryWindowFailureThroughSignalProcessor(String mode) throws Exception {
        initialize(mode, true);
        source.setErrorHandler(errorHandler);
        schema.refresh(table("a", false));
        context.maximumKey(new Object[]{ 5 });
        seedVerifiedSchema();
        if (mode.equals("insert_insert")) {
            observer.execute("CREATE TRIGGER fail_close BEFORE INSERT ON signal_table FOR EACH ROW "
                    + "BEGIN IF NEW.type = 'snapshot-window-close' THEN "
                    + "SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Injected window close failure'; END IF; END");
        }
        else {
            observer.execute("CREATE TRIGGER fail_close BEFORE DELETE ON signal_table FOR EACH ROW "
                    + "SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Injected window close failure'");
        }
        context.pauseSnapshot();
        source.init(partition, offset);

        // SignalProcessor catches action exceptions; the task must still see the recovery failure.
        processSignal("resume", ResumeIncrementalSnapshot.NAME);

        assertThat(context.snapshotRunning()).isTrue();
        assertThat(context.currentDataCollectionId().getId()).isEqualTo(tableId("a"));
        assertThat(errorHandler.getProducerThrowable()).hasMessageContaining("Could not close incremental snapshot window");
        assertThatThrownBy(() -> queue.poll()).isInstanceOf(ConnectException.class);
        assertDdlIsNotBlocked();
    }

    @SuppressWarnings("unchecked")
    private void initialize(String mode, boolean schemaChanges) throws Exception {
        readOnly = mode.equals("read-only");
        final var configuration = database.defaultConfig()
                .with(BinlogConnectorConfig.USER, "mysqluser")
                .with(BinlogConnectorConfig.PASSWORD, "mysqlpw")
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(BinlogConnectorConfig.READ_ONLY_CONNECTION, readOnly)
                .with(BinlogConnectorConfig.INCREMENTAL_SNAPSHOT_CHUNK_SIZE, 2)
                .with(BinlogConnectorConfig.INCREMENTAL_SNAPSHOT_ALLOW_SCHEMA_CHANGES, schemaChanges)
                .with(BinlogConnectorConfig.SIGNAL_DATA_COLLECTION, database.qualifiedTableName("signal_table"))
                .with(BinlogConnectorConfig.SIGNAL_EMIT_FAILURE_MAX_RETRIES, 0)
                .with(BinlogConnectorConfig.INCREMENTAL_SNAPSHOT_WATERMARKING_STRATEGY, readOnly ? "insert_insert" : mode)
                .with("schema.history.internal", "io.debezium.relational.history.MemorySchemaHistory")
                .build();
        config = createConfig(configuration);
        config.getServiceRegistry().registerServiceProvider(new CustomConverterServiceProvider());
        jdbc = createConnection(configuration);
        jdbc.connect();
        assumeTrue(!readOnly || jdbc.isGtidModeEnabled(), "Read-only incremental snapshots require GTID mode");
        final var taskContext = createTaskContext(configuration);
        schema = createSchema(taskContext);
        schema.refresh(table("a", true));
        schema.refresh(table("b", true));
        partition = createPartition();
        offset = createOffset();
        context = (IncrementalSnapshotContext<TableId>) offset.getIncrementalSnapshotContext();
        context.addDataCollectionNamesToSnapshot("recovery", List.of(database.qualifiedTableName("a"), database.qualifiedTableName("b")), List.of(), "");
        queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .maxBatchSize(100).maxQueueSize(1000).pollInterval(Duration.ofMillis(10))
                .loggingContextSupplier(() -> LoggingContext.forConnector(getConnectorName(), "recovery", "test"))
                .build();
        dispatcher = new EventDispatcher<>(config, config.getTopicNamingStrategy(BinlogConnectorConfig.TOPIC_NAMING_STRATEGY),
                schema, queue, config.getTableFilters().dataCollectionFilter(), DataChangeEvent::new,
                new BinlogEventMetadataProvider(), Heartbeat.ScheduledHeartbeat.NOOP_HEARTBEAT,
                config.schemaNameAdjuster(), new DebeziumHeaderProducer(taskContext));
        notifications = new NotificationService<>(List.of(), config, SchemaFactory.get(), record -> {
        });
        source = readOnly ? createReadOnlySource()
                : new BinlogSignalBasedIncrementalSnapshotChangeEventSource<>(config, jdbc, dispatcher, schema,
                        Clock.system(), SnapshotProgressListener.NO_OP(), DataChangeEventListener.NO_OP(), notifications);
        errorHandler = new ErrorHandler(getConnectorClass(), config, queue, null);
        dispatcher.setIncrementalSnapshotChangeEventSource(java.util.Optional.of(source));
        signalChannel = new SourceSignalChannel();
        signalProcessor = new SignalProcessor<>(getConnectorClass(), config,
                Map.of(OpenIncrementalSnapshotWindow.NAME, new OpenIncrementalSnapshotWindow<>(),
                        CloseIncrementalSnapshotWindow.NAME, new CloseIncrementalSnapshotWindow<>(dispatcher),
                        ResumeIncrementalSnapshot.NAME, new ResumeIncrementalSnapshot<>(dispatcher)),
                List.of(signalChannel), DocumentReader.defaultReader(), Offsets.of(partition, offset));
    }

    private void seedVerifiedSchema() throws SQLException {
        // Schema verification compares JDBC metadata across windows, not with the binlog model.
        // Keep that metadata current while deliberately leaving the cached table definition stale.
        jdbc.query("SELECT * FROM " + tableId("a").toQuotedString('`') + " LIMIT 0", rows -> {
            final var metadata = rows.getMetaData();
            List<Column> columns = new ArrayList<>();
            for (int i = 1; i <= metadata.getColumnCount(); i++) {
                columns.add(Column.editor().name(metadata.getColumnName(i)).jdbcType(metadata.getColumnType(i))
                        .type(metadata.getColumnTypeName(i)).optional(metadata.isNullable(i) > 0)
                        .length(metadata.getPrecision(i)).scale(metadata.getScale(i)).create());
            }
            Collections.sort(columns);
            context.setSchema(Table.editor().tableId(tableId("a")).addColumns(columns).create());
        });
        context.setSchemaVerificationPassed(true);
    }

    private void assertRetryPreservesTableAndReleasesLock() throws Exception {
        assertThat(context.snapshotRunning()).isTrue();
        assertThat(context.currentDataCollectionId().getId()).isEqualTo(tableId("a"));
        assertThat(queue.poll()).isEmpty();
        assertDdlIsNotBlocked();
    }

    private void assertDdlIsNotBlocked() throws SQLException {
        // A closed ResultSet is insufficient: a retained read transaction blocks this DDL.
        observer.execute("ALTER TABLE a COMMENT='chunk transaction released'");
    }

    private List<SourceRecord> completeSnapshot() throws Exception {
        List<SourceRecord> records = new ArrayList<>();
        for (int i = 0; i < 30 && context.snapshotRunning(); i++) {
            advanceWatermark();
            queue.poll().forEach(event -> records.add(event.getRecord()));
        }
        assertThat(context.snapshotRunning()).as("both tables must complete").isFalse();
        queue.poll().forEach(event -> records.add(event.getRecord()));
        return records;
    }

    private void advanceWatermark() throws Exception {
        if (readOnly) {
            observer.execute("INSERT INTO ticks VALUES (NULL)");
            final String gtidSet = jdbc.knownGtidSet().toString();
            jdbc.commit();
            final String gtid;
            if (isMariaDb()) {
                final String serverId = observer.queryAndMap("SELECT @@SESSION.gtid_domain_id, @@server_id", rows -> {
                    rows.next();
                    return rows.getString(1) + "-" + rows.getString(2) + "-";
                });
                gtid = Arrays.stream(gtidSet.split(",")).map(String::trim)
                        .filter(value -> value.startsWith(serverId)).findFirst().orElseThrow();
            }
            else {
                final String serverId = observer.queryAndMap("SELECT @@server_uuid", rows -> {
                    rows.next();
                    return rows.getString(1);
                });
                final String serverSet = Arrays.stream(gtidSet.split(",")).map(String::trim)
                        .filter(value -> value.startsWith(serverId + ":")).findFirst().orElseThrow();
                final String interval = serverSet.substring(serverSet.lastIndexOf(':') + 1);
                gtid = serverId + ":" + interval.substring(interval.lastIndexOf('-') + 1);
            }
            offset.startGtid(gtid, gtidSet);
            source.processTransactionCommittedEvent(partition, offset);
        }
        else {
            final String chunkId = context.currentChunkId();
            processSignal(chunkId + "-open", OpenIncrementalSnapshotWindow.NAME);
            processSignal(chunkId + "-close", CloseIncrementalSnapshotWindow.NAME);
        }
    }

    private void processSignal(String id, String type) {
        signalChannel.signals.add(new SignalRecord(id, type, "{}", Map.of()));
        signalProcessor.processSourceSignal(partition);
    }

    private void assertAllRows(List<SourceRecord> records) {
        assertRows(records, List.of(1, 2, 3, 4, 5, 11, 12, 13));
    }

    private void assertRows(List<SourceRecord> records, List<Integer> expectedKeys) {
        List<Integer> keys = new ArrayList<>();
        for (final var record : records) {
            final var envelope = (Struct) record.value();
            assertThat(envelope.getString("op")).isEqualTo("r");
            final var row = envelope.getStruct("after");
            final int key = row.getInt32("pk");
            keys.add(key);
            assertThat(row.getInt32("c")).isEqualTo(100 + key);
            assertThat(record.topic()).isEqualTo(database.topicForTable(key < 10 ? "a" : "b"));
        }
        assertThat(keys).containsExactlyElementsOf(expectedKeys);
    }

    private TableId tableId(String name) {
        return new TableId(database.getDatabaseName(), null, name);
    }

    private Table table(String name, boolean includeNewColumn) {
        final var editor = Table.editor().tableId(tableId(name))
                .addColumns(Column.editor().name("pk").jdbcType(Types.INTEGER).type("INT").optional(false).create(),
                        Column.editor().name("aa").jdbcType(Types.INTEGER).type("INT").optional(true).create());
        if (includeNewColumn) {
            editor.addColumn(Column.editor().name("c").jdbcType(Types.INTEGER).type("INT").optional(true).create());
        }
        return editor.setPrimaryKeyNames("pk").create();
    }
}
