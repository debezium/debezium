/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.sql.Types;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.GtidEventData;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.RotateEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.BinlogEventMetadataProvider;
import io.debezium.connector.common.DebeziumHeaderProducer;
import io.debezium.connector.mysql.gtid.MySqlGtidSet;
import io.debezium.connector.mysql.jdbc.MySqlConnection;
import io.debezium.connector.mysql.jdbc.MySqlConnectionConfiguration;
import io.debezium.connector.mysql.jdbc.MySqlTextProtocolFieldReader;
import io.debezium.connector.mysql.jdbc.MySqlValueConverters;
import io.debezium.doc.FixFor;
import io.debezium.heartbeat.Heartbeat.ScheduledHeartbeat;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.Column;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.MemorySchemaHistory;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Clock;

class MySqlStreamingChangeEventSourceTest {

    private static final String SERVER_UUID = "24bc7850-2c16-11e6-a073-0242ac110002";

    private static final String FILENAME = "mysql-bin.000001";
    private static final long BEGIN_POSITION = (1L << 32) + 120;
    private final List<Map<String, ?>> heartbeatOffsets = new ArrayList<>();
    private MySqlConnectorConfig connectorConfig;
    private MySqlTaskContext taskContext;
    private OfflineConnection connection;
    private MySqlDatabaseSchema schema;
    private ChangeEventQueue<DataChangeEvent> queue;
    private EventDispatcher<MySqlPartition, TableId> dispatcher;
    private final BinlogEventMetadataProvider metadataProvider = new BinlogEventMetadataProvider();

    @BeforeEach
    void setUp() {
        final var configuration = Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "server")
                .with(BinlogConnectorConfig.HOSTNAME, "localhost")
                .with(BinlogConnectorConfig.PORT, 3306)
                .with(BinlogConnectorConfig.USER, "user")
                .with(BinlogConnectorConfig.SERVER_ID, 1)
                .with(BinlogConnectorConfig.KEEP_ALIVE, false)
                .with(MySqlConnectorConfig.SSL_MODE, "disabled")
                .with(MySqlConnectorConfig.READ_ONLY_CONNECTION, true)
                .with(MySqlConnectorConfig.SCHEMA_HISTORY, MemorySchemaHistory.class)
                .build();
        connectorConfig = new MySqlConnectorConfig(configuration);
        taskContext = new MySqlTaskContext(configuration, connectorConfig);
        final var schemaNameAdjuster = SchemaNameAdjuster.create();
        final TopicNamingStrategy<TableId> topicNamingStrategy = connectorConfig.getTopicNamingStrategy(MySqlConnectorConfig.TOPIC_NAMING_STRATEGY);
        final var valueConverters = new MySqlValueConverters(connectorConfig.getDecimalMode(), connectorConfig.getTemporalPrecisionMode(),
                connectorConfig.getBigIntUnsignedHandlingMode().asBigIntUnsignedMode(), connectorConfig.binaryHandlingMode(),
                MySqlValueConverters::adjustTemporal, connectorConfig.getEventConvertingFailureHandlingMode(),
                connectorConfig.getServiceRegistry(), connectorConfig.getUnavailableValuePlaceholder());

        connection = new OfflineConnection(connectorConfig);
        schema = new MySqlDatabaseSchema(connectorConfig, valueConverters, topicNamingStrategy, schemaNameAdjuster,
                false, new CustomConverterRegistry(List.of()), taskContext);
        queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(Duration.ofMillis(10))
                .loggingContextSupplier(() -> taskContext.configureLoggingContext("streaming"))
                .maxBatchSize(10)
                .maxQueueSize(20)
                .build();
        final var heartbeat = new ScheduledHeartbeat() {
            @Override
            public boolean isEnabled() {
                return true;
            }

            @Override
            public void emitWithDelay(Map<String, ?> partition, OffsetContext offsetContext) {
                heartbeatOffsets.add(offsetContext.getOffset());
            }
        };
        dispatcher = new EventDispatcher<>(connectorConfig, topicNamingStrategy,
                schema, queue, tableId -> true, DataChangeEvent::new, metadataProvider, heartbeat, schemaNameAdjuster,
                new DebeziumHeaderProducer(taskContext));
    }

    @AfterEach
    void tearDown() throws Exception {
        try (var testConnection = connection) {
            schema.close();
        }
        finally {
            connectorConfig.getServiceRegistry().close();
        }
    }

    @ParameterizedTest
    @CsvSource({
            "HEARTBEAT,    4294967496, IDLE",
            "HEARTBEAT_V2, 8589934592, IDLE",
            "HEARTBEAT_V2,          0, IDLE",
            "HEARTBEAT_V2, 8589934592, IN_TRANSACTION",
            "HEARTBEAT_V2, 8589934592, RESTORING"
    })
    @FixFor("debezium/dbz#2508")
    @SuppressWarnings("unchecked")
    void shouldDispatchServerHeartbeatsWithoutCountingThemAsBinlogEvents(EventType eventType, long heartbeatPosition, TransactionState state) throws Exception {
        final var offset = MySqlOffsetContext.initial(connectorConfig);
        offset.setBinlogStartPoint(FILENAME, BEGIN_POSITION);
        if (state == TransactionState.RESTORING) {
            offset.setInitialSkips(1, 2);
        }
        final Event heartbeat = event(eventType, heartbeatPosition, 40, null);
        offset.startGtid(SERVER_UUID + ":10", SERVER_UUID + ":1-10");
        final var snapshotContext = (MySqlReadOnlyIncrementalSnapshotContext<TableId>) offset.getIncrementalSnapshotContext();
        snapshotContext.addDataCollectionNamesToSnapshot("test", List.of("inventory.products"), List.of(), "");
        assertThat(snapshotContext.snapshotRunning()).isTrue();
        snapshotContext.setLowWatermark(new MySqlGtidSet(SERVER_UUID + ":1-5"));
        snapshotContext.setHighWatermark(new MySqlGtidSet(SERVER_UUID + ":1-10"));
        assertThat(snapshotContext.reachedHighWatermark(SERVER_UUID + ":10")).isTrue();
        final var incrementalSource = new ReadOnlySnapshotSource(connectorConfig, connection, dispatcher, schema, offset);
        dispatcher.setIncrementalSnapshotChangeEventSource(Optional.of(incrementalSource));
        final var metrics = stream(offset, List.of(heartbeat, heartbeat), () -> {
            if (state == TransactionState.IN_TRANSACTION) {
                offset.startNextTransaction();
                offset.completeEvent();
                offset.setRowNumber(1, 3);
            }
        });

        assertThat(incrementalSource.chunksRead).isEqualTo(1);
        assertThat(snapshotContext.reachedHighWatermark(SERVER_UUID + ":10")).isFalse();
        assertThat(metrics.getMilliSecondsBehindSource()).isZero();
        final long sourcePosition = heartbeatPosition > 0 ? heartbeatPosition : BEGIN_POSITION;
        assertThat(offset.getSource().binlogPosition()).isEqualTo(sourcePosition);
        assertThat(heartbeatOffsets).hasSize(2).allSatisfy(emitted -> {
            if (state == TransactionState.IDLE) {
                assertThat(emitted.get("pos")).isEqualTo(sourcePosition);
                assertThat(emitted).doesNotContainKeys("event", "row");
            }
            else {
                assertThat(emitted.get("pos")).isEqualTo(BEGIN_POSITION);
                assertThat(emitted.get("event")).isEqualTo(1L);
                assertThat(emitted.get("row")).isEqualTo(2L);
            }
        });
        assertThat(offset.eventsToSkipUponRestart()).isEqualTo(state == TransactionState.IDLE ? 0 : 1);
    }

    private static Stream<Arguments> replayCheckpoints() {
        return Stream.of(
                Arguments.of(2, 2, List.of(3003, 4001), 2L, 3L),
                Arguments.of(2, 0, List.of(3001, 3002, 3003, 4001), 2L, 1L),
                Arguments.of(0, 2, List.of(3003, 4001), 2L, 3L),
                Arguments.of(2, 3, List.of(4001), 3L, 1L),
                Arguments.of(3, 0, List.of(4001), 3L, 1L));
    }

    @ParameterizedTest
    @MethodSource("replayCheckpoints")
    @FixFor("debezium/dbz#2508")
    void shouldPreserveRestartPositionWhenInterruptedDuringReplay(int eventSkip, int rowSkip, List<Integer> expectedIds,
                                                                  long expectedFirstEvent, long expectedFirstRow)
            throws Exception {
        final Map<String, Object> saved = new HashMap<>(Map.of("file", FILENAME, "pos", BEGIN_POSITION, "gtids", SERVER_UUID + ":1-10"));
        if (eventSkip != 0) {
            saved.put("event", (long) eventSkip);
        }
        if (rowSkip != 0) {
            saved.put("row", (long) rowSkip);
        }
        final var loader = new MySqlOffsetContext.Loader(connectorConfig);
        final var rotate = new RotateEventData();
        rotate.setBinlogFilename(FILENAME);
        rotate.setBinlogPosition(4);
        // A GTID resume sends file metadata and a heartbeat for excluded transactions before the first BEGIN.
        final List<Event> preamble = List.of(event(EventType.ROTATE, 0, 47, rotate), event(EventType.FORMAT_DESCRIPTION, 127, 123, null),
                event(EventType.PREVIOUS_GTIDS, 198, 71, null), event(EventType.HEARTBEAT_V2, 1L << 32, 53, null));
        stream(loader.load(saved), preamble, () -> {
        });
        final Map<String, ?> checkpoint = heartbeatOffsets.get(heartbeatOffsets.size() - 1);

        final var tableId = new TableId("inventory", null, "products");
        final var id = Column.editor().name("id").position(1).jdbcType(Types.INTEGER).type("INT").optional(false).create();
        schema.refresh(Table.editor().tableId(tableId).addColumn(id).setPrimaryKeyNames("id").create());
        final var gtid = new GtidEventData();
        gtid.setGtid(SERVER_UUID + ":11");
        final var begin = new QueryEventData();
        begin.setSql("BEGIN");
        final var tableMap = new TableMapEventData();
        tableMap.setTableId(2);
        tableMap.setTable(tableId.table());
        tableMap.setDatabase(tableId.catalog());
        final List<Event> replay = new ArrayList<>(preamble);
        replay.add(event(EventType.GTID, BEGIN_POSITION, 120, gtid));
        replay.add(event(EventType.QUERY, BEGIN_POSITION + 35, 35, begin));
        replay.add(event(EventType.TABLE_MAP, BEGIN_POSITION + 75, 40, tableMap));
        replay.add(rows(BEGIN_POSITION + 125, 3001, 3002, 3003));
        replay.add(rows(BEGIN_POSITION + 175, 4001));
        final var nextFile = new RotateEventData();
        nextFile.setBinlogFilename("mysql-bin.000002");
        nextFile.setBinlogPosition(4);
        replay.add(event(EventType.XID, BEGIN_POSITION + 206, 31, null));
        replay.add(event(EventType.ROTATE, BEGIN_POSITION + 253, 47, nextFile));
        stream(loader.load(checkpoint), replay, () -> {
        });

        final var records = queue.poll().stream().map(DataChangeEvent::getRecord).toList();
        assertThat(records).extracting(record -> ((Struct) record.key()).getInt32("id")).containsExactlyElementsOf(expectedIds);
        assertThat(new HashMap<String, Object>(checkpoint)).containsAllEntriesOf(saved);
        final var first = records.get(0).sourceOffset();
        assertThat(first.get("pos")).isEqualTo(BEGIN_POSITION);
        assertThat(first.get("event")).isEqualTo(expectedFirstEvent);
        assertThat(first.get("row")).isEqualTo(expectedFirstRow);
        final var last = records.get(records.size() - 1).sourceOffset();
        assertThat(last.get("pos")).isEqualTo(BEGIN_POSITION);
        assertThat(last.get("event")).isEqualTo(3L);
        assertThat(last.get("row")).isEqualTo(1L);
        final var afterCommitAndRotate = heartbeatOffsets.get(heartbeatOffsets.size() - 1);
        assertThat(afterCommitAndRotate.get("file")).isEqualTo(nextFile.getBinlogFilename());
        assertThat(afterCommitAndRotate.get("pos")).isEqualTo(4L);
        assertThat(afterCommitAndRotate).doesNotContainKeys("event", "row");
    }

    private static Event rows(long nextPosition, Integer... ids) {
        final var rows = new WriteRowsEventData();
        rows.setTableId(2);
        final BitSet includedColumns = new BitSet();
        includedColumns.set(0);
        rows.setIncludedColumns(includedColumns);
        rows.setRows(Arrays.stream(ids).map(id -> new Serializable[]{ id }).toList());
        return event(EventType.WRITE_ROWS, nextPosition, 50, rows);
    }

    private static Event event(EventType type, long nextPosition, int length, EventData data) {
        final var header = new EventHeaderV4();
        header.setEventType(type);
        header.setTimestamp(nextPosition != 0 && type != EventType.HEARTBEAT && type != EventType.HEARTBEAT_V2 ? 12000 : 0);
        header.setEventLength(length);
        header.setNextPosition(nextPosition);
        return new Event(header, data);
    }

    private MySqlStreamingChangeEventSourceMetrics stream(MySqlOffsetContext offset, List<Event> events, Runnable onConnect) throws Exception {
        final var context = new TestContext();
        final var client = new BinaryLogClient("localhost", 3306, "user", "") {
            @Override
            public void connect(long timeout) {
                try {
                    onConnect.run();
                    for (Event event : events) {
                        getEventListeners().forEach(listener -> listener.onEvent(event));
                    }
                }
                finally {
                    context.running = false;
                }
            }
        };
        final var metrics = new MySqlStreamingChangeEventSourceMetrics(taskContext, queue, metadataProvider, schema::tableIds, client);
        metrics.setMilliSecondsBehindSource(123);
        final var errorHandler = new MySqlErrorHandler(connectorConfig, queue, null);
        final var source = new MySqlStreamingChangeEventSource(connectorConfig, connection, dispatcher, errorHandler, Clock.system(),
                taskContext, schema, metrics, null, client);
        source.init(offset);
        source.execute(context, new MySqlPartition("server", null), offset);
        assertThat(errorHandler.getProducerThrowable()).isNull();
        return metrics;
    }

    private enum TransactionState {
        IDLE,
        IN_TRANSACTION,
        RESTORING
    }

    private static class OfflineConnection extends MySqlConnection {
        private OfflineConnection(MySqlConnectorConfig connectorConfig) {
            super(new MySqlConnectionConfiguration(connectorConfig.getConfig()), new MySqlTextProtocolFieldReader(connectorConfig));
        }

        @Override
        public JdbcConnection query(String query, ResultSetConsumer consumer) {
            assertThat(query).isEqualTo(BINARY_LOG_STATUS_STATEMENT);
            return this;
        }

        @Override
        public boolean isGtidModeEnabled() {
            return true;
        }

        @Override
        public MySqlGtidSet knownGtidSet() {
            return new MySqlGtidSet(SERVER_UUID + ":1-10");
        }

        @Override
        public MySqlGtidSet purgedGtidSet() {
            return new MySqlGtidSet("");
        }

        @Override
        public boolean isBinlogRowImageNoblob() {
            return false;
        }
    }

    private static class ReadOnlySnapshotSource extends MySqlReadOnlyIncrementalSnapshotChangeEventSource {
        private int chunksRead;

        @SuppressWarnings("unchecked")
        private ReadOnlySnapshotSource(MySqlConnectorConfig config, MySqlConnection connection,
                                       EventDispatcher<MySqlPartition, TableId> dispatcher, MySqlDatabaseSchema schema,
                                       MySqlOffsetContext offset) {
            super(config, connection, dispatcher, schema, Clock.system(), SnapshotProgressListener.NO_OP(), DataChangeEventListener.NO_OP(), null);
            setContext((IncrementalSnapshotContext<TableId>) offset.getIncrementalSnapshotContext());
        }

        @Override
        protected void readChunk(MySqlPartition partition, OffsetContext offsetContext) {
            ++chunksRead;
        }
    }

    private static class TestContext implements ChangeEventSourceContext {
        private boolean running = true;

        @Override
        public boolean isPaused() {
            return false;
        }

        @Override
        public boolean isRunning() {
            return running;
        }

        @Override
        public void resumeStreaming() {
        }

        @Override
        public void waitSnapshotCompletion() {
        }

        @Override
        public void streamingPaused() {
        }

        @Override
        public void waitStreamingPaused() {
        }
    }
}
