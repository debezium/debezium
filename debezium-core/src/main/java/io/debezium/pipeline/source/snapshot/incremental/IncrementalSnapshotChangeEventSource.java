/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.annotation.NotThreadSafe;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.SnapshotChangeRecordEmitter;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.schema.DataCollectionId;
import io.debezium.schema.DatabaseSchema;
import io.debezium.util.Clock;
import io.debezium.util.ColumnUtils;
import io.debezium.util.HexConverter;
import io.debezium.util.Strings;
import io.debezium.util.Threads;
import io.debezium.util.Threads.Timer;

@NotThreadSafe
public class IncrementalSnapshotChangeEventSource<T extends DataCollectionId> {

    private static final Logger LOGGER = LoggerFactory.getLogger(IncrementalSnapshotChangeEventSource.class);

    public static final String INCREMENTAL_SNAPSHOT_KEY = "incremental_snapshot";
    public static final String DATA_COLLECTIONS_TO_SNAPSHOT_KEY = INCREMENTAL_SNAPSHOT_KEY + "_collections";
    public static final String EVENT_PRIMARY_KEY = INCREMENTAL_SNAPSHOT_KEY + "_primary_key";
    public static final String TABLE_MAXIMUM_KEY = INCREMENTAL_SNAPSHOT_KEY + "_maximum_key";

    // TODO Metrics
    // Need to discuss and decide if
    // - SnapshotChangeEventSourceMetricsMXBean would be extended with window metrics
    // - new MXBean would be introduced with subset of SnapshotChangeEventSourceMetricsMXBean and additional window metrics
    // - SnapshotChangeEventSourceMetricsMXBean would be reused and new MXBean should be introduce for window metrics

    // List needs to be used as key as it implements hashCode/equals contract
    private Map<Struct, Object[]> window = new LinkedHashMap<>();
    private CommonConnectorConfig connectorConfig;
    private JdbcConnection jdbcConnection;
    // TODO Pass Clock
    private final Clock clock = Clock.system();
    private final String signalWindowStatement;
    private final RelationalDatabaseSchema databaseSchema;
    private final EventDispatcher<T> dispatcher;

    private boolean windowOpened = false;
    private Object[] chunkEndPosition;
    private Table currentTable;

    // TODO Extract into a separate IncrementalSnapshotContext
    // TODO After extracting add into source info optional block incrementalSnapshotWindow{String from, String to}
    // State to be stored and recovered from offsets
    private final Queue<T> dataCollectionsToSnapshot = new LinkedList<>();
    // The PK of the last record that was passed to Kafka Connect
    // In case of connector restart the start of the first window will be populated from it
    private Object[] lastEventSentKey;
    // The largest PK in the table at the start of snapshot
    private Object[] maximumKey;

    public IncrementalSnapshotChangeEventSource(CommonConnectorConfig config, JdbcConnection jdbcConnection,
                                                DatabaseSchema<?> databaseSchema, EventDispatcher<T> dispatcher) {
        this.connectorConfig = config;
        this.jdbcConnection = jdbcConnection;
        signalWindowStatement = "INSERT INTO " + connectorConfig.getSignalingDataCollectionId()
                + " VALUES (?, ?, null)";
        this.databaseSchema = (RelationalDatabaseSchema) databaseSchema;
        this.dispatcher = dispatcher;
    }

    public void windowOpen() {
        LOGGER.info("Opening window for incremental snapshot batch");
        windowOpened = true;
    }

    public void windowClosed(OffsetContext offsetContext) {
        try {
            LOGGER.info("Closing window for incremental snapshot chunk");
            windowOpened = false;
            // TODO There is an issue here
            // Events are emitted with tx log coordinates of the CloseIncrementalSnapshotWindow
            // These means that if the connector is restarted in the middle of emptying the buffer
            // then the rest of the buffer might not be resent or even the snapshotting restarted
            // as there is no more of events.
            // Most probably could be solved by injecting a sequence of windowOpen/Closed upon the start
            offsetContext.incrementalSnapshotWindow();
            for (Object[] row : window.values()) {
                sendEnvent(offsetContext, row);
            }
            offsetContext.postSnapshotCompletion();
            window.clear();
            populateWindow();
        }
        catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    protected void sendEnvent(OffsetContext offsetContext, Object[] row) throws InterruptedException {
        lastEventSentKey = keyFromRow(row);
        offsetContext.event((T) currentDataCollectionId(), clock.currentTimeAsInstant());
        dispatcher.dispatchSnapshotEvent((T) currentDataCollectionId(),
                getChangeRecordEmitter(currentDataCollectionId(), offsetContext, row),
                dispatcher.getIncrementalSnapshotChangeEventReceiver());
    }

    /**
     * Returns a {@link ChangeRecordEmitter} producing the change records for
     * the given table row.
     */
    protected ChangeRecordEmitter getChangeRecordEmitter(T dataCollectionId, OffsetContext offsetContext,
                                                         Object[] row) {
        return new SnapshotChangeRecordEmitter(offsetContext, row, clock);
    }

    public void processMessage(DataCollectionId dataCollectionId, Object key) {
        LOGGER.trace("Checking window for table '{}', key '{}', window contains '{}'", dataCollectionId, key, window);
        if (!windowOpened || window.isEmpty()) {
            return;
        }
        if (!currentDataCollectionId().equals(dataCollectionId)) {
            return;
        }
        if (key instanceof Struct) {
            if (window.remove((Struct) key) != null) {
                LOGGER.info("Removed '{}' from window", key);
            }
        }
    }

    protected T currentDataCollectionId() {
        return dataCollectionsToSnapshot.peek();
    }

    private void emitWindowOpen() throws SQLException {
        jdbcConnection.prepareUpdate(signalWindowStatement, x -> {
            x.setString(1, UUID.randomUUID().toString());
            x.setString(2, OpenIncrementalSnapshotWindow.NAME);
        });
        jdbcConnection.commit();
    }

    private void emitWindowClose() throws SQLException {
        jdbcConnection.prepareUpdate(signalWindowStatement, x -> {
            x.setString(1, UUID.randomUUID().toString());
            x.setString(2, CloseIncrementalSnapshotWindow.NAME);
        });
        jdbcConnection.commit();
    }

    protected String buildChunkQuery(Table table) {
        final StringBuilder sql = new StringBuilder("SELECT * FROM ");
        sql.append(table.id().toString());

        // Add condition when this is not the first query
        if (isNonInitialChunk()) {
            // Window boundaries
            sql.append(" WHERE ");
            addKeyColumnsToCondition(table, sql, " >= ?");
            sql.append(" AND NOT (");
            addKeyColumnsToCondition(table, sql, " = ?");
            sql.append(")");
            // Table boundaries
            sql.append(" AND ");
            addKeyColumnsToCondition(table, sql, " <= ?");
        }
        // TODO limiting is db dialect based
        sql.append(" ORDER BY ")
                .append(table.primaryKeyColumns().stream().map(Column::name).collect(Collectors.joining(", ")))
                .append(" LIMIT ").append(connectorConfig.getIncrementalSnashotChunkSize());
        return sql.toString();
    }

    private boolean isNonInitialChunk() {
        return chunkEndPosition != null;
    }

    protected String buildMaxPrimaryKeyQuery(Table table) {
        final StringBuilder sql = new StringBuilder("SELECT * FROM ");
        sql.append(table.id().toString());

        // TODO limiting is db dialect based
        sql.append(" ORDER BY ")
                .append(table.primaryKeyColumns().stream().map(Column::name).collect(Collectors.joining(" DESC, ")))
                .append(" DESC LIMIT ").append(1);
        return sql.toString();
    }

    public void nextChunk(Object[] end) {
        chunkEndPosition = end;
    }

    private void resetChunk() {
        chunkEndPosition = null;
        maximumKey = null;
    }

    protected boolean tablesAvailable() {
        return !dataCollectionsToSnapshot.isEmpty();
    }

    protected void setMaximumKey(Object[] key) {
        maximumKey = key;
    }

    private boolean hasMaximumKey() {
        return maximumKey != null;
    }

    private void populateWindow() throws InterruptedException {
        if (!tablesAvailable()) {
            return;
        }
        try {
            emitWindowOpen();
            while (tablesAvailable()) {
                final TableId currentTableId = (TableId) currentDataCollectionId();
                currentTable = databaseSchema.tableFor(currentTableId);
                if (!hasMaximumKey()) {
                    setMaximumKey(jdbcConnection.queryAndMap(buildMaxPrimaryKeyQuery(currentTable), rs -> {
                        if (!rs.next()) {
                            return null;
                        }
                        return keyFromRow(rowToArray(currentTable, rs, ColumnUtils.toArray(rs, currentTable)));
                    }));
                    if (!hasMaximumKey()) {
                        LOGGER.info(
                                "No maximum key returned by the query, incremental snapshotting of table '{}' finished as it is empty",
                                currentTableId);
                        nextDataCollection();
                        continue;
                    }
                    LOGGER.info("Incremental snapshot for table '{}' will end at position {}", currentTableId,
                            maximumKey);
                }
                createDataEventsForTable(dataCollectionsToSnapshot.size());
                if (window.isEmpty()) {
                    LOGGER.info("No data returned by the query, incremental snapshotting of table '{}' finished",
                            currentTableId);
                    nextDataCollection();
                }
                else {
                    break;
                }
            }
            emitWindowClose();
        }
        catch (SQLException e) {
            throw new DebeziumException("Database error while executing incremental snapshot", e);
        }
    }

    protected T nextDataCollection() {
        resetChunk();
        return dataCollectionsToSnapshot.poll();
    }

    private void addTablesIdsToSnapshot(List<T> dataCollectionIds) {
        boolean shouldPopulateWindow = false;
        if (!tablesAvailable()) {
            shouldPopulateWindow = true;
        }
        dataCollectionsToSnapshot.addAll(dataCollectionIds);
        if (shouldPopulateWindow) {
            try {
                populateWindow();
            }
            catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void addDataCollectionNamesToSnapshot(List<String> dataCollectionIds) {
        addTablesIdsToSnapshot(dataCollectionIds.stream().map(x -> (T) TableId.parse(x)).collect(Collectors.toList()));
    }

    protected void addKeyColumnsToCondition(Table table, StringBuilder sql, String predicate) {
        for (Iterator<Column> i = table.primaryKeyColumns().iterator(); i.hasNext();) {
            final Column key = i.next();
            sql.append(key.name()).append(predicate);
            if (i.hasNext()) {
                sql.append(" AND ");
            }
        }
    }

    /**
     * Dispatches the data change events for the records of a single table.
     */
    private void createDataEventsForTable(int tableCount) throws InterruptedException {
        long exportStart = clock.currentTimeInMillis();
        LOGGER.debug("Exporting data chunk from table '{}' (total {} tables)", currentTable.id(), tableCount);

        final String selectStatement = buildChunkQuery(currentTable);
        LOGGER.debug("\t For table '{}' using select statement: '{}', key: '{}', maximum key: '{}'", currentTable.id(),
                selectStatement, chunkEndPosition, maximumKey);

        final TableSchema tableSchema = databaseSchema.schemaFor(currentTable.id());

        try (PreparedStatement statement = readTableStatement(selectStatement);
                ResultSet rs = statement.executeQuery()) {

            final ColumnUtils.ColumnArray columnArray = ColumnUtils.toArray(rs, currentTable);
            long rows = 0;
            Timer logTimer = getTableScanLogTimer();

            Object[] lastRow = null;
            while (rs.next()) {
                rows++;
                final Object[] row = rowToArray(currentTable, rs, columnArray);
                final Struct keyStruct = tableSchema.keyFromColumnData(row);
                window.put(keyStruct, row);
                if (logTimer.expired()) {
                    long stop = clock.currentTimeInMillis();
                    LOGGER.debug("\t Exported {} records for table '{}' after {}", rows, currentTable.id(),
                            Strings.duration(stop - exportStart));
                    logTimer = getTableScanLogTimer();
                }
                lastRow = row;
            }
            nextChunk(keyFromRow(lastRow));
            if (lastRow != null) {
                LOGGER.debug("\t Next window will resume from '{}'", chunkEndPosition);
            }

            LOGGER.debug("\t Finished exporting {} records for window of table table '{}'; total duration '{}'", rows,
                    currentTable.id(), Strings.duration(clock.currentTimeInMillis() - exportStart));
        }
        catch (SQLException e) {
            throw new DebeziumException("Snapshotting of table " + currentTable.id() + " failed", e);
        }
    }

    // Extract to JdbcConnection, same as in RelationalSnapshotChangeEventSource
    protected Object[] rowToArray(Table table, ResultSet rs, ColumnUtils.ColumnArray columnArray) throws SQLException {
        final Object[] row = new Object[columnArray.getGreatestColumnPosition()];
        for (int i = 0; i < columnArray.getColumns().length; i++) {
            row[columnArray.getColumns()[i].position() - 1] = getColumnValue(rs, i + 1, columnArray.getColumns()[i],
                    table);
        }
        return row;
    }

    // TODO Parmetrize the method and extract it to JdbcConnection
    /**
     * Allow per-connector query creation to override for best database
     * performance depending on the table size.
     */
    protected PreparedStatement readTableStatement(String sql) throws SQLException {
        int fetchSize = connectorConfig.getSnapshotFetchSize();
        PreparedStatement statement = jdbcConnection.connection().prepareStatement(sql);
        if (isNonInitialChunk()) {
            for (int i = 0; i < chunkEndPosition.length; i++) {
                statement.setObject(i + 1, chunkEndPosition[i]);
                statement.setObject(i + 1 + chunkEndPosition.length, chunkEndPosition[i]);
                statement.setObject(i + 1 + 2 * chunkEndPosition.length, maximumKey[i]);
            }
        }
        statement.setFetchSize(fetchSize);
        return statement;
    }

    private Timer getTableScanLogTimer() {
        return Threads.timer(clock, RelationalSnapshotChangeEventSource.LOG_INTERVAL);
    }

    // TODO Extract these two methods from *SnapshotChangeEventSource to JdbcValueConverters or JdbcConnection
    protected Object getColumnValue(ResultSet rs, int columnIndex, Column column, Table table) throws SQLException {
        return getColumnValue(rs, columnIndex, column);
    }

    @Deprecated
    protected Object getColumnValue(ResultSet rs, int columnIndex, Column column) throws SQLException {
        return rs.getObject(columnIndex);
    }

    private Object[] keyFromRow(Object[] row) {
        if (row == null) {
            return null;
        }
        final List<Column> keyColumns = currentTable.primaryKeyColumns();
        final Object[] key = new Object[keyColumns.size()];
        for (int i = 0; i < keyColumns.size(); i++) {
            key[i] = row[keyColumns.get(i).position() - 1];
        }
        return key;
    }

    private String arrayToSerializedString(Object[] array) {
        try (final ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(array);
            return HexConverter.convertToHexString(bos.toByteArray());
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private Object[] serializedStringToArray(String field, String serialized) {
        try (final ByteArrayInputStream bis = new ByteArrayInputStream(HexConverter.convertFromHex(serialized));
                ObjectInputStream ois = new ObjectInputStream(bis)) {
            return (Object[]) ois.readObject();
        }
        catch (Exception e) {
            throw new DebeziumException(String.format("Failed to deserialize '%s' with value '%s'", field, serialized), e);
        }
    }

    private String dataCollectionsToSnapshotAsString() {
        // TODO Handle non-standard table ids containing dots, commas etc.
        return dataCollectionsToSnapshot.stream()
                .map(x -> x.toString())
                .collect(Collectors.joining(","));
    }

    private List<String> stringToDataCollections(String dataCollectionsStr) {
        return Arrays.asList(dataCollectionsStr.split(","));
    }

    public Map<String, ?> store(Map<String, ?> iOffset) {
        @SuppressWarnings("unchecked")
        final Map<String, Object> offset = (Map<String, Object>) iOffset;
        if (!tablesAvailable()) {
            return offset;
        }
        offset.put(EVENT_PRIMARY_KEY, arrayToSerializedString(lastEventSentKey));
        offset.put(TABLE_MAXIMUM_KEY, arrayToSerializedString(maximumKey));
        offset.put(DATA_COLLECTIONS_TO_SNAPSHOT_KEY, dataCollectionsToSnapshotAsString());
        return offset;
    }

    // TODO Call on connector start
    public void load(Map<String, ?> offsets) {
        if (offsets == null) {
            return;
        }
        final String lastEventSentKeyStr = (String) offsets.get(EVENT_PRIMARY_KEY);
        chunkEndPosition = (lastEventSentKeyStr != null) ? serializedStringToArray(EVENT_PRIMARY_KEY, lastEventSentKeyStr) : null;
        lastEventSentKey = null;
        final String maximumKeyStr = (String) offsets.get(TABLE_MAXIMUM_KEY);
        maximumKey = (maximumKeyStr != null) ? serializedStringToArray(TABLE_MAXIMUM_KEY, maximumKeyStr) : null;
        final String dataCollectionsStr = (String) offsets.get(DATA_COLLECTIONS_TO_SNAPSHOT_KEY);
        dataCollectionsToSnapshot.clear();
        if (dataCollectionsStr != null) {
            addDataCollectionNamesToSnapshot(stringToDataCollections(dataCollectionsStr));
        }
    }
}
