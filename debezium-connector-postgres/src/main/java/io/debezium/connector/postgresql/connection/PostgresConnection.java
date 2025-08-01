/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql.connection;

import java.nio.charset.Charset;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.postgresql.core.BaseConnection;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.jdbc.TimestampUtils;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.util.PGmoney;
import org.postgresql.util.PSQLState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.annotation.VisibleForTesting;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PgOid;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.PostgresType;
import io.debezium.connector.postgresql.PostgresValueConverter;
import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.connector.postgresql.spi.SlotState;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.source.snapshot.incremental.ChunkQueryBuilder;
import io.debezium.pipeline.source.snapshot.incremental.RowValueConstructorChunkQueryBuilder;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;

/**
 * {@link JdbcConnection} connection extension used for connecting to Postgres instances.
 *
 * @author Horia Chiorean
 */
public class PostgresConnection extends JdbcConnection {

    public static final String CONNECTION_STREAMING = "Debezium Streaming";
    public static final String CONNECTION_SLOT_INFO = "Debezium Slot Info";
    public static final String CONNECTION_DROP_SLOT = "Debezium Drop Slot";
    public static final String CONNECTION_VALIDATE_CONNECTION = "Debezium Validate Connection";
    public static final String CONNECTION_HEARTBEAT = "Debezium Heartbeat";
    public static final String CONNECTION_GENERAL = "Debezium General";

    private static final Pattern FUNCTION_DEFAULT_PATTERN = Pattern.compile("^[(]?[A-Za-z0-9_.]+\\((?:.+(?:, ?.+)*)?\\)");
    private static final Pattern EXPRESSION_DEFAULT_PATTERN = Pattern.compile("\\(+(?:.+(?:[+ - * / < > = ~ ! @ # % ^ & | ` ?] ?.+)+)+\\)");
    private static Logger LOGGER = LoggerFactory.getLogger(PostgresConnection.class);

    private static final String URL_PATTERN = "jdbc:postgresql://${" + JdbcConfiguration.HOSTNAME + "}:${"
            + JdbcConfiguration.PORT + "}/${" + JdbcConfiguration.DATABASE + "}";
    protected static final ConnectionFactory FACTORY = JdbcConnection.patternBasedFactory(URL_PATTERN,
            org.postgresql.Driver.class.getName(),
            PostgresConnection.class.getClassLoader(), JdbcConfiguration.PORT.withDefault(PostgresConnectorConfig.PORT.defaultValueAsString()));

    /**
     * Obtaining a replication slot may fail if there's a pending transaction. We're retrying to get a slot for 30 min.
     */
    private static final int MAX_ATTEMPTS_FOR_OBTAINING_REPLICATION_SLOT = 900;

    private static final Duration PAUSE_BETWEEN_REPLICATION_SLOT_RETRIEVAL_ATTEMPTS = Duration.ofSeconds(2);

    private final TypeRegistry typeRegistry;
    private final PostgresDefaultValueConverter defaultValueConverter;

    /**
     * Creates a Postgres connection using the supplied configuration.
     * If necessary this connection is able to resolve data type mappings.
     * Such a connection requires a {@link PostgresValueConverter}, and will provide its own {@link TypeRegistry}.
     * Usually only one such connection per connector is needed.
     *
     * @param config {@link Configuration} instance, may not be null.
     * @param valueConverterBuilder supplies a configured {@link PostgresValueConverter} for a given {@link TypeRegistry}
     * @param connectionUsage a symbolic name of the connection to be tracked in monitoring tools
     */
    public PostgresConnection(JdbcConfiguration config, PostgresValueConverterBuilder valueConverterBuilder, String connectionUsage) {
        super(addDefaultSettings(config, connectionUsage), FACTORY, PostgresConnection::validateServerVersion, "\"", "\"");

        if (Objects.isNull(valueConverterBuilder)) {
            this.typeRegistry = null;
            this.defaultValueConverter = null;
        }
        else {
            this.typeRegistry = new TypeRegistry(this);

            final PostgresValueConverter valueConverter = valueConverterBuilder.build(this.typeRegistry);
            this.defaultValueConverter = new PostgresDefaultValueConverter(valueConverter, this.getTimestampUtils(), typeRegistry);
        }
    }

    /**
     * Create a Postgres connection using the supplied configuration and {@link TypeRegistry}
     * @param config {@link Configuration} instance, may not be null.
     * @param typeRegistry an existing/already-primed {@link TypeRegistry} instance
     * @param connectionUsage a symbolic name of the connection to be tracked in monitoring tools
     */
    public PostgresConnection(PostgresConnectorConfig config, TypeRegistry typeRegistry, String connectionUsage) {
        super(addDefaultSettings(config.getJdbcConfig(), connectionUsage),
                FACTORY,
                PostgresConnection::validateServerVersion,
                "\"", "\"");

        if (Objects.isNull(typeRegistry)) {
            this.typeRegistry = null;
            this.defaultValueConverter = null;
        }
        else {
            this.typeRegistry = typeRegistry;
            final PostgresValueConverter valueConverter = PostgresValueConverter.of(config, this.getDatabaseCharset(), typeRegistry);
            this.defaultValueConverter = new PostgresDefaultValueConverter(valueConverter, this.getTimestampUtils(), typeRegistry);
        }
    }

    /**
     * Creates a Postgres connection using the supplied configuration.
     * The connector is the regular one without datatype resolution capabilities.
     *
     * @param config {@link Configuration} instance, may not be null.
     * @param connectionUsage a symbolic name of the connection to be tracked in monitoring tools
     */
    public PostgresConnection(JdbcConfiguration config, String connectionUsage) {
        this(config, null, connectionUsage);
    }

    static JdbcConfiguration addDefaultSettings(JdbcConfiguration configuration, String connectionUsage) {
        // we require Postgres 9.4 as the minimum server version since that's where logical replication was first introduced
        return JdbcConfiguration.adapt(configuration.edit()
                .with("assumeMinServerVersion", "9.4")
                .with("ApplicationName", connectionUsage)
                .build());
    }

    /**
     * Returns a JDBC connection string for the current configuration.
     *
     * @return a {@code String} where the variables in {@code urlPattern} are replaced with values from the configuration
     */
    public String connectionString() {
        return connectionString(URL_PATTERN);
    }

    /**
     * Prints out information about the REPLICA IDENTITY status of a table.
     * This in turn determines how much information is available for UPDATE and DELETE operations for logical replication.
     *
     * @param tableId the identifier of the table
     * @return the replica identity information; never null
     * @throws SQLException if there is a problem obtaining the replica identity information for the given table
     */
    @VisibleForTesting
    public ReplicaIdentityInfo readReplicaIdentityInfo(TableId tableId) throws SQLException {
        String statement = "SELECT relreplident FROM pg_catalog.pg_class c " +
                "LEFT JOIN pg_catalog.pg_namespace n ON c.relnamespace=n.oid " +
                "WHERE n.nspname=? and c.relname=?";
        String schema = tableId.schema() != null && tableId.schema().length() > 0 ? tableId.schema() : "public";
        StringBuilder replIdentity = new StringBuilder();
        prepareQuery(statement, stmt -> {
            stmt.setString(1, schema);
            stmt.setString(2, tableId.table());
        }, rs -> {
            if (rs.next()) {
                replIdentity.append(rs.getString(1));
            }
            else {
                LOGGER.warn("Cannot determine REPLICA IDENTITY information for table '{}'", tableId);
            }
        });
        return new ReplicaIdentityInfo(ReplicaIdentityInfo.ReplicaIdentity.parseFromDB(replIdentity.toString()));
    }

    /**
     * This query retrieves information about the INDEX as long as replica identity is configure USING INDEX
     *
     * @param tableId the identifier of the table
     * @return Index name linked to replica identity; never null
     * @throws SQLException if there is a problem obtaining the replica identity and index information for the given table
     */
    @VisibleForTesting
    public String readIndexOfReplicaIdentity(TableId tableId) throws SQLException {
        String statement = "with rel_index as (" +
                "select split_part(indexrelid::regclass::text, '.', 1) as index_schema, split_part(indexrelid::regclass::text, '.', 2) as index_name " +
                "from pg_catalog.pg_index " +
                "where indisreplident " +
                ") " +
                "SELECT i.index_name " +
                "FROM pg_catalog.pg_class c " +
                "    LEFT JOIN pg_catalog.pg_namespace n ON c.relnamespace=n.oid " +
                "    LEFT join rel_index i on n.nspname = i.index_schema " +
                "WHERE n.nspname=? and c.relname=?";
        String schema = tableId.schema() != null && tableId.schema().length() > 0 ? tableId.schema() : "public";
        StringBuilder indexName = new StringBuilder();
        prepareQuery(statement, stmt -> {
            stmt.setString(1, schema);
            stmt.setString(2, tableId.table());
        }, rs -> {
            if (rs.next()) {
                indexName.append(rs.getString(1));
            }
            else {
                LOGGER.warn("Cannot determine index linked to REPLICA IDENTITY for table '{}'", tableId);
            }
        });
        return indexName.toString();
    }

    /**
     * Update REPLICA IDENTITY status of a table.
     * This in turn determines how much information is available for UPDATE and DELETE operations for logical replication.
     *
     * @param tableId the identifier of the table
     * @param replicaIdentityValue Replica Identity value
     */
    public void setReplicaIdentityForTable(TableId tableId, ReplicaIdentityInfo replicaIdentityValue) {
        try {
            LOGGER.debug("Updating Replica Identity '{}'", tableId.table());
            execute(String.format("ALTER TABLE %s REPLICA IDENTITY %s;", tableId, replicaIdentityValue));
        }
        catch (SQLException e) {

            if (e.getSQLState().equals("42501")) {
                LOGGER.error("Replica identity could not be updated because of lack of privileges", e);
            }
            else {
                LOGGER.error("Unexpected error while attempting to alter Replica Identity", e);
            }
        }
    }

    /**
     * Returns the current state of the replication slot
     * @param slotName the name of the slot
     * @param pluginName the name of the plugin used for the desired slot
     * @return the {@link SlotState} or null, if no slot state is found
     * @throws SQLException
     */
    public SlotState getReplicationSlotState(String slotName, String pluginName) throws SQLException {
        ServerInfo.ReplicationSlot slot;
        try {
            slot = readReplicationSlotInfo(slotName, pluginName);
            if (slot.equals(ServerInfo.ReplicationSlot.INVALID)) {
                return null;
            }
            else {
                return slot.asSlotState();
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ConnectException("Interrupted while waiting for valid replication slot info", e);
        }
    }

    /**
     * Fetches the state of a replication stage given a slot name and plugin name
     * @param slotName the name of the slot
     * @param pluginName the name of the plugin used for the desired slot
     * @return the {@link ServerInfo.ReplicationSlot} object or a {@link ServerInfo.ReplicationSlot#INVALID} if
     *         the slot is not valid
     * @throws SQLException is thrown by the underlying JDBC
     */
    private ServerInfo.ReplicationSlot fetchReplicationSlotInfo(String slotName, String pluginName) throws SQLException {
        final String database = database();
        final ServerInfo.ReplicationSlot slot = queryForSlot(slotName, database, pluginName,
                rs -> {
                    if (rs.next()) {
                        boolean active = rs.getBoolean("active");
                        final Lsn confirmedFlushedLsn = parseConfirmedFlushLsn(slotName, pluginName, database, rs);
                        if (confirmedFlushedLsn == null) {
                            return null;
                        }
                        Lsn restartLsn = parseRestartLsn(slotName, pluginName, database, rs);
                        if (restartLsn == null) {
                            return null;
                        }
                        final Long xmin = rs.getLong("catalog_xmin");
                        return new ServerInfo.ReplicationSlot(active, confirmedFlushedLsn, restartLsn, xmin);
                    }
                    else {
                        LOGGER.debug("No replication slot '{}' is present for plugin '{}' and database '{}'", slotName,
                                pluginName, database);
                        return ServerInfo.ReplicationSlot.INVALID;
                    }
                });
        return slot;
    }

    /**
     * Fetches a replication slot, repeating the query until either the slot is created or until
     * the max number of attempts has been reached
     *
     * To fetch the slot without the retries, use the {@link PostgresConnection#fetchReplicationSlotInfo} call
     * @param slotName the slot name
     * @param pluginName the name of the plugin
     * @return the {@link ServerInfo.ReplicationSlot} object or a {@link ServerInfo.ReplicationSlot#INVALID} if
     *         the slot is not valid
     * @throws SQLException is thrown by the underyling jdbc driver
     * @throws InterruptedException is thrown if we don't return an answer within the set number of retries
     */
    @VisibleForTesting
    ServerInfo.ReplicationSlot readReplicationSlotInfo(String slotName, String pluginName) throws SQLException, InterruptedException {
        final String database = database();
        final Metronome metronome = Metronome.parker(PAUSE_BETWEEN_REPLICATION_SLOT_RETRIEVAL_ATTEMPTS, Clock.SYSTEM);

        for (int attempt = 1; attempt <= MAX_ATTEMPTS_FOR_OBTAINING_REPLICATION_SLOT; attempt++) {
            final ServerInfo.ReplicationSlot slot = fetchReplicationSlotInfo(slotName, pluginName);
            if (slot != null) {
                LOGGER.info("Obtained valid replication slot {}", slot);
                return slot;
            }
            LOGGER.warn(
                    "Cannot obtain valid replication slot '{}' for plugin '{}' and database '{}' [during attempt {} out of {}, concurrent tx probably blocks taking snapshot.",
                    slotName, pluginName, database, attempt, MAX_ATTEMPTS_FOR_OBTAINING_REPLICATION_SLOT);
            metronome.pause();
        }

        throw new ConnectException("Unable to obtain valid replication slot. "
                + "Make sure there are no long-running transactions running in parallel as they may hinder the allocation of the replication slot when starting this connector");
    }

    protected ServerInfo.ReplicationSlot queryForSlot(String slotName, String database, String pluginName,
                                                      ResultSetMapper<ServerInfo.ReplicationSlot> map)
            throws SQLException {
        return prepareQueryAndMap("select * from pg_replication_slots where slot_name = ? and database = ? and plugin = ?", statement -> {
            statement.setString(1, slotName);
            statement.setString(2, database);
            statement.setString(3, pluginName);
        }, map);
    }

    /**
     * Obtains the LSN to resume streaming from. On PG 9.5 there is no confirmed_flushed_lsn yet, so restart_lsn will be
     * read instead. This may result in more records to be re-read after a restart.
     */
    private Lsn parseConfirmedFlushLsn(String slotName, String pluginName, String database, ResultSet rs) {
        Lsn confirmedFlushedLsn = null;

        try {
            confirmedFlushedLsn = tryParseLsn(slotName, pluginName, database, rs, "confirmed_flush_lsn");
            if (confirmedFlushedLsn == null) {
                LOGGER.info("Failed to obtain valid replication slot, confirmed flush lsn is null");
                if (!hasIdleTransactions()) {
                    confirmedFlushedLsn = tryFallbackToRestartLsn(slotName, pluginName, database, rs);
                }
            }
        }
        catch (SQLException e) {
            confirmedFlushedLsn = tryFallbackToRestartLsn(slotName, pluginName, database, rs);
        }

        return confirmedFlushedLsn;
    }

    private boolean hasIdleTransactions() throws SQLException {
        return queryAndMap(
                "select * from pg_stat_activity where state like 'idle in transaction' AND application_name != '" + CONNECTION_GENERAL + "' AND pid <> pg_backend_pid()",
                rs -> {
                    if (rs.next()) {
                        LOGGER.debug("Found at least one idle transaction with pid " + rs.getInt("pid") + " for application" + rs.getString("application_name"));
                        return true;
                    }
                    else {
                        return false;
                    }
                });
    }

    private Lsn tryFallbackToRestartLsn(String slotName, String pluginName, String database, ResultSet rs) {
        Lsn confirmedFlushedLsn;
        LOGGER.info("Unable to find confirmed_flushed_lsn, falling back to restart_lsn");
        try {
            confirmedFlushedLsn = tryParseLsn(slotName, pluginName, database, rs, "restart_lsn");
        }
        catch (SQLException e) {
            throw new DebeziumException("Neither confirmed_flush_lsn nor restart_lsn could be found", e);
        }
        return confirmedFlushedLsn;
    }

    private Lsn parseRestartLsn(String slotName, String pluginName, String database, ResultSet rs) {
        Lsn restartLsn = null;
        try {
            restartLsn = tryParseLsn(slotName, pluginName, database, rs, "restart_lsn");
        }
        catch (SQLException e) {
            throw new DebeziumException("restart_lsn could be found");
        }

        return restartLsn;
    }

    private Lsn tryParseLsn(String slotName, String pluginName, String database, ResultSet rs, String column) throws ConnectException, SQLException {
        Lsn lsn = null;

        String lsnStr = rs.getString(column);
        if (lsnStr == null) {
            return null;
        }
        try {
            lsn = Lsn.valueOf(lsnStr);
        }
        catch (Exception e) {
            throw new DebeziumException("Value " + column + " in the pg_replication_slots table for slot = '"
                    + slotName + "', plugin = '"
                    + pluginName + "', database = '"
                    + database + "' is not valid. This is an abnormal situation and the database status should be checked.");
        }
        if (!lsn.isValid()) {
            throw new DebeziumException("Invalid LSN returned from database");
        }
        return lsn;
    }

    /**
     * Drops a replication slot that was created on the DB
     *
     * @param slotName the name of the replication slot, may not be null
     * @return {@code true} if the slot was dropped, {@code false} otherwise
     */
    public boolean dropReplicationSlot(String slotName) {
        final int ATTEMPTS = 3;
        for (int i = 0; i < ATTEMPTS; i++) {
            try {
                execute("select pg_drop_replication_slot('" + slotName + "')");
                return true;
            }
            catch (SQLException e) {
                // slot is active
                if (PSQLState.OBJECT_IN_USE.getState().equals(e.getSQLState())) {
                    if (i < ATTEMPTS - 1) {
                        LOGGER.debug("Cannot drop replication slot '{}' because it's still in use", slotName);
                    }
                    else {
                        LOGGER.warn("Cannot drop replication slot '{}' because it's still in use", slotName);
                        return false;
                    }
                }
                else if (PSQLState.UNDEFINED_OBJECT.getState().equals(e.getSQLState())) {
                    LOGGER.debug("Replication slot {} has already been dropped", slotName);
                    return false;
                }
                else {
                    LOGGER.error("Unexpected error while attempting to drop replication slot", e);
                    return false;
                }
            }
            try {
                Metronome.parker(Duration.ofSeconds(1), Clock.system()).pause();
            }
            catch (InterruptedException e) {
            }
        }
        return false;
    }

    /**
     * Drops the debezium publication that was created.
     *
     * @param publicationName the publication name, may not be null
     * @return {@code true} if the publication was dropped, {@code false} otherwise
     */
    public boolean dropPublication(String publicationName) {
        try {
            LOGGER.debug("Dropping publication '{}'", publicationName);
            execute("DROP PUBLICATION " + publicationName);
            return true;
        }
        catch (SQLException e) {
            if (PSQLState.UNDEFINED_OBJECT.getState().equals(e.getSQLState())) {
                LOGGER.debug("Publication {} has already been dropped", publicationName);
            }
            else {
                LOGGER.error("Unexpected error while attempting to drop publication", e);
            }
            return false;
        }
    }

    @Override
    public synchronized void close() {
        try {
            super.close();
        }
        catch (SQLException e) {
            LOGGER.error("Unexpected error while closing Postgres connection", e);
        }
    }

    /**
     * Returns the PG id of the current active transaction
     *
     * @return a PG transaction identifier, or null if no tx is active
     * @throws SQLException if anything fails.
     */
    public Long currentTransactionId() throws SQLException {
        AtomicLong txId = new AtomicLong(0);
        int majorVersion = connection().getMetaData().getDatabaseMajorVersion();
        String txIdQuery = majorVersion >= 13 ? "select (case pg_is_in_recovery() when 't' then '0'::xid8 else pg_current_xact_id() end) AS pg_current_txid"
                : "select (case pg_is_in_recovery() when 't' then 0 else txid_current() end) AS pg_current_txid";
        query(txIdQuery, rs -> {
            if (rs.next()) {
                txId.compareAndSet(0, rs.getLong(1));
            }
        });
        long value = txId.get();
        return value > 0 ? value : null;
    }

    /**
     * Returns the current position in the server tx log.
     *
     * @return a long value, never negative
     * @throws SQLException if anything unexpected fails.
     */
    public long currentXLogLocation() throws SQLException {
        AtomicLong result = new AtomicLong(0);
        int majorVersion = connection().getMetaData().getDatabaseMajorVersion();
        query(majorVersion >= 10 ? "select (case pg_is_in_recovery() when 't' then pg_last_wal_receive_lsn() else pg_current_wal_lsn() end) AS pg_current_wal_lsn"
                : "select * from pg_current_xlog_location()", rs -> {
                    if (!rs.next()) {
                        throw new IllegalStateException("there should always be a valid xlog position");
                    }
                    result.compareAndSet(0, LogSequenceNumber.valueOf(rs.getString(1)).asLong());
                });
        return result.get();
    }

    /**
     * Returns information about the PG server to which this instance is connected.
     *
     * @return a {@link ServerInfo} instance, never {@code null}
     * @throws SQLException if anything fails
     */
    public ServerInfo serverInfo() throws SQLException {
        ServerInfo serverInfo = new ServerInfo();
        query("SELECT version(), current_user, current_database()", rs -> {
            if (rs.next()) {
                serverInfo
                        .withServer(rs.getString(1))
                        .withUsername(rs.getString(2))
                        .withDatabase(rs.getString(3));
            }
        });
        String username = serverInfo.username();
        if (username != null) {
            query("SELECT oid, rolname, rolsuper, rolinherit, rolcreaterole, rolcreatedb, rolcanlogin, rolreplication FROM pg_roles " +
                    "WHERE pg_has_role('" + username + "', oid, 'member')",
                    rs -> {
                        while (rs.next()) {
                            String roleInfo = "superuser: " + rs.getBoolean(3) + ", replication: " + rs.getBoolean(8) +
                                    ", inherit: " + rs.getBoolean(4) + ", create role: " + rs.getBoolean(5) +
                                    ", create db: " + rs.getBoolean(6) + ", can log in: " + rs.getBoolean(7);
                            String roleName = rs.getString(2);
                            serverInfo.addRole(roleName, roleInfo);
                        }
                    });
        }

        serverInfo.withMajorVersion(connection().getMetaData().getDatabaseMajorVersion());

        return serverInfo;
    }

    public Charset getDatabaseCharset() {
        try {
            return Charset.forName(((BaseConnection) connection()).getEncoding().name());
        }
        catch (SQLException e) {
            throw new DebeziumException("Couldn't obtain encoding for database " + database(), e);
        }
    }

    public TimestampUtils getTimestampUtils() {
        try {
            return ((PgConnection) this.connection()).getTimestampUtils();
        }
        catch (SQLException e) {
            throw new DebeziumException("Couldn't get timestamp utils from underlying connection", e);
        }
    }

    private static void validateServerVersion(Statement statement) throws SQLException {
        DatabaseMetaData metaData = statement.getConnection().getMetaData();
        int majorVersion = metaData.getDatabaseMajorVersion();
        int minorVersion = metaData.getDatabaseMinorVersion();
        if (majorVersion < 9 || (majorVersion == 9 && minorVersion < 4)) {
            throw new SQLException("Cannot connect to a version of Postgres lower than 9.4");
        }
    }

    @Override
    protected int resolveNativeType(String typeName) {
        return getTypeRegistry().get(typeName).getRootType().getOid();
    }

    @Override
    protected int resolveJdbcType(int metadataJdbcType, int nativeType) {
        // Special care needs to be taken for columns that use user-defined domain type data types
        // where resolution of the column's JDBC type needs to be that of the root type instead of
        // the actual column to properly influence schema building and value conversion.
        return getTypeRegistry().get(nativeType).getRootType().getJdbcId();
    }

    @Override
    protected Optional<ColumnEditor> readTableColumn(ResultSet columnMetadata, TableId tableId, Tables.ColumnNameFilter columnFilter) throws SQLException {
        return doReadTableColumn(columnMetadata, tableId, columnFilter);
    }

    public Optional<Column> readColumnForDecoder(ResultSet columnMetadata, TableId tableId, Tables.ColumnNameFilter columnNameFilter)
            throws SQLException {
        return doReadTableColumn(columnMetadata, tableId, columnNameFilter).map(ColumnEditor::create);
    }

    private Optional<ColumnEditor> doReadTableColumn(ResultSet columnMetadata, TableId tableId, Tables.ColumnNameFilter columnFilter)
            throws SQLException {
        final String columnName = columnMetadata.getString(4);
        if (columnFilter == null || columnFilter.matches(tableId.catalog(), tableId.schema(), tableId.table(), columnName)) {
            final ColumnEditor column = Column.editor().name(columnName);
            column.type(columnMetadata.getString(6));

            // first source the length/scale from the column metadata provided by the driver
            // this may be overridden below if the column type is a user-defined domain type
            column.length(columnMetadata.getInt(7));
            if (columnMetadata.getObject(9) != null) {
                column.scale(columnMetadata.getInt(9));
            }

            column.optional(isNullable(columnMetadata.getInt(11)));
            column.position(columnMetadata.getInt(17));
            column.autoIncremented("YES".equalsIgnoreCase(columnMetadata.getString(23)));

            String autogenerated = null;
            try {
                autogenerated = columnMetadata.getString(24);
            }
            catch (SQLException e) {
                // ignore, some drivers don't have this index - e.g. Postgres
            }
            column.generated("YES".equalsIgnoreCase(autogenerated));

            // Lookup the column type from the TypeRegistry
            // For all types, we need to set the Native and Jdbc types by using the root-type
            final PostgresType nativeType = getTypeRegistry().get(column.typeName());
            column.nativeType(nativeType.getRootType().getOid());
            column.jdbcType(nativeType.getRootType().getJdbcId());

            // For domain types, the postgres driver is unable to traverse a nested unbounded
            // hierarchy of types and report the right length/scale of a given type. We use
            // the TypeRegistry to accomplish this since it is capable of traversing the type
            // hierarchy upward to resolve length/scale regardless of hierarchy depth.
            if (TypeRegistry.DOMAIN_TYPE == nativeType.getJdbcId()) {
                column.length(nativeType.getDefaultLength());
                column.scale(nativeType.getDefaultScale());
            }

            final String defaultValueExpression = columnMetadata.getString(13);
            if (defaultValueExpression != null && getDefaultValueConverter().supportConversion(column.typeName())) {
                column.defaultValueExpression(defaultValueExpression);
            }

            return Optional.of(column);
        }

        return Optional.empty();
    }

    public PostgresDefaultValueConverter getDefaultValueConverter() {
        Objects.requireNonNull(defaultValueConverter, "Connection does not provide default value converter");
        return defaultValueConverter;
    }

    public TypeRegistry getTypeRegistry() {
        Objects.requireNonNull(typeRegistry, "Connection does not provide type registry");
        return typeRegistry;
    }

    @Override
    public Object getColumnValue(ResultSet rs, int columnIndex, Column column, Table table) throws SQLException {
        try {
            final ResultSetMetaData metaData = rs.getMetaData();
            final String columnTypeName = metaData.getColumnTypeName(columnIndex);
            final PostgresType type = getTypeRegistry().get(columnTypeName);

            LOGGER.trace("Type of incoming data is: {}", type.getOid());
            LOGGER.trace("ColumnTypeName is: {}", columnTypeName);
            LOGGER.trace("Type is: {}", type);

            if (type.isArrayType()) {
                return rs.getArray(columnIndex);
            }

            switch (type.getOid()) {
                case PgOid.MONEY:
                    // TODO author=Horia Chiorean date=14/11/2016 description=workaround for https://github.com/pgjdbc/pgjdbc/issues/100
                    final String sMoney = rs.getString(columnIndex);
                    if (sMoney == null) {
                        return sMoney;
                    }
                    if (sMoney.startsWith("-")) {
                        // PGmoney expects negative values to be provided in the format of "($XXXXX.YY)"
                        final String negativeMoney = "(" + sMoney.substring(1) + ")";
                        return new PGmoney(negativeMoney).val;
                    }
                    return new PGmoney(sMoney).val;
                case PgOid.BIT:
                    return rs.getString(columnIndex);
                case PgOid.NUMERIC:
                    final String s = rs.getString(columnIndex);
                    if (s == null) {
                        return s;
                    }

                    Optional<SpecialValueDecimal> value = PostgresValueConverter.toSpecialValue(s);
                    return value.isPresent() ? value.get() : new SpecialValueDecimal(rs.getBigDecimal(columnIndex));
                case PgOid.TIME:
                    // To handle time 24:00:00 supported by TIME columns, read the column as a string.
                case PgOid.TIMETZ:
                    // In order to guarantee that we resolve TIMETZ columns with proper microsecond precision,
                    // read the column as a string instead and then re-parse inside the converter.
                    return rs.getString(columnIndex);
                default:
                    Object x = rs.getObject(columnIndex);
                    if (x != null) {
                        LOGGER.trace("rs getobject returns class: {}; rs getObject value is: {}", x.getClass(), x);
                    }
                    return x;
            }
        }
        catch (SQLException e) {
            // not a known type
            return super.getColumnValue(rs, columnIndex, column, table);
        }
    }

    @Override
    protected String[] supportedTableTypes() {
        return new String[]{ "VIEW", "MATERIALIZED VIEW", "TABLE", "PARTITIONED TABLE" };
    }

    @Override
    protected boolean isTableType(String tableType) {
        return "TABLE".equals(tableType) || "PARTITIONED TABLE".equals(tableType);
    }

    @Override
    protected boolean isTableUniqueIndexIncluded(String indexName, String columnName) {
        if (columnName != null) {
            return !FUNCTION_DEFAULT_PATTERN.matcher(columnName).matches()
                    && !EXPRESSION_DEFAULT_PATTERN.matcher(columnName).matches();
        }
        return false;
    }

    /**
     * Retrieves all {@code TableId}s in a given database catalog, including partitioned tables.
     *
     * @param catalogName the catalog/database name
     * @return set of all table ids for existing table objects
     * @throws SQLException if a database exception occurred
     */
    public Set<TableId> getAllTableIds(String catalogName) throws SQLException {
        return readTableNames(
                catalogName,
                null,
                null,
                new String[]{ "TABLE", "PARTITIONED TABLE" });
    }

    @Override
    public <T extends DataCollectionId> ChunkQueryBuilder<T> chunkQueryBuilder(RelationalDatabaseConnectorConfig connectorConfig) {
        // PostgreSQL definitely must use row value constructors in order to yield optimal results. See DBZ-5071.
        return new RowValueConstructorChunkQueryBuilder<>(connectorConfig, this);
    }

    @Override
    public Optional<Boolean> nullsSortLast() {
        // "By default, null values sort as if larger than any non-null value"
        // https://www.postgresql.org/docs/16/queries-order.html
        return Optional.of(true);
    }

    @Override
    public Map<String, Object> reselectColumns(Table table, List<String> columns, List<String> keyColumns, List<Object> keyValues, Struct source)
            throws SQLException {
        final String query = String.format("SELECT %s FROM %s WHERE %s",
                columns.stream().map(this::quoteIdentifier).collect(Collectors.joining(",")),
                quotedTableIdString(table.id()),
                keyColumns.stream()
                        .map(key -> {
                            Column column = table.columnWithName(key);
                            String castableType = typeRegistry.get(column.nativeType()).getName();
                            return key + "=?::" + castableType;
                        })
                        .collect(Collectors.joining(" AND ")));
        return reselectColumns(query, table.id(), columns, keyValues);
    }

    @Override
    public void setQueryColumnValue(PreparedStatement statement, Column column, int pos, Object value)
            throws SQLException {
        final PostgresType resolvedType = typeRegistry.get(column.nativeType());

        if (resolvedType != null && resolvedType.isEnumType()) {
            // ENUMs require explicit casting so the comparison operators can correctly work
            statement.setObject(pos, value, Types.OTHER);
        }
        else {
            super.setQueryColumnValue(statement, column, pos, value);
        }
    }

    @Override
    public TableId createTableId(String databaseName, String schemaName, String tableName) {
        return new TableId(null, schemaName, tableName);
    }

    public boolean validateLogPosition(Partition partition, OffsetContext offset, CommonConnectorConfig config) {

        final Lsn storedLsn = ((PostgresOffsetContext) offset).lastCommitLsn();
        final String slotName = ((PostgresConnectorConfig) config).slotName();
        final String postgresPluginName = ((PostgresConnectorConfig) config).plugin().getPostgresPluginName();

        try {
            SlotState slotState = getReplicationSlotState(slotName, postgresPluginName);
            if (slotState == null) {
                return false;
            }
            LOGGER.info("Slot '{}' has restart LSN '{}'", slotName, slotState.slotRestartLsn());
            return storedLsn == null || slotState.slotRestartLsn().compareTo(storedLsn) <= 0;
        }
        catch (SQLException e) {
            throw new DebeziumException("Unable to get last available log position", e);
        }
    }

    public List<Column> getTableColumnsForDecoder(TableId tableId, Tables.ColumnNameFilter columnFilter) throws SQLException {
        try {
            final List<Column> readColumns = new ArrayList<>();

            final DatabaseMetaData databaseMetaData = connection().getMetaData();
            final String schemaNamePattern = createPatternFromName(tableId.schema(), databaseMetaData.getSearchStringEscape());
            final String tableNamePattern = createPatternFromName(tableId.table(), databaseMetaData.getSearchStringEscape());

            try (ResultSet columnMetadata = databaseMetaData.getColumns(null, schemaNamePattern, tableNamePattern, null)) {
                while (columnMetadata.next()) {
                    readColumnForDecoder(columnMetadata, tableId, columnFilter).ifPresent(readColumns::add);
                }
            }

            return readColumns;
        }
        catch (SQLException e) {
            LOGGER.error("Failed to read column metadata for '{}.{}'", tableId.schema(), tableId.table());
            throw e;
        }
    }

    @FunctionalInterface
    public interface PostgresValueConverterBuilder {
        PostgresValueConverter build(TypeRegistry registry);
    }
}
