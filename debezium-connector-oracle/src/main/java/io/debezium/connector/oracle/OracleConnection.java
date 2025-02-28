/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Field;
import io.debezium.connector.oracle.OracleConnectorConfig.ConnectorAdapter;
import io.debezium.connector.oracle.logminer.SqlUtils;
import io.debezium.connector.oracle.util.OracleUtils;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.Attribute;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.Tables.ColumnNameFilter;
import io.debezium.util.Strings;

import oracle.jdbc.OracleTypes;
import oracle.sql.CharacterSet;

public class OracleConnection extends JdbcConnection {

    private final static Logger LOGGER = LoggerFactory.getLogger(OracleConnection.class);

    /**
     * Returned by column metadata in Oracle if no scale is set;
     */
    private static final int ORACLE_UNSET_SCALE = -127;

    /**
     * Pattern to identify system generated indices and column names.
     */
    private static final Pattern SYS_NC_PATTERN = Pattern.compile("^SYS_NC(?:_OID|_ROWINFO|[0-9][0-9][0-9][0-9][0-9])\\$$");

    /**
     * Pattern to identify abstract data type indices and column names.
     */
    private static final Pattern ADT_INDEX_NAMES_PATTERN = Pattern.compile("^\".*\"\\.\".*\".*");

    /**
     * Pattern to identify a hidden column based on redefining a table with the {@code ROWID} option.
     */
    private static final Pattern MROW_PATTERN = Pattern.compile("^M_ROW\\$\\$");

    /**
     * A field for the raw jdbc url. This field has no default value.
     */
    private static final Field URL = Field.create("url", "Raw JDBC url");

    /**
     * The database version.
     */
    private final OracleDatabaseVersion databaseVersion;

    private static final String QUOTED_CHARACTER = "\"";

    public OracleConnection(JdbcConfiguration config) {
        this(config, true);
    }

    public OracleConnection(JdbcConfiguration config, ConnectionFactory connectionFactory) {
        this(config, connectionFactory, true);
    }

    public OracleConnection(JdbcConfiguration config, ConnectionFactory connectionFactory, boolean showVersion) {
        super(config, connectionFactory, QUOTED_CHARACTER, QUOTED_CHARACTER);
        LOGGER.trace("JDBC connection string: " + connectionString(config));
        this.databaseVersion = resolveOracleDatabaseVersion();
        if (showVersion) {
            LOGGER.info("Database Version: {}", databaseVersion.getBanner());
        }
    }

    public OracleConnection(JdbcConfiguration config, boolean showVersion) {
        super(config, resolveConnectionFactory(config), QUOTED_CHARACTER, QUOTED_CHARACTER);
        LOGGER.trace("JDBC connection string: " + connectionString(config));
        this.databaseVersion = resolveOracleDatabaseVersion();
        if (showVersion) {
            LOGGER.info("Database Version: {}", databaseVersion.getBanner());
        }
    }

    public void setSessionToPdb(String pdbName) {
        setContainerAs(pdbName);
    }

    public void resetSessionToCdb() {
        setContainerAs("cdb$root");
    }

    private void setContainerAs(String containerName) {
        Statement statement = null;
        try {
            statement = connection().createStatement();
            statement.execute("alter session set container=" + containerName);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
        finally {
            if (statement != null) {
                try {
                    statement.close();
                }
                catch (SQLException e) {
                    LOGGER.error("Couldn't close statement", e);
                }
            }
        }
    }

    public OracleDatabaseVersion getOracleVersion() {
        return databaseVersion;
    }

    private OracleDatabaseVersion resolveOracleDatabaseVersion() {
        String versionStr;
        try {
            try {
                // Oracle 18.1 introduced BANNER_FULL as the new column rather than BANNER
                // This column uses a different format than the legacy BANNER.
                versionStr = queryAndMap("SELECT BANNER_FULL FROM V$VERSION WHERE BANNER_FULL LIKE 'Oracle Database%'", (rs) -> {
                    if (rs.next()) {
                        return rs.getString(1);
                    }
                    return null;
                });
            }
            catch (SQLException e) {
                // exception ignored
                if (e.getMessage().contains("ORA-00904: \"BANNER_FULL\"")) {
                    LOGGER.debug("BANNER_FULL column not in V$VERSION, using BANNER column as fallback");
                    versionStr = null;
                }
                else {
                    throw e;
                }
            }

            // For databases prior to 18.1, a SQLException will be thrown due to BANNER_FULL not being a column and
            // this will cause versionStr to remain null, use fallback column BANNER for versions prior to 18.1.
            if (versionStr == null) {
                versionStr = queryAndMap("SELECT BANNER FROM V$VERSION WHERE BANNER LIKE 'Oracle Database%'", (rs) -> {
                    if (rs.next()) {
                        return rs.getString(1);
                    }
                    return null;
                });
            }
        }
        catch (SQLException e) {
            if (e instanceof SQLRecoverableException) {
                throw new RetriableException("Failed to resolve Oracle database version", e);
            }
            throw new RuntimeException("Failed to resolve Oracle database version", e);
        }

        if (versionStr == null) {
            throw new RuntimeException("Failed to resolve Oracle database version");
        }

        return OracleDatabaseVersion.parse(versionStr);
    }

    @Override
    public Set<TableId> readTableNames(String databaseCatalog, String schemaNamePattern, String tableNamePattern,
                                       String[] tableTypes)
            throws SQLException {

        Set<TableId> tableIds = super.readTableNames(null, schemaNamePattern, tableNamePattern, tableTypes);

        return tableIds.stream()
                .map(t -> new TableId(databaseCatalog, t.schema(), t.table()))
                .collect(Collectors.toSet());
    }

    /**
     * Retrieves all {@code TableId} in a given database catalog, filtering certain ids that
     * should be omitted from the returned set such as special spatial tables and index-organized
     * tables.
     *
     * @param catalogName the catalog/database name
     * @return set of all table ids for existing table objects
     * @throws SQLException if a database exception occurred
     */
    protected Set<TableId> getAllTableIds(String catalogName) throws SQLException {
        final String query = "select owner, table_name from all_tables " +
        // filter special spatial tables
                "where table_name NOT LIKE 'MDRT_%' " +
                "and table_name NOT LIKE 'MDRS_%' " +
                "and table_name NOT LIKE 'MDXT_%' " +
                // filter index-organized-tables
                "and (table_name NOT LIKE 'SYS_IOT_OVER_%' and IOT_NAME IS NULL) " +
                // filter nested tables
                "and nested = 'NO'" +
                // filter parent tables of nested tables
                "and table_name not in (select PARENT_TABLE_NAME from ALL_NESTED_TABLES)";

        Set<TableId> tableIds = new HashSet<>();
        query(query, (rs) -> {
            while (rs.next()) {
                tableIds.add(new TableId(catalogName, rs.getString(1), rs.getString(2)));
            }
            LOGGER.trace("TableIds are: {}", tableIds);
        });

        return tableIds;
    }

    @Override
    protected String resolveCatalogName(String catalogName) {
        final String pdbName = OracleUtils.getObjectName(config().getString("pdb.name"));
        final String databaseName = OracleUtils.getObjectName(config().getString("dbname"));
        return !OracleUtils.isObjectNameNullOrEmpty(pdbName) ? pdbName : databaseName;
    }

    @Override
    public List<String> readTableUniqueIndices(DatabaseMetaData metadata, TableId id) throws SQLException {
        return super.readTableUniqueIndices(metadata, id.toDoubleQuoted());
    }

    @Override
    public Optional<Instant> getCurrentTimestamp() throws SQLException {
        return queryAndMap("SELECT CURRENT_TIMESTAMP FROM DUAL",
                rs -> rs.next() ? Optional.of(rs.getTimestamp(1).toInstant()) : Optional.empty());
    }

    @Override
    protected boolean isTableUniqueIndexIncluded(String indexName, String columnName) {
        if (columnName != null) {
            return !SYS_NC_PATTERN.matcher(columnName).matches()
                    && !ADT_INDEX_NAMES_PATTERN.matcher(columnName).matches()
                    && !MROW_PATTERN.matcher(columnName).matches();
        }
        return false;
    }

    /**
     * Get the current, most recent system change number.
     *
     * @return the current system change number
     * @throws SQLException if an exception occurred
     * @throws IllegalStateException if the query does not return at least one row
     */
    public Scn getCurrentScn() throws SQLException {
        return queryAndMap("SELECT CURRENT_SCN FROM V$DATABASE", (rs) -> {
            if (rs.next()) {
                return Scn.valueOf(rs.getString(1));
            }
            throw new IllegalStateException("Could not get SCN");
        });
    }

    /**
     * Generate a given table's DDL metadata.
     *
     * @param tableId table identifier, should never be {@code null}
     * @return generated DDL
     * @throws SQLException if an exception occurred obtaining the DDL metadata
     * @throws NonRelationalTableException the table is not a relational table
     */
    public String getTableMetadataDdl(TableId tableId) throws SQLException, NonRelationalTableException {
        try {
            // This table contains all available objects that are considered relational & object based.
            // By querying for TABLE_TYPE is null, we are explicitly confirming what if an entry exists
            // that the table is in-fact a relational table and if the result set is empty, the object
            // is another type, likely an object-based table, in which case we cannot generate DDL.
            final String tableType = "SELECT COUNT(1) FROM ALL_ALL_TABLES WHERE OWNER=? AND TABLE_NAME=? AND TABLE_TYPE IS NULL";
            if (prepareQueryAndMap(tableType,
                    ps -> {
                        ps.setString(1, tableId.schema());
                        ps.setString(2, tableId.table());
                    },
                    rs -> rs.next() ? rs.getInt(1) : 0) == 0) {
                throw new NonRelationalTableException("Table " + tableId + " is not a relational table");
            }

            // The storage and segment attributes aren't necessary
            executeWithoutCommitting("begin dbms_metadata.set_transform_param(DBMS_METADATA.SESSION_TRANSFORM, 'STORAGE', false); end;");
            executeWithoutCommitting("begin dbms_metadata.set_transform_param(DBMS_METADATA.SESSION_TRANSFORM, 'SEGMENT_ATTRIBUTES', false); end;");
            // In case DDL is returned as multiple DDL statements, this allows the parser to parse each separately.
            // This is only critical during streaming as during snapshot the table structure is built from JDBC driver queries.
            executeWithoutCommitting("begin dbms_metadata.set_transform_param(DBMS_METADATA.SESSION_TRANSFORM, 'SQLTERMINATOR', true); end;");
            return prepareQueryAndMap(
                    "SELECT dbms_metadata.get_ddl('TABLE',?,?) FROM DUAL",
                    ps -> {
                        ps.setString(1, tableId.table());
                        ps.setString(2, tableId.schema());
                    },
                    rs -> {
                        if (!rs.next()) {
                            throw new DebeziumException("Could not get DDL metadata for table: " + tableId);
                        }

                        Object res = rs.getObject(1);
                        return ((Clob) res).getSubString(1, (int) ((Clob) res).length());
                    });
        }
        finally {
            executeWithoutCommitting("begin dbms_metadata.set_transform_param(DBMS_METADATA.SESSION_TRANSFORM, 'DEFAULT'); end;");
        }
    }

    /**
     * Get the current connection's session statistic by name.
     *
     * @param name the name of the statistic to be fetched, must not be {@code null}
     * @return the session statistic value, never {@code null}
     * @throws SQLException if an exception occurred obtaining the session statistic value
     */
    public Long getSessionStatisticByName(String name) throws SQLException {
        return queryAndMap("SELECT VALUE FROM v$statname n, v$mystat m WHERE n.name='" + name +
                "' AND n.statistic#=m.statistic#", rs -> rs.next() ? rs.getLong(1) : 0L);
    }

    /**
     * Returns whether the given table exists or not.
     *
     * @param tableId table id, should not be {@code null}
     * @return true if the table exists, false if it does not
     * @throws SQLException if a database exception occurred
     */
    public boolean isTableExists(TableId tableId) throws SQLException {
        if (Strings.isNullOrBlank(tableId.schema())) {
            return prepareQueryAndMap("SELECT COUNT(1) FROM USER_TABLES WHERE TABLE_NAME=?",
                    ps -> ps.setString(1, tableId.table()),
                    rs -> rs.next() && rs.getLong(1) > 0);
        }
        return prepareQueryAndMap("SELECT COUNT(1) FROM ALL_TABLES WHERE OWNER=? AND TABLE_NAME=?",
                ps -> {
                    ps.setString(1, tableId.schema());
                    ps.setString(2, tableId.table());
                },
                rs -> rs.next() && rs.getLong(1) > 0);
    }

    /**
     * Returns whether the given table is empty or not.
     *
     * @param tableId table id, should not be {@code null}
     * @return true if the table has no records, false otherwise
     * @throws SQLException if a database exception occurred
     */
    public boolean isTableEmpty(TableId tableId) throws SQLException {
        return getRowCount(tableId) == 0L;
    }

    /**
     * Returns the number of rows in a given table.
     *
     * @param tableId table id, should not be {@code null}
     * @return the number of rows
     * @throws SQLException if a database exception occurred
     */
    public long getRowCount(TableId tableId) throws SQLException {
        return queryAndMap("SELECT COUNT(1) FROM " + tableId.toDoubleQuotedString(), rs -> {
            if (rs.next()) {
                return rs.getLong(1);
            }
            return 0L;
        });
    }

    public <T> T singleOptionalValue(String query, ResultSetExtractor<T> extractor) throws SQLException {
        return queryAndMap(query, rs -> rs.next() ? extractor.apply(rs) : null);
    }

    /**
     * Gets the first system change number in both archive and redo logs.
     *
     * @param archiveLogRetention retention of the archive log
     * @param archiveDestinationName name of the archive log destination to be used for reading archive logs
     * @return the oldest system change number
     * @throws SQLException      if a database exception occurred
     * @throws DebeziumException if the oldest system change number cannot be found due to no logs available
     */
    public Optional<Scn> getFirstScnInLogs(Duration archiveLogRetention, String archiveDestinationName) throws SQLException {

        final String oldestFirstChangeQuery = SqlUtils.oldestFirstChangeQuery(archiveLogRetention, archiveDestinationName);
        final String oldestScn = singleOptionalValue(oldestFirstChangeQuery, rs -> rs.getString(1));

        if (oldestScn == null) {
            return Optional.empty();
        }

        LOGGER.trace("Oldest SCN in logs is '{}'", oldestScn);
        return Optional.of(Scn.valueOf(oldestScn));
    }

    public boolean validateLogPosition(Partition partition, OffsetContext offset, CommonConnectorConfig config) {

        final Duration archiveLogRetention = ((OracleConnectorConfig) config).getArchiveLogRetention();
        final String archiveDestinationName = ((OracleConnectorConfig) config).getArchiveLogDestinationName();
        final Scn storedOffset = ((OracleConnectorConfig) config).getAdapter().getOffsetScn((OracleOffsetContext) offset);

        try {
            Optional<Scn> firstAvailableScn = getFirstScnInLogs(archiveLogRetention, archiveDestinationName);
            return firstAvailableScn.filter(isLessThan(storedOffset)).isPresent();
        }
        catch (SQLException e) {
            throw new DebeziumException("Unable to get last available log position", e);
        }
    }

    private static Predicate<Scn> isLessThan(Scn storedOffset) {
        return scn -> scn.compareTo(storedOffset) < 0;
    }

    @Override
    public String buildSelectWithRowLimits(TableId tableId,
                                           int limit,
                                           String projection,
                                           Optional<String> condition,
                                           Optional<String> additionalCondition,
                                           String orderBy) {
        final TableId table = new TableId(null, tableId.schema(), tableId.table());
        final StringBuilder sql = new StringBuilder("SELECT ");
        sql
                .append(projection)
                .append(" FROM ");
        sql.append(quotedTableIdString(table));
        if (condition.isPresent()) {
            sql
                    .append(" WHERE ")
                    .append(condition.get());
            if (additionalCondition.isPresent()) {
                sql.append(" AND ");
                sql.append(additionalCondition.get());
            }
        }
        else if (additionalCondition.isPresent()) {
            sql.append(" WHERE ");
            sql.append(additionalCondition.get());
        }
        if (getOracleVersion().getMajor() < 12) {
            sql
                    .insert(0, " SELECT * FROM (")
                    .append(" ORDER BY ")
                    .append(orderBy)
                    .append(")")
                    .append(" WHERE ROWNUM <=")
                    .append(limit);
        }
        else {
            sql
                    .append(" ORDER BY ")
                    .append(orderBy)
                    .append(" FETCH NEXT ")
                    .append(limit)
                    .append(" ROWS ONLY");
        }
        return sql.toString();
    }

    public static String connectionString(JdbcConfiguration config) {
        return config.getString(URL) != null ? config.getString(URL)
                : ConnectorAdapter.parse(config.getString("connection.adapter")).getConnectionUrl();
    }

    private static ConnectionFactory resolveConnectionFactory(JdbcConfiguration config) {
        return JdbcConnection.patternBasedFactory(connectionString(config));
    }

    /**
     * Determine whether the Oracle server has the archive log enabled.
     *
     * @return {@code true} if the server's {@code LOG_MODE} is set to {@code ARCHIVELOG}, or {@code false} otherwise
     */
    protected boolean isArchiveLogMode() {
        try {
            final String mode = queryAndMap("SELECT LOG_MODE FROM V$DATABASE", rs -> rs.next() ? rs.getString(1) : "");
            LOGGER.debug("LOG_MODE={}", mode);
            return "ARCHIVELOG".equalsIgnoreCase(mode);
        }
        catch (SQLException e) {
            throw new DebeziumException("Unexpected error while connecting to Oracle and looking at LOG_MODE mode: ", e);
        }
    }

    /**
     * Resolve a system change number to a timestamp, return value is in database timezone.
     *
     * The SCN to TIMESTAMP mapping is only retained for the duration of the flashback query area.
     * This means that eventually the mapping between these values are no longer kept by Oracle
     * and making a call with a SCN value that has aged out will result in an ORA-08181 error.
     * This function explicitly checks for this use case and if a ORA-08181 error is thrown, it is
     * therefore treated as if a value does not exist returning an empty optional value.
     *
     * @param scn the system change number, must not be {@code null}
     * @return an optional timestamp when the system change number occurred
     * @throws SQLException if a database exception occurred
     */
    public Optional<Instant> getScnToTimestamp(Scn scn) throws SQLException {
        try {
            return queryAndMap("SELECT scn_to_timestamp('" + scn + "') FROM DUAL", rs -> rs.next()
                    ? Optional.of(rs.getTimestamp(1).toInstant())
                    : Optional.empty());
        }
        catch (SQLException e) {
            if (e.getMessage().startsWith("ORA-08181")) {
                // ORA-08181 specified number is not a valid system change number
                // This happens when the SCN provided is outside the flashback area range
                // This should be treated as a value is not available rather than an error
                return Optional.empty();
            }
            // Any other SQLException should be thrown
            throw e;
        }
    }

    public Scn getScnAdjustedByTime(Scn scn, Duration adjustment) throws SQLException {
        try {
            final String result = prepareQueryAndMap(
                    "SELECT timestamp_to_scn(scn_to_timestamp(?) - (? / 86400000)) FROM DUAL",
                    st -> {
                        st.setString(1, scn.toString());
                        st.setLong(2, adjustment.toMillis());
                    },
                    singleResultMapper(rs -> rs.getString(1), "Failed to get adjusted SCN from: " + scn));
            return Scn.valueOf(result);
        }
        catch (SQLException e) {
            if (e.getErrorCode() == 8181 || e.getErrorCode() == 8180) {
                // This happens when the SCN provided is outside the flashback/undo area
                return Scn.NULL;
            }
            throw e;
        }
    }

    /**
     * Get the database system time in the database system's time zone.
     *
     * @return the database system time
     * @throws SQLException if a database exception occurred
     */
    public OffsetDateTime getDatabaseSystemTime() throws SQLException {
        return singleOptionalValue("SELECT SYSTIMESTAMP FROM DUAL", rs -> rs.getObject(1, OffsetDateTime.class));
    }

    public boolean isArchiveLogDestinationValid(String archiveDestinationName) throws SQLException {
        return prepareQueryAndMap("SELECT STATUS, TYPE FROM V$ARCHIVE_DEST_STATUS WHERE DEST_NAME=?",
                st -> st.setString(1, archiveDestinationName),
                rs -> {
                    if (!rs.next()) {
                        throw new DebeziumException(
                                String.format("Archive log destination name '%s' is unknown to Oracle",
                                        archiveDestinationName));
                    }
                    return "VALID".equals(rs.getString("STATUS")) && "LOCAL".equals(rs.getString("TYPE"));
                });
    }

    public boolean isOnlyOneArchiveLogDestinationValid() throws SQLException {
        return queryAndMap("SELECT COUNT(1) FROM V$ARCHIVE_DEST_STATUS WHERE STATUS='VALID' AND TYPE='LOCAL'",
                rs -> {
                    if (!rs.next()) {
                        throw new DebeziumException("Unable to resolve number of archive log destinations");
                    }
                    return rs.getLong(1) == 1L;
                });
    }

    @Override
    protected ColumnEditor overrideColumn(ColumnEditor column) {
        // This allows the column state to be overridden before default-value resolution so that the
        // output of the default value is within the same precision as that of the column values.
        if (OracleTypes.TIMESTAMP == column.jdbcType()) {
            column.length(column.scale().orElse(Column.UNSET_INT_VALUE)).scale(null);
        }
        else if (OracleTypes.NUMBER == column.jdbcType()) {
            column.scale().filter(s -> s == ORACLE_UNSET_SCALE).ifPresent(s -> column.scale(null));
        }
        return column;
    }

    @Override
    protected Map<TableId, List<Column>> getColumnsDetails(String databaseCatalog,
                                                           String schemaNamePattern,
                                                           String tableName,
                                                           Tables.TableFilter tableFilter,
                                                           ColumnNameFilter columnFilter,
                                                           DatabaseMetaData metadata,
                                                           Set<TableId> viewIds)
            throws SQLException {
        // The Oracle JDBC driver expects that if the table name contains a "/" character that
        // the table name is pre-escaped prior to the JDBC driver call, or else it throws an
        // exception about the character sequence being improperly escaped.
        if (tableName != null && tableName.contains("/")) {
            tableName = tableName.replace("/", "//");
        }
        return super.getColumnsDetails(databaseCatalog, schemaNamePattern, tableName, tableFilter, columnFilter, metadata, viewIds);
    }

    /**
     * An exception that indicates the operation failed because the table is not a relational table.
     */
    public static class NonRelationalTableException extends DebeziumException {
        public NonRelationalTableException(String message) {
            super(message);
        }
    }

    @Override
    public Optional<Boolean> nullsSortLast() {
        // "NULLS LAST is the default for ascending order"
        // https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/SELECT.html
        return Optional.of(true);
    }

    @Override
    public Map<String, Object> reselectColumns(Table table, List<String> columns, List<String> keyColumns, List<Object> keyValues, Struct source)
            throws SQLException {
        final TableId oracleTableId = new TableId(null, table.id().schema(), table.id().table());
        if (source != null) {
            final String commitScn = source.getString(SourceInfo.COMMIT_SCN_KEY);
            if (!Strings.isNullOrEmpty(commitScn)) {
                final String query = String.format("SELECT %s FROM (SELECT * FROM %s AS OF SCN ?) WHERE %s",
                        columns.stream().map(this::quotedColumnIdString).collect(Collectors.joining(",")),
                        quotedTableIdString(oracleTableId),
                        keyColumns.stream().map(key -> key + "=?").collect(Collectors.joining(" AND ")));
                final List<Object> bindValues = new ArrayList<>(keyValues.size() + 1);
                bindValues.add(commitScn);
                bindValues.addAll(keyValues);
                try {
                    return reselectColumns(query, oracleTableId, columns, bindValues);
                }
                catch (SQLException e) {
                    // Check if the exception is about a flashback area error with an aged SCN
                    if (!(e.getErrorCode() == 1555 || e.getMessage().startsWith("ORA-01555"))) {
                        throw e;
                    }
                    LOGGER.warn("Failed to re-select row for table {} and key columns {} with values {}. " +
                            "Trying to perform re-selection without flashback.", table.id(), keyColumns, keyValues);
                }
            }
        }

        final String query = String.format("SELECT %s FROM %s WHERE %s",
                columns.stream().map(this::quotedColumnIdString).collect(Collectors.joining(",")),
                quotedTableIdString(oracleTableId),
                keyColumns.stream().map(key -> key + "=?").collect(Collectors.joining(" AND ")));

        return reselectColumns(query, oracleTableId, columns, keyValues);
    }

    @Override
    protected Map<TableId, List<Attribute>> getAttributeDetails(TableId tableId, String tableType) {
        final Map<TableId, List<Attribute>> results = new HashMap<>();
        try {
            getDatabaseObjectDetails(tableId, tableType, (objectId, dataObjectId) -> {
                LOGGER.info("\tRegistering '{}' attributes: object_id={}, data_object_id={}", tableId, objectId, dataObjectId);
                final List<Attribute> attributes = new ArrayList<>();
                attributes.add(Attribute.editor().name(OracleDatabaseSchema.ATTRIBUTE_OBJECT_ID).value(objectId).create());
                attributes.add(Attribute.editor().name(OracleDatabaseSchema.ATTRIBUTE_DATA_OBJECT_ID).value(dataObjectId).create());
                results.put(tableId, attributes);
            });
            return results;
        }
        catch (SQLException e) {
            throw new DebeziumException("Failed to get table attributes for table: " + tableId, e);
        }
    }

    @SuppressWarnings("resource")
    private void getDatabaseObjectDetails(TableId tableId, String tableType, ObjectIdentifierConsumer consumer) throws SQLException {
        final String query = "SELECT OBJECT_ID, DATA_OBJECT_ID FROM ALL_OBJECTS WHERE OBJECT_NAME=? AND OWNER=? AND OBJECT_TYPE=?";
        prepareQuery(query, List.of(tableId.table(), tableId.schema(), tableType), (params, rs) -> {
            if (!rs.next()) {
                throw new SQLException("Query '" + query + "' returned no results.");
            }
            consumer.apply(rs.getLong(1), rs.getLong(2));
        });
    }

    public Long getTableObjectId(TableId tableId) throws SQLException {
        return prepareQueryAndMap(
                "SELECT OBJECT_ID FROM ALL_OBJECTS WHERE OBJECT_TYPE='TABLE' AND OWNER=? AND OBJECT_NAME=?",
                ps -> {
                    ps.setString(1, tableId.schema());
                    ps.setString(2, tableId.table());
                }, rs -> rs.next() ? rs.getLong(1) : null);
    }

    public Long getTableDataObjectId(TableId tableId) throws SQLException {
        return prepareQueryAndMap(
                "SELECT DATA_OBJECT_ID FROM ALL_OBJECTS WHERE OBJECT_TYPE='TABLE' AND OWNER=? AND OBJECT_NAME=?",
                ps -> {
                    ps.setString(1, tableId.schema());
                    ps.setString(2, tableId.table());
                }, rs -> rs.next() ? rs.getLong(1) : null);
    }

    /**
     * Get the nationalized character set used for {@code NVARCHAR} and {@code NCHAR} data types.
     *
     * This method will lazily fetch the nationalized character set once per runtime. This is because
     * the nationalized character set must be set only at database creation, and therefore it is fine to
     * lazily query this when needed the first time and cache the value.
     *
     * <a href="https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Data-Types.html#GUID-FE15E51B-52C6-45D7-9883-4DF47716A17D">NCHAR</a>
     * <a href="https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Data-Types.html#GUID-CC15FC97-BE94-4FA4-994A-6DDF7F1A9904">NVARCHAR2</a>
     *
     * @return the character set, can only be {@code AL16UTF16} or {@code UTF8}.
     */
    public CharacterSet getNationalCharacterSet() {
        final String query = "select VALUE from NLS_DATABASE_PARAMETERS where PARAMETER = 'NLS_NCHAR_CHARACTERSET'";
        try {
            final String nlsCharacterSet = queryAndMap(query, rs -> {
                if (rs.next()) {
                    return rs.getString(1);
                }
                return null;
            });
            if (nlsCharacterSet != null) {
                switch (nlsCharacterSet) {
                    case "AL16UTF16":
                        return CharacterSet.make(CharacterSet.AL16UTF16_CHARSET);
                    case "UTF8":
                        return CharacterSet.make(CharacterSet.UTF8_CHARSET);
                }
            }
            throw new SQLException("An unexpected NLS_NCHAR_CHARACTERSET detected: " + nlsCharacterSet);
        }
        catch (SQLException e) {
            throw new DebeziumException("Failed to resolve Oracle's NLS_NCHAR_CHARACTERSET property", e);
        }
    }

    public void removeAllLogFilesFromLogMinerSession() throws SQLException {
        final Set<String> fileNames = queryAndMap("SELECT FILENAME AS NAME FROM V$LOGMNR_LOGS", rs -> {
            final Set<String> results = new HashSet<>();
            while (rs.next()) {
                results.add(rs.getString(1));
            }
            return results;
        });

        for (String fileName : fileNames) {
            LOGGER.debug("Removing file {} from LogMiner mining session.", fileName);
            final String sql = "BEGIN SYS.DBMS_LOGMNR.REMOVE_LOGFILE(LOGFILENAME => '" + fileName + "');END;";
            try (CallableStatement statement = connection(false).prepareCall(sql)) {
                statement.execute();
            }
        }
    }

    public RedoThreadState getRedoThreadState() throws SQLException {
        final String query = "SELECT * FROM V$THREAD";
        try {
            return queryAndMap(query, rs -> {
                RedoThreadState.Builder builder = RedoThreadState.builder();
                while (rs.next()) {
                    // While this field should never be NULL, the database metadata allows it
                    final int threadId = rs.getInt("THREAD#");
                    if (!rs.wasNull()) {
                        RedoThreadState.RedoThread.Builder threadBuilder = builder.thread()
                                .threadId(threadId)
                                .status(rs.getString("STATUS"))
                                .enabled(rs.getString("ENABLED"))
                                .logGroups(rs.getLong("GROUPS"))
                                .instanceName(rs.getString("INSTANCE"))
                                .openTime(readTimestampAsInstant(rs, "OPEN_TIME"))
                                .currentGroupNumber(rs.getLong("CURRENT_GROUP#"))
                                .currentSequenceNumber(rs.getLong("SEQUENCE#"))
                                .checkpointScn(readScnColumnAsScn(rs, "CHECKPOINT_CHANGE#"))
                                .checkpointTime(readTimestampAsInstant(rs, "CHECKPOINT_TIME"))
                                .enabledScn(readScnColumnAsScn(rs, "ENABLE_CHANGE#"))
                                .enabledTime(readTimestampAsInstant(rs, "ENABLE_TIME"))
                                .disabledScn(readScnColumnAsScn(rs, "DISABLE_CHANGE#"))
                                .disabledTime(readTimestampAsInstant(rs, "DISABLE_TIME"));
                        if (getOracleVersion().getMajor() >= 11) {
                            threadBuilder = threadBuilder.lastRedoSequenceNumber(rs.getLong("LAST_REDO_SEQUENCE#"))
                                    .lastRedoBlock(rs.getLong("LAST_REDO_BLOCK"))
                                    .lastRedoScn(readScnColumnAsScn(rs, "LAST_REDO_CHANGE#"))
                                    .lastRedoTime(readTimestampAsInstant(rs, "LAST_REDO_TIME"));
                        }
                        if (getOracleVersion().getMajor() >= 12) {
                            threadBuilder = threadBuilder.conId(rs.getLong("CON_ID"));
                        }
                        builder = threadBuilder.build();
                    }
                }
                return builder.build();
            });
        }
        catch (SQLException e) {
            throw new DebeziumException("Failed to read the Oracle database redo thread state", e);
        }
    }

    public List<String> getSQLKeywords() {
        try {
            return Arrays.asList(connection().getMetaData().getSQLKeywords().split(","));
        }
        catch (SQLException e) {
            LOGGER.debug("Failed to acquire SQL keywords from JDBC driver.", e);
            return Collections.emptyList();
        }
    }

    public boolean hasExtendedStringSupport() {
        try {
            final String value = Strings.defaultIfBlank(getDatabaseParameterValue("MAX_STRING_SIZE"), "STANDARD");
            LOGGER.info("Oracle MAX_STRING_SIZE is {}", value);
            return "EXTENDED".equals(value);
        }
        catch (Exception e) {
            LOGGER.warn("Failed to check MAX_STRING_SIZE status, defaulting to STANDARD.", e);
            return false;
        }
    }

    public String getDatabaseParameterValue(String parameterName) throws SQLException {
        final String query = "SELECT VALUE FROM V$PARAMETER WHERE UPPER(NAME) = UPPER(?)";
        return prepareQueryAndMap(query, ps -> ps.setString(1, parameterName), rs -> rs.next() ? rs.getString(1) : null);
    }

    private static Scn readScnColumnAsScn(ResultSet rs, String columnName) throws SQLException {
        final String value = rs.getString(columnName);
        return Strings.isNullOrEmpty(value) ? Scn.NULL : Scn.valueOf(value);
    }

    private static Instant readTimestampAsInstant(ResultSet rs, String columnName) throws SQLException {
        final Timestamp value = rs.getTimestamp(columnName);
        return value == null ? null : value.toInstant();
    }

    @FunctionalInterface
    interface ObjectIdentifierConsumer {
        void apply(Long objectId, Long dataObjectId);
    }
}
