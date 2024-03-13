/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.connect.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.Field;
import io.debezium.connector.oracle.OracleConnectorConfig.ConnectorAdapter;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.Tables.ColumnNameFilter;
import io.debezium.util.Strings;

import oracle.jdbc.OracleTypes;

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

    /**
     * the character set encoding and time zone of the oracle database
     */
    private final OracleCharacterSetAndTimezone characterSetAndTimezone;

    private static final String QUOTED_CHARACTER = "\"";

    public OracleConnection(JdbcConfiguration config) {
        this(config, true);
    }

    public OracleConnection(JdbcConfiguration config, ConnectionFactory connectionFactory) {
        this(config, connectionFactory, true);
    }

    public OracleConnection(JdbcConfiguration config, ConnectionFactory connectionFactory, boolean showVersion) {
        super(config, connectionFactory, QUOTED_CHARACTER, QUOTED_CHARACTER);
        this.databaseVersion = resolveOracleDatabaseVersion();
        if (showVersion) {
            LOGGER.info("Database Version: {}", databaseVersion.getBanner());
        }

        this.characterSetAndTimezone = resolveOracleCharacterSetAndTimezone();
    }

    public OracleConnection(JdbcConfiguration config, boolean showVersion) {
        super(config, resolveConnectionFactory(config), QUOTED_CHARACTER, QUOTED_CHARACTER);
        this.databaseVersion = resolveOracleDatabaseVersion();
        if (showVersion) {
            LOGGER.info("Database Version: {}", databaseVersion.getBanner());
        }

        this.characterSetAndTimezone = resolveOracleCharacterSetAndTimezone();
    }

    public void setSessionToPdb(String pdbName) {
        Statement statement = null;

        try {
            statement = connection().createStatement();
            statement.execute("alter session set container=" + pdbName);
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

    public void resetSessionToCdb() {
        Statement statement = null;

        try {
            statement = connection().createStatement();
            statement.execute("alter session set container=cdb$root");
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

    public OracleCharacterSetAndTimezone getCharacterSetAndTimezone() {
        return characterSetAndTimezone;
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

    /**
     * Obtain the character set of the Oracle database
     */
    private OracleCharacterSetAndTimezone resolveOracleCharacterSetAndTimezone() {
        String characterSet;
        try {
            characterSet = queryAndMap("SELECT VALUE FROM nls_database_parameters WHERE PARAMETER = 'NLS_CHARACTERSET'", (rs) -> {
                if (rs.next()) {
                    return rs.getString(1);
                }
                return null;
            });
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to resolve Oracle database character set", e);
        }

        try {
            oracle.jdbc.OracleConnection conn = (oracle.jdbc.OracleConnection) connection();
            String sessionTimeZoneOffset = conn.getSessionTimeZoneOffset();
            String databaseTimeZone = conn.physicalConnectionWithin().getDatabaseTimeZone();

            return new OracleCharacterSetAndTimezone(characterSet, databaseTimeZone, sessionTimeZoneOffset);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to resolve Oracle database time zone", e);
        }
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
        final String pdbName = config().getString("pdb.name");
        return (!Strings.isNullOrEmpty(pdbName) ? pdbName : config().getString("dbname")).toUpperCase();
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
            final String tableType = "SELECT COUNT(1) FROM ALL_ALL_TABLES WHERE OWNER='" + tableId.schema()
                    + "' AND TABLE_NAME='" + tableId.table() + "' AND TABLE_TYPE IS NULL";
            if (queryAndMap(tableType, rs -> rs.next() ? rs.getInt(1) : 0) == 0) {
                throw new NonRelationalTableException("Table " + tableId + " is not a relational table");
            }

            // The storage and segment attributes aren't necessary
            executeWithoutCommitting("begin dbms_metadata.set_transform_param(DBMS_METADATA.SESSION_TRANSFORM, 'STORAGE', false); end;");
            executeWithoutCommitting("begin dbms_metadata.set_transform_param(DBMS_METADATA.SESSION_TRANSFORM, 'SEGMENT_ATTRIBUTES', false); end;");
            // In case DDL is returned as multiple DDL statements, this allows the parser to parse each separately.
            // This is only critical during streaming as during snapshot the table structure is built from JDBC driver queries.
            executeWithoutCommitting("begin dbms_metadata.set_transform_param(DBMS_METADATA.SESSION_TRANSFORM, 'SQLTERMINATOR', true); end;");
            return queryAndMap("SELECT dbms_metadata.get_ddl('TABLE','" + tableId.table() + "','" + tableId.schema() + "') FROM DUAL", rs -> {
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
     * @param tableName table name, should not be {@code null}
     * @return true if the table exists, false if it does not
     * @throws SQLException if a database exception occurred
     */
    public boolean isTableExists(String tableName) throws SQLException {
        return queryAndMap("SELECT COUNT(1) FROM USER_TABLES WHERE TABLE_NAME = '" + tableName + "'",
                rs -> rs.next() && rs.getLong(1) > 0);
    }

    public boolean isTableExists(TableId tableId) throws SQLException {
        return queryAndMap("SELECT COUNT(1) FROM ALL_TABLES WHERE OWNER = '" + tableId.schema() + "' AND TABLE_NAME = '" + tableId.table() + "'",
                rs -> rs.next() && rs.getLong(1) > 0);
    }

    /**
     * Returns whether the given table is empty or not.
     *
     * @param tableName table name, should not be {@code null}
     * @return true if the table has no records, false otherwise
     * @throws SQLException if a database exception occurred
     */
    public boolean isTableEmpty(String tableName) throws SQLException {
        return getRowCount(tableName) == 0L;
    }

    /**
     * Returns the number of rows in a given table.
     *
     * @param tableName table name, should not be {@code null}
     * @return the number of rows
     * @throws SQLException if a database exception occurred
     */
    public long getRowCount(String tableName) throws SQLException {
        return queryAndMap("SELECT COUNT(1) FROM " + tableName, rs -> {
            if (rs.next()) {
                return rs.getLong(1);
            }
            return 0L;
        });
    }

    public <T> T singleOptionalValue(String query, ResultSetExtractor<T> extractor) throws SQLException {
        return queryAndMap(query, rs -> rs.next() ? extractor.apply(rs) : null);
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
    protected Optional<ColumnEditor> readTableColumn(ResultSet columnMetadata, TableId tableId, ColumnNameFilter columnFilter) throws SQLException {
        Optional<ColumnEditor> columnEditor = super.readTableColumn(columnMetadata, tableId, columnFilter);
        if (columnEditor.isPresent()) {
            // The segment position is the position of the column that is actually recorded in the sql_redo of the redo log
            columnEditor.get().segmentPosition(columnMetadata.getInt(25));
        }
        return columnEditor;
    }

    @Override
    public ResultSet getTables(DatabaseMetaData metaData, String catalog, String schemaPattern) throws SQLException {
        String querySql = "SELECT NULL AS table_cat,\n" +
                "       o.owner AS table_schem,\n" +
                "       o.object_name AS table_name,\n" +
                "       o.object_type AS table_type,\n" +
                "       NULL AS remarks,\n" +
                "       o.object_id\n" +
                "  FROM all_objects o\n" +
                "  WHERE o.owner LIKE :1 ESCAPE '/'\n" +
                "    AND o.object_type IN ('xxx', 'TABLE', 'VIEW')\n" +
                "  ORDER BY table_type, table_schem, table_name";
        return queryResult(metaData.getConnection(), querySql, schemaPattern);
    }

    @Override
    public Long getObjectId(ResultSet rs) throws SQLException {
        return rs.getLong(6);
    }

    @Override
    public ResultSet getColumns(DatabaseMetaData metaData, String catalog, String schemaPattern, String table) throws SQLException {
        oracle.jdbc.internal.OracleConnection conn = (oracle.jdbc.internal.OracleConnection)metaData.getConnection();
        short versionNumber = conn.getVersionNumber();
        String querySql = "SELECT\n" +
                "  NULL AS table_cat,\n" +
                "  t.owner AS table_schem,\n" +
                "  t.table_name AS table_name,\n" +
                "  t.column_name AS column_name,\n" +
                "  DECODE(\n" +
                "    substr(t.data_type, 1, 9), \n" +
                "    'TIMESTAMP', \n" +
                "    DECODE(\n" +
                "      substr(t.data_type, 10, 1),\n" +
                "      '(', \n" +
                "      DECODE(substr(t.data_type, 19, 5), 'LOCAL', -102, 'TIME ', -101, 93),\n" +
                "      DECODE(substr(t.data_type, 16, 5), \n" +
                "      'LOCAL', -102, 'TIME ', -101, 93)\n" +
                "    ), \n" +
                "    'INTERVAL ', \n" +
                "    DECODE(substr(t.data_type, 10, 3), 'DAY', -104, 'YEA', -103), \n" +
                "    DECODE(\n" +
                "      t.data_type, \n" +
                "      'BINARY_DOUBLE', 101, \n" +
                "      'BINARY_FLOAT', 100, \n" +
                "      'BFILE', -13, \n" +
                "      'BLOB', 2004, \n" +
                "      'CHAR', 1, \n" +
                "      'CLOB', 2005, \n" +
                "      'COLLECTION', 2003, \n" +
                "      'DATE', 93, \n" +
                "      'FLOAT', 6, \n" +
                "      'LONG', -1, \n" +
                "      'LONG RAW', -4, \n" +
                "      'NCHAR', -15, \n" +
                "      'NCLOB', 2011, \n" +
                "      'NUMBER', 2, \n" +
                "      'NVARCHAR', -9, \n" +
                "      'NVARCHAR2', -9, \n" +
                "      'OBJECT', 2002, \n" +
                "      'OPAQUE/XMLTYPE', 2009, \n" +
                "      'RAW', -3, \n" +
                "      'REF', 2006, \n" +
                "      'ROWID', -8, \n" +
                "      'SQLXML', 2009, \n" +
                "      'UROWID', -8, \n" +
                "      'VARCHAR2', 12, \n" +
                "      'VARRAY', 2003, \n" +
                "      'XMLTYPE', 2009, \n" +
                "      DECODE((SELECT a.typecode FROM ALL_TYPES a WHERE a.type_name = t.data_type AND ((a.owner IS NULL AND t.data_type_owner IS NULL) OR (a.owner = t.data_type_owner))),'OBJECT', 2002, 'COLLECTION', 2003, 1111)\n" +
                "    )\n" +
                "  ) AS data_type,\n" +
                "  t.data_type AS type_name,\n" +
                "  DECODE (t.data_precision, NULL,\n" +
                "    DECODE(t.data_type,\n" +
                "      'NUMBER', DECODE(t.data_scale, NULL, 0, 38),\n" +
                "      DECODE(t.data_type,\n" +
                "        'CHAR', t.char_length,\n" +
                "        'VARCHAR', t.char_length,\n" +
                "        'VARCHAR2', t.char_length,\n" +
                "        'NVARCHAR2', t.char_length,\n" +
                "        'NCHAR', t.char_length,\n" +
                "        'NUMBER', 0,\n" +
                "        t.data_length)),\n" +
                "    t.data_precision\n" +
                "  ) AS column_size,\n" +
                "  0 AS buffer_length,\n" +
                "  DECODE (t.data_type, 'NUMBER', DECODE(t.data_precision, NULL, DECODE(t.data_scale, NULL, -127 , t.data_scale), t.data_scale), t.data_scale) AS decimal_digits,\n" +
                "  10 AS num_prec_radix,\n" +
                "  DECODE (t.nullable, 'N', 0, 1) AS nullable,\n" +
                "  NULL AS remarks,\n" +
                "  t.data_default AS column_def,\n" +
                "  0 AS sql_data_type,\n" +
                "  0 AS sql_datetime_sub,\n" +
                "  t.data_length AS char_octet_length,\n" +
                "  t.column_id AS ordinal_position,\n" +
                "  DECODE (t.nullable, 'N', 'NO', 'YES') AS is_nullable,\n" +
                "  NULL AS SCOPE_CATALOG,\n" +
                "  NULL AS SCOPE_SCHEMA,\n" +
                "  NULL AS SCOPE_TABLE,\n" +
                "  NULL AS SOURCE_DATA_TYPE,\n" +
                "  'NO' AS IS_AUTOINCREMENT,\n" +
                "  t.virtual_column AS IS_GENERATEDCOLUMN,\n" +
                "  t.segment_column_id AS segment_position\n" +
                "FROM all_tab_cols t\n" +
                "WHERE\n" +
                "\tt.owner LIKE :1 ESCAPE '/'\n" +
                "\tAND t.table_name LIKE :2 ESCAPE '/'\n";
        if (versionNumber >= 12000) {
            querySql += "\tAND t.user_generated = 'YES'\n";
        }
        querySql += "ORDER BY table_schem, table_name, segment_position";

        return queryResult(conn, querySql, schemaPattern, table);
    }

    private ResultSet queryResult(Connection conn, String querySql, String... parameters) throws SQLException {
        PreparedStatement ps = conn.prepareStatement(querySql);
        if (parameters != null && parameters.length > 0) {
            for (int i = 0; i < parameters.length; i++) {
                ps.setString(i+1, null != parameters[i] ? parameters[i] : "%");
            }
        }
        ps.closeOnCompletion();
        return ps.executeQuery();
    }

    @Override
    public Integer getSegmentPosition(ResultSet rs) throws SQLException {
        return rs.getInt(25);
    }
}
