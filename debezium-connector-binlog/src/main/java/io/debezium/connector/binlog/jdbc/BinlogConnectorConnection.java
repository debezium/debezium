/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.BinlogOffsetContext;
import io.debezium.connector.binlog.gtid.GtidSet;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Strings;

/**
 * An abstract binlog-based connector connection implementation of {@link JdbcConnection}.
 *
 * @author Jiri Pechanec
 * @author Randall Hauch
 * @author Chris Cranford
 */
public abstract class BinlogConnectorConnection extends JdbcConnection {

    private static final Logger LOGGER = LoggerFactory.getLogger(BinlogConnectorConnection.class);

    private static final String SQL_SHOW_SYSTEM_VARIABLES = "SHOW VARIABLES";
    private static final String SQL_SHOW_SYSTEM_VARIABLES_CHARACTER_SET = "SHOW VARIABLES WHERE Variable_name IN ('character_set_server','collation_server')";
    private static final String SQL_SHOW_SESSION_VARIABLE_SSL_VERSION = "SHOW SESSION STATUS LIKE 'Ssl_version'";
    private static final String QUOTED_CHARACTER = "`";
    public static final String MASTER_STATUS_STATEMENT = "SHOW MASTER STATUS";

    private final ConnectionConfiguration connectionConfig;
    private final BinlogFieldReader fieldReader;

    public BinlogConnectorConnection(ConnectionConfiguration configuration, BinlogFieldReader fieldReader) {
        super(configuration.config(), configuration.factory(), QUOTED_CHARACTER, QUOTED_CHARACTER);
        this.connectionConfig = configuration;
        this.fieldReader = fieldReader;
    }

    @Override
    public Object getColumnValue(ResultSet rs, int columnIndex, Column column, Table table) throws SQLException {
        return fieldReader.readField(rs, columnIndex, column, table);
    }

    @Override
    public String quotedTableIdString(TableId tableId) {
        return tableId.toQuotedString('`');
    }

    @Override
    public String getQualifiedTableName(TableId tableId) {
        return tableId.catalog() + "." + tableId.table();
    }

    @Override
    public Optional<Boolean> nullsSortLast() {
        // "any NULLs are considered to have the lowest value"
        // https://mariadb.com/kb/en/null-values/#ordering
        //
        // "NULL values are presented first if you do ORDER BY ... ASC"
        // https://dev.mysql.com/doc/refman/8.0/en/working-with-null.html
        return Optional.of(false);
    }

    public String connectionString() {
        return connectionString(connectionConfig.getUrlPattern());
    }

    public ConnectionConfiguration connectionConfig() {
        return connectionConfig;
    }

    /**
     * Determine whether the current user has the named privilege. If the user has the "ALL" privilege, this
     * method will always return {@code true}.
     *
     * @param grantName the name of the database privilege; may not be null
     * @return {@code true} if the user has the named privilege; {@code false} otherwise
     */
    public boolean userHasPrivileges(String grantName) {
        try {
            return queryAndMap("SHOW GRANTS FOR CURRENT_USER", rs -> {
                while (rs.next()) {
                    String grants = rs.getString(1);
                    LOGGER.debug(grants);
                    if (grants == null) {
                        return false;
                    }
                    grants = grants.toUpperCase();
                    if (grants.contains("ALL") || grants.contains(grantName.toUpperCase())) {
                        return true;
                    }
                }
                return false;
            });
        }
        catch (SQLException e) {
            throw new DebeziumException("Unexpected error while connecting to database and looking at privileges for current user: ", e);
        }
    }

    /**
     * Determine the earliest binlog filename that is still available in the server.
     *
     * @return the name of the earliest binlog filename, or null if there are none
     */
    public String earliestBinlogFilename() {
        // Accumulate the available binlog filenames ...
        List<String> logNames = new ArrayList<>();
        try {
            LOGGER.info("Checking all known binlogs from the database");
            query("SHOW BINARY LOGS", rs -> {
                while (rs.next()) {
                    logNames.add(rs.getString(1));
                }
            });
        }
        catch (SQLException e) {
            throw new DebeziumException("Unexpected error while connecting to the database and looking for binary logs: ", e);
        }

        if (logNames.isEmpty()) {
            return null;
        }
        return logNames.get(0);
    }

    /**
     * Query the database server and get the list of binlog files that are currently available.
     *
     * @return list of binlog files
     */
    public List<String> availableBinlogFiles() {
        List<String> logNames = new ArrayList<>();
        try {
            LOGGER.info("Get all known binlogs");
            query("SHOW BINARY LOGS", rs -> {
                while (rs.next()) {
                    logNames.add(rs.getString(1));
                }
            });
            return logNames;
        }
        catch (SQLException e) {
            throw new DebeziumException("Unexpected error while connecting to the database and looking for binary logs: ", e);
        }
    }

    /**
     * Query the available databases.
     *
     * @return list of databases
     */
    public List<String> availableDatabases() {
        final List<String> databaseNames = new ArrayList<>();
        try {
            query("SHOW DATABASES", rs -> {
                while (rs.next()) {
                    databaseNames.add(rs.getString(1));
                }
            });
            return databaseNames;
        }
        catch (SQLException e) {
            throw new DebeziumException("Unexpected error while getting available databases: ", e);
        }
    }

    /**
     * Get the estimated table size, aka number of rows.
     *
     * @param tableId the table identifier; should never be null
     * @return an optional long-value that may be empty if no data is available or an exception occurred
     */
    public OptionalLong getEstimatedTableSize(TableId tableId) {
        try {
            // Choose how we create statements based on the # of rows.
            // This is approximate and less accurate then COUNT(*),
            // but far more efficient for large InnoDB tables.
            executeWithoutCommitting("USE `" + tableId.catalog() + "`;");
            return queryAndMap("SHOW TABLE STATUS LIKE '" + tableId.table() + "';", rs -> {
                if (rs.next()) {
                    return OptionalLong.of((rs.getLong(5)));
                }
                return OptionalLong.empty();
            });
        }
        catch (SQLException e) {
            LOGGER.debug("Error while getting number of rows in table {}: {}", tableId, e.getMessage(), e);
        }
        return OptionalLong.empty();
    }

    /**
     * Read the charset-related system variables.
     *
     * @return the system variables that are related to server character sets; never null
     */
    public Map<String, String> readCharsetSystemVariables() {
        // Read the system variables from the MySQL instance and get the current database name ...
        LOGGER.debug("Reading charset-related system variables before parsing DDL history.");
        return querySystemVariables(SQL_SHOW_SYSTEM_VARIABLES_CHARACTER_SET);
    }

    /**
     * Executes a {@code SET} statement, setting each variable with it's specified value.
     *
     * @param variables key/value variable names as keys and the value(s) to be set
     * @return the constructed {@code SET} database statement; never null
     */
    public String setStatementFor(Map<String, String> variables) {
        StringBuilder sb = new StringBuilder("SET ");
        boolean first = true;
        List<String> varNames = new ArrayList<>(variables.keySet());
        Collections.sort(varNames);
        for (String varName : varNames) {
            if (first) {
                first = false;
            }
            else {
                sb.append(", ");
            }
            sb.append(varName).append("=");
            String value = variables.get(varName);
            if (value == null) {
                value = "";
            }
            if (value.contains(",") || value.contains(";")) {
                value = "'" + value + "'";
            }
            sb.append(value);
        }
        return sb.append(";").toString();
    }

    /**
     * Determines whether the binlog format used by the database server is {@code binlog_row_image='FULL'}.
     *
     * @return {@code true} if the {@code binlog_row_image} is set to {@code FULL}, {@code false} otherwise
     */
    public boolean isBinlogRowImageFull() {
        try {
            final String rowImage = queryAndMap("SHOW GLOBAL VARIABLES LIKE 'binlog_row_image'", rs -> {
                if (rs.next()) {
                    return rs.getString(2);
                }
                // This setting was introduced in MySQL 5.6+ with default of 'FULL'.
                // For older versions, assume 'FULL'.
                return "FULL";
            });
            LOGGER.debug("binlog_row_image={}", rowImage);
            return "FULL".equalsIgnoreCase(rowImage);
        }
        catch (SQLException e) {
            throw new DebeziumException("Unexpected error while connecting to the database and looking at BINLOG_ROW_IMAGE mode: ", e);
        }
    }

    /**
     * Determine whether the database server has the row-level binlog enabled.
     *
     * @return {@code true} if the server's {@code binlog_format} is set to {@code ROW}, {@code false} otherwise
     */
    public boolean isBinlogFormatRow() {
        try {
            final String mode = queryAndMap("SHOW GLOBAL VARIABLES LIKE 'binlog_format'", rs -> rs.next() ? rs.getString(2) : "");
            LOGGER.debug("binlog_format={}", mode);
            return "ROW".equalsIgnoreCase(mode);
        }
        catch (SQLException e) {
            throw new DebeziumException("Unexpected error while connecting to the database and looking at BINLOG_FORMAT mode: ", e);
        }
    }

    /**
     * Read the database server's default character sets for existing databases.
     *
     * @return the map of database names and their default character sets; never null
     */
    public Map<String, DatabaseLocales> readDatabaseCollations() {
        LOGGER.debug("Reading default database charsets");
        try {
            return queryAndMap("SELECT schema_name, default_character_set_name, default_collation_name FROM information_schema.schemata", rs -> {
                final Map<String, DatabaseLocales> charsets = new HashMap<>();
                while (rs.next()) {
                    String databaseName = rs.getString(1);
                    String characterSet = rs.getString(2);
                    String collationName = rs.getString(3);
                    if (databaseName != null && (characterSet != null || collationName != null)) {
                        charsets.put(databaseName, new DatabaseLocales(characterSet, collationName));
                        LOGGER.debug("\t{} = {}, {}",
                                Strings.pad(databaseName, 45, ' '),
                                Strings.pad(characterSet, 45, ' '),
                                Strings.pad(collationName, 45, ' '));
                    }
                }
                return charsets;
            });
        }
        catch (SQLException e) {
            throw new DebeziumException("Error reading default database charsets: " + e.getMessage(), e);
        }
    }

    /**
     * Return whether the table identifiers are case-sensitive.
     *
     * @return {@code true} if the table identifiers are case-sensitive, {@code false} otherwise
     */
    public boolean isTableIdCaseSensitive() {
        return !"0".equals(readSystemVariables().get(BinlogSystemVariables.LOWER_CASE_TABLE_NAMES));
    }

    /**
     * Determine whether the binlog position as set in the offset details is available on the server.
     *
     * @param config the connector configuration; should not be null
     * @param gtid the GTID from the connector offsets; may be null
     * @param binlogFileName the binlog file name from the connector offsets; may be null
     * @return {@code true} if the binlog position is available, {@code false} otherwise
     */
    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public boolean isBinlogPositionAvailable(BinlogConnectorConfig config, String gtid, String binlogFileName) {
        if (gtid != null) {
            if (gtid.trim().isEmpty()) {
                // Start at the beginning
                return true;
            }

            GtidSet availableGtidSet = knownGtidSet();
            if (availableGtidSet.isEmpty()) {
                // Last offsets had GTIDs but the server does not use them
                LOGGER.info("Connector used GTIDs previously, but server does not know of any GTIDs or they are not enabled");
                return false;
            }

            // GTIDs are enabled, used previously, retain only the ranges allowed
            GtidSet gtidSet = config.getGtidSetFactory().createGtidSet(gtid).retainAll(config.getGtidSourceFilter());
            LOGGER.info("GTID Set retained: '{}'", gtidSet);

            // Get the GTID set that is available on the server
            if (gtidSet.isContainedWithin(availableGtidSet)) {
                LOGGER.info("The current GTID set '{}' does not contain the GTID set '{}' required by the connector",
                        availableGtidSet, gtidSet);

                final GtidSet knownServerSet = availableGtidSet.retainAll(config.getGtidSourceFilter());
                final GtidSet gtidSetToReplicate = subtractGtidSet(knownServerSet, gtidSet);
                final GtidSet purgedGtidSet = purgedGtidSet();
                LOGGER.info("Server has already purged '{}' GTIDs", purgedGtidSet);

                final GtidSet nonPurgedGtidSetTemplate = subtractGtidSet(gtidSetToReplicate, purgedGtidSet);
                LOGGER.info("GTIDs known by the server but not processed yet '{}', for replication are available only '{}'",
                        gtidSetToReplicate, nonPurgedGtidSetTemplate);

                if (!gtidSetToReplicate.equals(nonPurgedGtidSetTemplate)) {
                    LOGGER.info("Some of the GTIDs needed to replicate have been already purged");
                    return false;
                }
                return true;
            }

            LOGGER.info("Connector last known GTIDs are '{}', but server has '{}'", gtidSet, availableGtidSet);
            return false;
        }

        if (Strings.isNullOrBlank(binlogFileName)) {
            // Start at the current position
            return true;
        }

        // Accumulate the available binlog filenames, and compare with the one we're supposed to use
        List<String> logNames = availableBinlogFiles();
        boolean found = logNames.stream().anyMatch(binlogFileName::equals);
        if (!found && LOGGER.isInfoEnabled()) {
            LOGGER.info("Connector requires binlog file '{}', but server only has {}", binlogFileName, String.join(", ", logNames));
        }
        else if (found && LOGGER.isInfoEnabled()) {
            LOGGER.info("Server has the binlog file '{}' required by the connector", binlogFileName);
        }

        return found;
    }

    /**
     * Read the SSL version session variable.
     *
     * @return the session variable value related to the session SSL version
     */
    public String getSessionVariableForSslVersion() {
        final String SSL_VERSION = "Ssl_version";
        LOGGER.debug("Reading session variable for Ssl Version");
        Map<String, String> sessionVariables = querySystemVariables(SQL_SHOW_SESSION_VARIABLE_SSL_VERSION);
        if (!sessionVariables.isEmpty() && sessionVariables.containsKey(SSL_VERSION)) {
            return sessionVariables.get(SSL_VERSION);
        }
        return null;
    }

    public boolean validateLogPosition(Partition partition, OffsetContext offset, CommonConnectorConfig config) {
        final String gtidSet = ((BinlogOffsetContext) offset).gtidSet();
        final String binlogFilename = ((BinlogOffsetContext) offset).getSource().binlogFilename();
        return isBinlogPositionAvailable((BinlogConnectorConfig) config, gtidSet, binlogFilename);
    }

    public String binaryLogStatusStatement() {
        return MASTER_STATUS_STATEMENT;
    }

    /**
     * Determine whether the server has enabled GTID support.
     *
     * @return {@code false} if the server has not enabled GTIDs, {@code true} otherwise
     */
    public abstract boolean isGtidModeEnabled();

    /**
     * Returns the most recent executed GTID set or position.
     *
     * @return the string representation of the most recent executed GTID set or position; never null but
     *         will be empty if the server does not support or has not processed any GTID
     */
    public abstract GtidSet knownGtidSet();

    /**
     * Determines the difference between two GTID sets.
     *
     * @param set1 the first set; should never be null
     * @param set2 the second set; should never be null
     * @return the subtraction of the two sets in a new GtidSet instance; never null
     */
    public abstract GtidSet subtractGtidSet(GtidSet set1, GtidSet set2);

    /**
     * Get the purged GTID values from the server.
     *
     * @return A GTID set; may be empty of GTID support is not enabled or if none have been purged
     */
    public abstract GtidSet purgedGtidSet();

    /**
     * Apply the include/exclude GTID source filters to the current offset's GTID set and merge them onto the
     * currently available GTID set from the database server.<p></p>
     *
     * The merging behavior of this method might seem a bit strange at first. It's required in order for Debezium
     * to consume a binlog that has multi-source replication enabled, if a fail-over has to occur. In such a case,
     * the server thta Debezium is failing over to might have a different set of sources, but still include the
     * sources required for Debezium to continue to function. The database does not allow downstream replicas to
     * connect if the GTID set does not contain GTIDs for all channels that the server is replicating from, even
     * if the server does not have the data needed by the client.<p></p>
     *
     * To get around this, we can have Debezium merge the GTID set with whatever is on the server, so that the
     * database will allow it to connect. See <a href="https://issues.redhat.com/browse/DBZ-143">DBZ-143</a>.<p></p>
     *
     * This method does not mutate any state in the context.
     *
     * @param gtidSourceFilter the source filter
     * @param offsetGtids the gtids from the offsets
     * @param availableServerGtidSet the GTID set currently available in the server
     * @param purgedServerGtidSet the GTID set already purged by the server
     * @return A GTID set meant for consuming from a binlog; may return null if the SourceInfo has no GTIDs and none filtered
     */
    public abstract GtidSet filterGtidSet(Predicate<String> gtidSourceFilter,
                                          String offsetGtids,
                                          GtidSet availableServerGtidSet,
                                          GtidSet purgedServerGtidSet);

    /**
     * Read the system variables.
     *
     * @return all the system variables; never null
     */
    protected Map<String, String> readSystemVariables() {
        // Read the system variables from the MySQL instance and get the current database name ...
        LOGGER.debug("Reading system variables");
        return querySystemVariables(SQL_SHOW_SYSTEM_VARIABLES);
    }

    private Map<String, String> querySystemVariables(String statement) {
        final Map<String, String> variables = new HashMap<>();
        try {
            query(statement, rs -> {
                while (rs.next()) {
                    String varName = rs.getString(1);
                    String value = rs.getString(2);
                    if (varName != null && value != null) {
                        variables.put(varName, value);
                        LOGGER.debug("\t{} = {}", Strings.pad(varName, 45, ' '), Strings.pad(value, 45, ' '));
                    }
                }
            });
        }
        catch (SQLException e) {
            throw new DebeziumException("Error reading MySQL variables: " + e.getMessage(), e);
        }
        return variables;
    }

    public static class DatabaseLocales {
        private final String charset;
        private final String collation;

        public DatabaseLocales(String charset, String collation) {
            this.charset = charset;
            this.collation = collation;
        }

        public void appendToDdlStatement(String dbName, StringBuilder ddl) {
            if (charset != null) {
                LOGGER.debug("Setting default charset '{}' for database '{}'", charset, dbName);
                ddl.append(" CHARSET ").append(charset);
            }
            else {
                LOGGER.info("Default database charset for '{}' not found", dbName);
            }
            if (collation != null) {
                LOGGER.debug("Setting default collation '{}' for database '{}'", collation, dbName);
                ddl.append(" COLLATE ").append(collation);
            }
            else {
                LOGGER.info("Default database collation for '{}' not found", dbName);
            }
        }
    }

}
