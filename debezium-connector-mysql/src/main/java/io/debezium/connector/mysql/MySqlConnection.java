/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql;

import static io.debezium.config.CommonConnectorConfig.DATABASE_CONFIG_PREFIX;
import static io.debezium.config.CommonConnectorConfig.DRIVER_CONFIG_PREFIX;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mysql.cj.CharsetMapping;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.CommonConnectorConfig.EventProcessingFailureHandlingMode;
import io.debezium.config.Configuration;
import io.debezium.config.Configuration.Builder;
import io.debezium.config.Field;
import io.debezium.connector.mysql.MySqlConnectorConfig.SecureConnectionMode;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Strings;

/**
 * {@link JdbcConnection} extension to be used with MySQL Server
 *
 * @author Jiri Pechanec, Randall Hauch
 *
 */
public class MySqlConnection extends JdbcConnection {

    private static Logger LOGGER = LoggerFactory.getLogger(MySqlConnection.class);

    private static final String SQL_SHOW_SYSTEM_VARIABLES = "SHOW VARIABLES";
    private static final String SQL_SHOW_SYSTEM_VARIABLES_CHARACTER_SET = "SHOW VARIABLES WHERE Variable_name IN ('character_set_server','collation_server')";
    private static final String SQL_SHOW_SESSION_VARIABLE_SSL_VERSION = "SHOW SESSION STATUS LIKE 'Ssl_version'";
    private static final String QUOTED_CHARACTER = "`";

    protected static final String URL_PATTERN = "jdbc:mysql://${hostname}:${port}/?useInformationSchema=true&nullCatalogMeansCurrent=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&connectTimeout=${connectTimeout}";

    private final Map<String, String> originalSystemProperties = new HashMap<>();
    private final MySqlConnectionConfiguration connectionConfig;
    private final MySqlFieldReader mysqlFieldReader;

    /**
     * Creates a new connection using the supplied configuration.
     *
     * @param connectionConfig {@link MySqlConnectionConfiguration} instance, may not be null.
     * @param fieldReader binary or text protocol based readers
     */
    public MySqlConnection(MySqlConnectionConfiguration connectionConfig, MySqlFieldReader fieldReader) {
        super(connectionConfig.jdbcConfig, connectionConfig.factory(), QUOTED_CHARACTER, QUOTED_CHARACTER);
        this.connectionConfig = connectionConfig;
        this.mysqlFieldReader = fieldReader;
    }

    /**
     * Creates a new connection using the supplied configuration.
     *
     * @param connectionConfig {@link MySqlConnectionConfiguration} instance, may not be null.
     */
    public MySqlConnection(MySqlConnectionConfiguration connectionConfig) {
        this(connectionConfig, new MySqlTextProtocolFieldReader(null));
    }

    @Override
    public void close() throws SQLException {
        try {
            super.close();
        }
        finally {
            // Reset the system properties to their original value ...
            originalSystemProperties.forEach((name, value) -> {
                if (value != null) {
                    System.setProperty(name, value);
                }
                else {
                    System.clearProperty(name);
                }
            });
        }
    }

    /**
     * Read the MySQL charset-related system variables.
     *
     * @return the system variables that are related to server character sets; never null
     */
    protected Map<String, String> readMySqlCharsetSystemVariables() {
        // Read the system variables from the MySQL instance and get the current database name ...
        LOGGER.debug("Reading MySQL charset-related system variables before parsing DDL history.");
        return querySystemVariables(SQL_SHOW_SYSTEM_VARIABLES_CHARACTER_SET);
    }

    /**
     * Read the MySQL system variables.
     *
     * @return the system variables that are related to server character sets; never null
     */
    protected Map<String, String> readMySqlSystemVariables() {
        // Read the system variables from the MySQL instance and get the current database name ...
        LOGGER.debug("Reading MySQL system variables");
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
                        LOGGER.debug("\t{} = {}",
                                Strings.pad(varName, 45, ' '),
                                Strings.pad(value, 45, ' '));
                    }
                }
            });
        }
        catch (SQLException e) {
            throw new DebeziumException("Error reading MySQL variables: " + e.getMessage(), e);
        }

        return variables;
    }

    protected String setStatementFor(Map<String, String> variables) {
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

    protected void setSystemProperty(String property, Field field, boolean showValueInError) {
        String value = connectionConfig.originalConfig().getString(field);
        if (value != null) {
            value = value.trim();
            String existingValue = System.getProperty(property);
            if (existingValue == null) {
                // There was no existing property ...
                String existing = System.setProperty(property, value);
                originalSystemProperties.put(property, existing); // the existing value may be null
            }
            else {
                existingValue = existingValue.trim();
                if (!existingValue.equalsIgnoreCase(value)) {
                    // There was an existing property, and the value is different ...
                    String msg = "System or JVM property '" + property + "' is already defined, but the configuration property '"
                            + field.name()
                            + "' defines a different value";
                    if (showValueInError) {
                        msg = "System or JVM property '" + property + "' is already defined as " + existingValue
                                + ", but the configuration property '" + field.name() + "' defines a different value '" + value + "'";
                    }
                    throw new DebeziumException(msg);
                }
                // Otherwise, there was an existing property, and the value is exactly the same (so do nothing!)
            }
        }
    }

    /**
     * Read the Ssl Version session variable.
     *
     * @return the session variables that are related to sessions ssl version
     */
    protected String getSessionVariableForSslVersion() {
        final String SSL_VERSION = "Ssl_version";
        LOGGER.debug("Reading MySQL Session variable for Ssl Version");
        Map<String, String> sessionVariables = querySystemVariables(SQL_SHOW_SESSION_VARIABLE_SSL_VERSION);
        if (!sessionVariables.isEmpty() && sessionVariables.containsKey(SSL_VERSION)) {
            return sessionVariables.get(SSL_VERSION);
        }
        return null;
    }

    /**
     * Determine whether the MySQL server has GTIDs enabled.
     *
     * @return {@code false} if the server's {@code gtid_mode} is set and is {@code OFF}, or {@code true} otherwise
     */
    public boolean isGtidModeEnabled() {
        try {
            return queryAndMap("SHOW GLOBAL VARIABLES LIKE 'GTID_MODE'", rs -> {
                if (rs.next()) {
                    return "ON".equalsIgnoreCase(rs.getString(2));
                }
                return false;
            });
        }
        catch (SQLException e) {
            throw new DebeziumException("Unexpected error while connecting to MySQL and looking at GTID mode: ", e);
        }
    }

    /**
     * Determine the executed GTID set for MySQL.
     *
     * @return the string representation of MySQL's GTID sets; never null but an empty string if the server does not use GTIDs
     */
    public String knownGtidSet() {
        try {
            return queryAndMap("SHOW MASTER STATUS", rs -> {
                if (rs.next() && rs.getMetaData().getColumnCount() > 4) {
                    return rs.getString(5); // GTID set, may be null, blank, or contain a GTID set
                }
                return "";
            });
        }
        catch (SQLException e) {
            throw new DebeziumException("Unexpected error while connecting to MySQL and looking at GTID mode: ", e);
        }
    }

    /**
     * Determine the difference between two sets.
     *
     * @return a subtraction of two GTID sets; never null
     */
    public GtidSet subtractGtidSet(GtidSet set1, GtidSet set2) {
        try {
            return prepareQueryAndMap("SELECT GTID_SUBTRACT(?, ?)",
                    ps -> {
                        ps.setString(1, set1.toString());
                        ps.setString(2, set2.toString());
                    },
                    rs -> {
                        if (rs.next()) {
                            return new GtidSet(rs.getString(1));
                        }
                        return new GtidSet("");
                    });
        }
        catch (SQLException e) {
            throw new DebeziumException("Unexpected error while connecting to MySQL and looking at GTID mode: ", e);
        }
    }

    /**
     * Get the purged GTID values from MySQL (gtid_purged value)
     *
     * @return A GTID set; may be empty if not using GTIDs or none have been purged yet
     */
    public GtidSet purgedGtidSet() {
        try {
            return queryAndMap("SELECT @@global.gtid_purged", rs -> {
                if (rs.next() && rs.getMetaData().getColumnCount() > 0) {
                    return new GtidSet(rs.getString(1)); // GTID set, may be null, blank, or contain a GTID set
                }
                return new GtidSet("");
            });
        }
        catch (SQLException e) {
            throw new DebeziumException("Unexpected error while connecting to MySQL and looking at gtid_purged variable: ", e);
        }
    }

    /**
     * Determine if the current user has the named privilege. Note that if the user has the "ALL" privilege this method
     * returns {@code true}.
     *
     * @param grantName the name of the MySQL privilege; may not be null
     * @return {@code true} if the user has the named privilege, or {@code false} otherwise
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
            throw new DebeziumException("Unexpected error while connecting to MySQL and looking at privileges for current user: ", e);
        }
    }

    /**
     * Determine the earliest binlog filename that is still available in the server.
     *
     * @return the name of the earliest binlog filename, or null if there are none.
     */
    public String earliestBinlogFilename() {
        // Accumulate the available binlog filenames ...
        List<String> logNames = new ArrayList<>();
        try {
            LOGGER.info("Checking all known binlogs from MySQL");
            query("SHOW BINARY LOGS", rs -> {
                while (rs.next()) {
                    logNames.add(rs.getString(1));
                }
            });
        }
        catch (SQLException e) {
            throw new DebeziumException("Unexpected error while connecting to MySQL and looking for binary logs: ", e);
        }

        if (logNames.isEmpty()) {
            return null;
        }
        return logNames.get(0);
    }

    /**
     * Determine whether the MySQL server has the binlog_row_image set to 'FULL'.
     *
     * @return {@code true} if the server's {@code binlog_row_image} is set to {@code FULL}, or {@code false} otherwise
     */
    protected boolean isBinlogRowImageFull() {
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
            throw new DebeziumException("Unexpected error while connecting to MySQL and looking at BINLOG_ROW_IMAGE mode: ", e);
        }
    }

    /**
     * Determine whether the MySQL server has the row-level binlog enabled.
     *
     * @return {@code true} if the server's {@code binlog_format} is set to {@code ROW}, or {@code false} otherwise
     */
    protected boolean isBinlogFormatRow() {
        try {
            final String mode = queryAndMap("SHOW GLOBAL VARIABLES LIKE 'binlog_format'", rs -> rs.next() ? rs.getString(2) : "");
            LOGGER.debug("binlog_format={}", mode);
            return "ROW".equalsIgnoreCase(mode);
        }
        catch (SQLException e) {
            throw new DebeziumException("Unexpected error while connecting to MySQL and looking at BINLOG_FORMAT mode: ", e);
        }
    }

    /**
     * Query the database server to get the list of the binlog files availble.
     *
     * @return list of the binlog files
     */
    public List<String> availableBinlogFiles() {
        List<String> logNames = new ArrayList<>();
        try {
            LOGGER.info("Get all known binlogs from MySQL");
            query("SHOW BINARY LOGS", rs -> {
                while (rs.next()) {
                    logNames.add(rs.getString(1));
                }
            });
            return logNames;
        }
        catch (SQLException e) {
            throw new DebeziumException("Unexpected error while connecting to MySQL and looking for binary logs: ", e);
        }
    }

    public OptionalLong getEstimatedTableSize(TableId tableId) {
        try {
            // Choose how we create statements based on the # of rows.
            // This is approximate and less accurate then COUNT(*),
            // but far more efficient for large InnoDB tables.
            execute("USE `" + tableId.catalog() + "`;");
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

    public boolean isTableIdCaseSensitive() {
        return !"0".equals(readMySqlSystemVariables().get(MySqlSystemVariables.LOWER_CASE_TABLE_NAMES));
    }

    /**
     * Read the MySQL default character sets for exisiting databases.
     *
     * @return the map of database names with their default character sets; never null
     */
    protected Map<String, DatabaseLocales> readDatabaseCollations() {
        LOGGER.debug("Reading default database charsets");
        try {
            return queryAndMap("SELECT schema_name, default_character_set_name, default_collation_name FROM information_schema.schemata", rs -> {
                final Map<String, DatabaseLocales> charsets = new HashMap<>();
                while (rs.next()) {
                    String dbName = rs.getString(1);
                    String charset = rs.getString(2);
                    String collation = rs.getString(3);
                    if (dbName != null && (charset != null || collation != null)) {
                        charsets.put(dbName, new DatabaseLocales(charset, collation));
                        LOGGER.debug("\t{} = {}, {}",
                                Strings.pad(dbName, 45, ' '),
                                Strings.pad(charset, 45, ' '),
                                Strings.pad(collation, 45, ' '));
                    }
                }
                return charsets;
            });
        }
        catch (SQLException e) {
            throw new DebeziumException("Error reading default database charsets: " + e.getMessage(), e);
        }
    }

    public MySqlConnectionConfiguration connectionConfig() {
        return connectionConfig;
    }

    public String connectionString() {
        return connectionString(URL_PATTERN);
    }

    public static String getJavaEncodingForMysqlCharSet(String mysqlCharsetName) {
        return CharsetMappingWrapper.getJavaEncodingForMysqlCharSet(mysqlCharsetName);
    }

    /**
     * Helper to gain access to protected method
     */
    private final static class CharsetMappingWrapper extends CharsetMapping {
        static String getJavaEncodingForMysqlCharSet(String mySqlCharsetName) {
            return CharsetMapping.getStaticJavaEncodingForMysqlCharset(mySqlCharsetName);
        }
    }

    public static class MySqlConnectionConfiguration {

        protected static final String JDBC_PROPERTY_CONNECTION_TIME_ZONE = "connectionTimeZone";

        private final JdbcConfiguration jdbcConfig;
        private final ConnectionFactory factory;
        private final Configuration config;

        public MySqlConnectionConfiguration(Configuration config) {
            // Set up the JDBC connection without actually connecting, with extra MySQL-specific properties
            // to give us better JDBC database metadata behavior, including using UTF-8 for the client-side character encoding
            // per https://dev.mysql.com/doc/connector-j/5.1/en/connector-j-reference-charsets.html
            this.config = config;
            final boolean useSSL = sslModeEnabled();
            final Configuration dbConfig = config
                    .edit()
                    .withDefault(MySqlConnectorConfig.PORT, MySqlConnectorConfig.PORT.defaultValue())
                    .build()
                    .subset(DATABASE_CONFIG_PREFIX, true)
                    .merge(config.subset(DRIVER_CONFIG_PREFIX, true));

            final Builder jdbcConfigBuilder = dbConfig
                    .edit()
                    .with("connectTimeout", Long.toString(getConnectionTimeout().toMillis()))
                    .with("sslMode", sslMode().getValue());

            if (useSSL) {
                if (!Strings.isNullOrBlank(sslTrustStore())) {
                    jdbcConfigBuilder.with("trustCertificateKeyStoreUrl", "file:" + sslTrustStore());
                }
                if (sslTrustStorePassword() != null) {
                    jdbcConfigBuilder.with("trustCertificateKeyStorePassword", String.valueOf(sslTrustStorePassword()));
                }
                if (!Strings.isNullOrBlank(sslKeyStore())) {
                    jdbcConfigBuilder.with("clientCertificateKeyStoreUrl", "file:" + sslKeyStore());
                }
                if (sslKeyStorePassword() != null) {
                    jdbcConfigBuilder.with("clientCertificateKeyStorePassword", String.valueOf(sslKeyStorePassword()));
                }
            }

            jdbcConfigBuilder.with(JDBC_PROPERTY_CONNECTION_TIME_ZONE, determineConnectionTimeZone(dbConfig));

            // Set and remove options to prevent potential vulnerabilities
            jdbcConfigBuilder
                    .with("allowLoadLocalInfile", "false")
                    .with("allowUrlInLocalInfile", "false")
                    .with("autoDeserialize", false)
                    .without("queryInterceptors");

            this.jdbcConfig = JdbcConfiguration.adapt(jdbcConfigBuilder.build());
            String driverClassName = this.jdbcConfig.getString(MySqlConnectorConfig.JDBC_DRIVER);
            factory = JdbcConnection.patternBasedFactory(MySqlConnection.URL_PATTERN, driverClassName, getClass().getClassLoader());
        }

        private static String determineConnectionTimeZone(final Configuration dbConfig) {
            // Debezium by default expects timezoned data delivered in server timezone
            String connectionTimeZone = dbConfig.getString(JDBC_PROPERTY_CONNECTION_TIME_ZONE);

            if (connectionTimeZone != null) {
                return connectionTimeZone;
            }

            return "SERVER";
        }

        public JdbcConfiguration config() {
            return jdbcConfig;
        }

        public Configuration originalConfig() {
            return config;
        }

        public ConnectionFactory factory() {
            return factory;
        }

        public String username() {
            return config.getString(MySqlConnectorConfig.USER);
        }

        public String password() {
            return config.getString(MySqlConnectorConfig.PASSWORD);
        }

        public String hostname() {
            return config.getString(MySqlConnectorConfig.HOSTNAME);
        }

        public int port() {
            return config.getInteger(MySqlConnectorConfig.PORT);
        }

        public SecureConnectionMode sslMode() {
            String mode = config.getString(MySqlConnectorConfig.SSL_MODE);
            return SecureConnectionMode.parse(mode);
        }

        public boolean sslModeEnabled() {
            return sslMode() != SecureConnectionMode.DISABLED;
        }

        public String sslKeyStore() {
            return config.getString(MySqlConnectorConfig.SSL_KEYSTORE);
        }

        public char[] sslKeyStorePassword() {
            String password = config.getString(MySqlConnectorConfig.SSL_KEYSTORE_PASSWORD);
            return Strings.isNullOrBlank(password) ? null : password.toCharArray();
        }

        public String sslTrustStore() {
            return config.getString(MySqlConnectorConfig.SSL_TRUSTSTORE);
        }

        public char[] sslTrustStorePassword() {
            String password = config.getString(MySqlConnectorConfig.SSL_TRUSTSTORE_PASSWORD);
            return Strings.isNullOrBlank(password) ? null : password.toCharArray();
        }

        public Duration getConnectionTimeout() {
            return Duration.ofMillis(config.getLong(MySqlConnectorConfig.CONNECTION_TIMEOUT_MS));
        }

        public EventProcessingFailureHandlingMode eventProcessingFailureHandlingMode() {
            String mode = config.getString(CommonConnectorConfig.EVENT_PROCESSING_FAILURE_HANDLING_MODE);
            if (mode == null) {
                mode = config.getString(MySqlConnectorConfig.EVENT_DESERIALIZATION_FAILURE_HANDLING_MODE);
            }
            return EventProcessingFailureHandlingMode.parse(mode);
        }

        public EventProcessingFailureHandlingMode inconsistentSchemaHandlingMode() {
            String mode = config.getString(MySqlConnectorConfig.INCONSISTENT_SCHEMA_HANDLING_MODE);
            return EventProcessingFailureHandlingMode.parse(mode);
        }
    }

    @Override
    public Object getColumnValue(ResultSet rs, int columnIndex, Column column, Table table) throws SQLException {
        return mysqlFieldReader.readField(rs, columnIndex, column, table);
    }

    @Override
    public String quotedTableIdString(TableId tableId) {
        return tableId.toQuotedString('`');
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
