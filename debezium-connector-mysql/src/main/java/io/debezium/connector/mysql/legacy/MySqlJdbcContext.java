/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.legacy;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.CommonConnectorConfig.EventProcessingFailureHandlingMode;
import io.debezium.config.Configuration;
import io.debezium.config.Configuration.Builder;
import io.debezium.config.Field;
import io.debezium.connector.mysql.GtidSet;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlConnectorConfig.SecureConnectionMode;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.jdbc.JdbcConnection.ConnectionFactory;
import io.debezium.relational.history.DatabaseHistory;
import io.debezium.util.Strings;

/**
 * A context for a JDBC connection to MySQL.
 *
 * @author Randall Hauch
 */
public class MySqlJdbcContext implements AutoCloseable {

    protected static final String MYSQL_CONNECTION_URL = "jdbc:mysql://${hostname}:${port}/?useInformationSchema=true&nullCatalogMeansCurrent=false&useSSL=${useSSL}&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&connectTimeout=${connectTimeout}";
    protected static final String JDBC_PROPERTY_LEGACY_DATETIME = "useLegacyDatetimeCode";

    private static final String SQL_SHOW_SYSTEM_VARIABLES = "SHOW VARIABLES";
    private static final String SQL_SHOW_SYSTEM_VARIABLES_CHARACTER_SET = "SHOW VARIABLES WHERE Variable_name IN ('character_set_server','collation_server')";
    private static final String SQL_SHOW_SESSION_VARIABLE_SSL_VERSION = "SHOW SESSION STATUS LIKE 'Ssl_version'";

    protected static ConnectionFactory FACTORY = JdbcConnection.patternBasedFactory(MYSQL_CONNECTION_URL,
            JdbcConfiguration.PORT.withDefault(MySqlConnectorConfig.PORT.defaultValueAsString()));

    protected static final Logger logger = LoggerFactory.getLogger(MySqlJdbcContext.class);
    protected final Configuration config;
    protected final JdbcConnection jdbc;
    private final Map<String, String> originalSystemProperties = new HashMap<>();

    public MySqlJdbcContext(MySqlConnectorConfig config) {
        this.config = config.getConfig(); // must be set before most methods are used

        // Set up the JDBC connection without actually connecting, with extra MySQL-specific properties
        // to give us better JDBC database metadata behavior, including using UTF-8 for the client-side character encoding
        // per https://dev.mysql.com/doc/connector-j/5.1/en/connector-j-reference-charsets.html
        boolean useSSL = sslModeEnabled();
        Configuration jdbcConfig = this.config
                .filter(x -> !(x.startsWith(DatabaseHistory.CONFIGURATION_FIELD_PREFIX_STRING) || x.equals(MySqlConnectorConfig.DATABASE_HISTORY.name())))
                .edit()
                .withDefault(MySqlConnectorConfig.PORT, MySqlConnectorConfig.PORT.defaultValue())
                .withDefault("database.useCursorFetch", config.useCursorFetch())
                .build()
                .subset("database.", true);

        Builder jdbcConfigBuilder = jdbcConfig
                .edit()
                .with("connectTimeout", Long.toString(config.getConnectionTimeout().toMillis()))
                .with("useSSL", Boolean.toString(useSSL));

        final String legacyDateTime = jdbcConfig.getString(JDBC_PROPERTY_LEGACY_DATETIME);
        if (legacyDateTime == null) {
            jdbcConfigBuilder.with(JDBC_PROPERTY_LEGACY_DATETIME, "false");
        }
        else if ("true".equals(legacyDateTime)) {
            logger.warn("'{}' is set to 'true'. This setting is not recommended and can result in timezone issues.", JDBC_PROPERTY_LEGACY_DATETIME);
        }

        jdbcConfig = jdbcConfigBuilder.build();
        String driverClassName = jdbcConfig.getString(MySqlConnectorConfig.JDBC_DRIVER);
        this.jdbc = new JdbcConnection(jdbcConfig,
                JdbcConnection.patternBasedFactory(MYSQL_CONNECTION_URL, driverClassName, getClass().getClassLoader()), "`", "`");
    }

    public Configuration config() {
        return config;
    }

    public JdbcConnection jdbc() {
        return jdbc;
    }

    public Logger logger() {
        return logger;
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

    public void start() {
        if (sslModeEnabled()) {
            originalSystemProperties.clear();
            // Set the System properties for SSL for the MySQL driver ...
            setSystemProperty("javax.net.ssl.keyStore", MySqlConnectorConfig.SSL_KEYSTORE, true);
            setSystemProperty("javax.net.ssl.keyStorePassword", MySqlConnectorConfig.SSL_KEYSTORE_PASSWORD, false);
            setSystemProperty("javax.net.ssl.trustStore", MySqlConnectorConfig.SSL_TRUSTSTORE, true);
            setSystemProperty("javax.net.ssl.trustStorePassword", MySqlConnectorConfig.SSL_TRUSTSTORE_PASSWORD, false);
        }
    }

    public void shutdown() {
        try {
            jdbc.close();
        }
        catch (SQLException e) {
            logger.error("Unexpected error shutting down the database connection", e);
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

    @Override
    public void close() {
        shutdown();
    }

    /**
     * Determine whether the MySQL server has GTIDs enabled.
     *
     * @return {@code false} if the server's {@code gtid_mode} is set and is {@code OFF}, or {@code true} otherwise
     */
    public boolean isGtidModeEnabled() {
        AtomicReference<String> mode = new AtomicReference<String>("off");
        try {
            jdbc().query("SHOW GLOBAL VARIABLES LIKE 'GTID_MODE'", rs -> {
                if (rs.next()) {
                    mode.set(rs.getString(2));
                }
            });
        }
        catch (SQLException e) {
            throw new ConnectException("Unexpected error while connecting to MySQL and looking at GTID mode: ", e);
        }

        return !"OFF".equalsIgnoreCase(mode.get());
    }

    /**
     * Determine the executed GTID set for MySQL.
     *
     * @return the string representation of MySQL's GTID sets; never null but an empty string if the server does not use GTIDs
     */
    public String knownGtidSet() {
        AtomicReference<String> gtidSetStr = new AtomicReference<String>();
        try {
            jdbc.query("SHOW MASTER STATUS", rs -> {
                if (rs.next() && rs.getMetaData().getColumnCount() > 4) {
                    gtidSetStr.set(rs.getString(5)); // GTID set, may be null, blank, or contain a GTID set
                }
            });
        }
        catch (SQLException e) {
            throw new ConnectException("Unexpected error while connecting to MySQL and looking at GTID mode: ", e);
        }

        String result = gtidSetStr.get();
        return result != null ? result : "";
    }

    /**
     * Determine the difference between two sets.
     *
     * @return a subtraction of two GTID sets; never null
     */
    public GtidSet subtractGtidSet(GtidSet set1, GtidSet set2) {
        try {
            return jdbc.prepareQueryAndMap("SELECT GTID_SUBTRACT(?, ?)",
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
            throw new ConnectException("Unexpected error while connecting to MySQL and looking at GTID mode: ", e);
        }
    }

    /**
     * Get the purged GTID values from MySQL (gtid_purged value)
     *
     * @return A GTID set; may be empty if not using GTIDs or none have been purged yet
     */
    public GtidSet purgedGtidSet() {
        AtomicReference<String> gtidSetStr = new AtomicReference<String>();
        try {
            jdbc.query("SELECT @@global.gtid_purged", rs -> {
                if (rs.next() && rs.getMetaData().getColumnCount() > 0) {
                    gtidSetStr.set(rs.getString(1)); // GTID set, may be null, blank, or contain a GTID set
                }
            });
        }
        catch (SQLException e) {
            throw new ConnectException("Unexpected error while connecting to MySQL and looking at gtid_purged variable: ", e);
        }

        String result = gtidSetStr.get();
        if (result == null) {
            result = "";
        }

        return new GtidSet(result);
    }

    /**
     * Determine if the current user has the named privilege. Note that if the user has the "ALL" privilege this method
     * returns {@code true}.
     *
     * @param grantName the name of the MySQL privilege; may not be null
     * @return {@code true} if the user has the named privilege, or {@code false} otherwise
     */
    public boolean userHasPrivileges(String grantName) {
        AtomicBoolean result = new AtomicBoolean(false);
        try {
            jdbc.query("SHOW GRANTS FOR CURRENT_USER", rs -> {
                while (rs.next()) {
                    String grants = rs.getString(1);
                    logger.debug(grants);
                    if (grants == null) {
                        return;
                    }
                    grants = grants.toUpperCase();
                    if (grants.contains("ALL") || grants.contains(grantName.toUpperCase())) {
                        result.set(true);
                    }
                }
            });
        }
        catch (SQLException e) {
            throw new ConnectException("Unexpected error while connecting to MySQL and looking at privileges for current user: ", e);
        }
        return result.get();
    }

    public String connectionString() {
        return jdbc.connectionString(MYSQL_CONNECTION_URL);
    }

    /**
     * Read the MySQL charset-related system variables.
     *
     * @return the system variables that are related to server character sets; never null
     */
    protected Map<String, String> readMySqlCharsetSystemVariables() {
        // Read the system variables from the MySQL instance and get the current database name ...
        logger.debug("Reading MySQL charset-related system variables before parsing DDL history.");
        return querySystemVariables(SQL_SHOW_SYSTEM_VARIABLES_CHARACTER_SET);
    }

    /**
     * Read the MySQL system variables.
     *
     * @return the system variables that are related to server character sets; never null
     */
    public Map<String, String> readMySqlSystemVariables() {
        // Read the system variables from the MySQL instance and get the current database name ...
        logger.debug("Reading MySQL system variables");
        return querySystemVariables(SQL_SHOW_SYSTEM_VARIABLES);
    }

    private Map<String, String> querySystemVariables(String statement) {
        Map<String, String> variables = new HashMap<>();
        try {
            start();
            jdbc.connect().query(statement, rs -> {
                while (rs.next()) {
                    String varName = rs.getString(1);
                    String value = rs.getString(2);
                    if (varName != null && value != null) {
                        variables.put(varName, value);
                        logger.debug("\t{} = {}",
                                Strings.pad(varName, 45, ' '),
                                Strings.pad(value, 45, ' '));
                    }
                }
            });
        }
        catch (SQLException e) {
            throw new ConnectException("Error reading MySQL variables: " + e.getMessage(), e);
        }

        return variables;
    }

    /**
     * Read the MySQL default character sets for exisiting databases.
     *
     * @return the map of database names with their default character sets; never null
     */
    protected Map<String, DatabaseLocales> readDatabaseCollations() {
        logger.debug("Reading default database charsets");
        try {
            start();
            return jdbc.connect().queryAndMap("SELECT schema_name, default_character_set_name, default_collation_name FROM information_schema.schemata", rs -> {
                final Map<String, DatabaseLocales> charsets = new HashMap<>();
                while (rs.next()) {
                    String dbName = rs.getString(1);
                    String charset = rs.getString(2);
                    String collation = rs.getString(3);
                    if (dbName != null && (charset != null || collation != null)) {
                        charsets.put(dbName, new DatabaseLocales(charset, collation));
                        logger.debug("\t{} = {}, {}",
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
        String value = config.getString(field);
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
                    throw new ConnectException(msg);
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
    public String getSessionVariableForSslVersion() {
        final String SSL_VERSION = "Ssl_version";
        logger.debug("Reading MySQL Session variable for Ssl Version");
        Map<String, String> sessionVariables = querySystemVariables(SQL_SHOW_SESSION_VARIABLE_SSL_VERSION);
        if (!sessionVariables.isEmpty() && sessionVariables.containsKey(SSL_VERSION)) {
            return sessionVariables.get(SSL_VERSION);
        }
        return null;
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
                logger.debug("Setting default charset '{}' for database '{}'", charset, dbName);
                ddl.append(" CHARSET ").append(charset);
            }
            else {
                logger.info("Default database charset for '{}' not found", dbName);
            }
            if (collation != null) {
                logger.debug("Setting default collation '{}' for database '{}'", collation, dbName);
                ddl.append(" COLLATE ").append(collation);
            }
            else {
                logger.info("Default database collation for '{}' not found", dbName);
            }
        }
    }
}
