/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

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

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.mysql.MySqlConnectorConfig.EventProcessingFailureHandlingMode;
import io.debezium.connector.mysql.MySqlConnectorConfig.SecureConnectionMode;
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

    protected static final String MYSQL_CONNECTION_URL = "jdbc:mysql://${hostname}:${port}/?useInformationSchema=true&nullCatalogMeansCurrent=false&useSSL=${useSSL}&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=convertToNull";
    protected static ConnectionFactory FACTORY = JdbcConnection.patternBasedFactory(MYSQL_CONNECTION_URL);

    protected final Logger logger = LoggerFactory.getLogger(getClass());
    protected final Configuration config;
    private final MySqlConnectorConfig connectorConfig;
    protected final JdbcConnection jdbc;
    private final Map<String, String> originalSystemProperties = new HashMap<>();

    public MySqlJdbcContext(Configuration config) {
        this.config = config; // must be set before most methods are used
        this.connectorConfig = new MySqlConnectorConfig(config);

        // Set up the JDBC connection without actually connecting, with extra MySQL-specific properties
        // to give us better JDBC database metadata behavior, including using UTF-8 for the client-side character encoding
        // per https://dev.mysql.com/doc/connector-j/5.1/en/connector-j-reference-charsets.html
        boolean useSSL = sslModeEnabled();
        Configuration jdbcConfig = config.filter(x -> !(x.startsWith(DatabaseHistory.CONFIGURATION_FIELD_PREFIX_STRING) || x.equals(MySqlConnectorConfig.DATABASE_HISTORY.name())))
                                         .subset("database.", true)
                                         .edit()
                                         .with("useSSL", Boolean.toString(useSSL))
                                         .build();
        String driverClassName = jdbcConfig.getString(MySqlConnectorConfig.JDBC_DRIVER);
        this.jdbc = new JdbcConnection(jdbcConfig,
                JdbcConnection.patternBasedFactory(MYSQL_CONNECTION_URL, driverClassName, getClass().getClassLoader()));
    }

    public Configuration config() {
        return config;
    }

    public MySqlConnectorConfig getConnectorConfig() {
        return connectorConfig;
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

    public EventProcessingFailureHandlingMode eventDeserializationFailureHandlingMode() {
        String mode = config.getString(MySqlConnectorConfig.EVENT_DESERIALIZATION_FAILURE_HANDLING_MODE);
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
            setSystemProperty("javax.net.ssl.trustStorePassword", MySqlConnectorConfig.SSL_KEYSTORE_PASSWORD, false);
        }
    }

    public void shutdown() {
        try {
            jdbc.close();
        } catch (SQLException e) {
            logger.error("Unexpected error shutting down the database connection", e);
        } finally {
            // Reset the system properties to their original value ...
            originalSystemProperties.forEach((name, value) -> {
                if (value != null) {
                    System.setProperty(name, value);
                } else {
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
     * Determine the available GTID set for MySQL.
     *
     * @return the string representation of MySQL's GTID sets; never null but an empty string if the server does not use GTIDs
     */
    public String knownGtidSet() {
        AtomicReference<String> gtidSetStr = new AtomicReference<String>();
        try {
            jdbc.query("SHOW MASTER STATUS", rs -> {
                if (rs.next() && rs.getMetaData().getColumnCount() > 4) {
                    gtidSetStr.set(rs.getString(5));// GTID set, may be null, blank, or contain a GTID set
                }
            });
        } catch (SQLException e) {
            throw new ConnectException("Unexpected error while connecting to MySQL and looking at GTID mode: ", e);
        }

        String result = gtidSetStr.get();
        return result != null ? result : "";
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
                    if (grants == null) return;
                    grants = grants.toUpperCase();
                    if (grants.contains("ALL") || grants.contains(grantName.toUpperCase())) {
                        result.set(true);
                    }
                }
            });
        } catch (SQLException e) {
            throw new ConnectException("Unexpected error while connecting to MySQL and looking at privileges for current user: ", e);
        }
        return result.get();
    }

    protected String connectionString() {
        return jdbc.connectionString(MYSQL_CONNECTION_URL);
    }

    /**
     * Read the MySQL charset-related system variables.
     *
     * @param sql the reference that should be set to the SQL statement; may be null if not needed
     * @return the system variables that are related to server character sets; never null
     */
    protected Map<String, String> readMySqlCharsetSystemVariables(AtomicReference<String> sql) {
        // Read the system variables from the MySQL instance and get the current database name ...
        Map<String, String> variables = new HashMap<>();
        try (JdbcConnection mysql = jdbc.connect()) {
            logger.debug("Reading MySQL charset-related system variables before parsing DDL history.");
            String statement = "SHOW VARIABLES WHERE Variable_name IN ('character_set_server','collation_server')";
            if (sql != null) sql.set(statement);
            mysql.query(statement, rs -> {
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
        } catch (SQLException e) {
            throw new ConnectException("Error reading MySQL variables: " + e.getMessage(), e);
        }
        return variables;
    }

    /**
     * Read the MySQL system variables.
     *
     * @param sql the reference that should be set to the SQL statement; may be null if not needed
     * @return the system variables that are related to server character sets; never null
     */
    protected Map<String, String> readMySqlSystemVariables(AtomicReference<String> sql) {
        // Read the system variables from the MySQL instance and get the current database name ...
        Map<String, String> variables = new HashMap<>();
        try (JdbcConnection mysql = jdbc.connect()) {
            logger.debug("Reading MySQL system variables");
            String statement = "SHOW VARIABLES";
            if (sql != null) sql.set(statement);
            mysql.query(statement, rs -> {
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
        } catch (SQLException e) {
            throw new ConnectException("Error reading MySQL variables: " + e.getMessage(), e);
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
            } else {
                sb.append(", ");
            }
            sb.append(varName).append("=");
            String value = variables.get(varName);
            if (value == null) value = "";
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
            } else {
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
}
