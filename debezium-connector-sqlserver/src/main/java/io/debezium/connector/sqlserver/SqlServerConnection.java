/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.sqlserver;

import java.sql.SQLException;
import java.util.Objects;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.util.IoUtil;

/**
 * {@link JdbcConnection} extension to be used with Microsoft SQL Server
 * 
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public class SqlServerConnection extends JdbcConnection {
    
    private static final String URL_PATTERN = "jdbc:sqlserver://${" + JdbcConfiguration.HOSTNAME + "}:${"
                                              + JdbcConfiguration.PORT + "};databaseName=${" + JdbcConfiguration.DATABASE + "}";
    protected static ConnectionFactory FACTORY = JdbcConnection.patternBasedFactory(URL_PATTERN,
                                                                                    com.microsoft.sqlserver.jdbc.SQLServerDriver.class.getName(),
                                                                                    SqlServerConnection.class.getClassLoader());
    private static Logger LOGGER = LoggerFactory.getLogger(SqlServerConnection.class);
    
    private static final String STATEMENTS_PLACEHOLDER = "#";
    private static final String ENABLE_DB_CDC;
    private static final String ENABLE_TABLE_CDC;
    private static final String CDC_WRAPPERS_DML;

    static {
        try {
            Properties statements = new Properties();
            ClassLoader classLoader = SqlServerConnection.class.getClassLoader();
            statements.load(classLoader.getResourceAsStream("statements.properties"));
            ENABLE_DB_CDC = statements.getProperty("enable_cdc_for_db");
            ENABLE_TABLE_CDC = statements.getProperty("enable_cdc_for_table");
            CDC_WRAPPERS_DML = IoUtil.read(classLoader.getResourceAsStream("generate_cdc_wrappers.sql"));
        } catch (Exception e) {
            throw new RuntimeException("Cannot load SQL Server statements", e);
        }
    }
    
    /**
     * Creates a new connection using the supplied configuration.
     *
     * @param config {@link Configuration} instance, may not be null.
     */
    public SqlServerConnection(Configuration config) {
        super(config, FACTORY);
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
     * Enables CDC for a given database, if not already enabled.
     * 
     * @param name the name of the DB, may not be {@code null}
     * @throws SQLException if anything unexpected fails
     */
    public void enableDBCDC( String name) throws SQLException {
        Objects.requireNonNull(name);
        execute(ENABLE_DB_CDC.replace(STATEMENTS_PLACEHOLDER, name));
    }
    
    
    /**
     * Enables CDC for a table if not already enabled and generates the wrapper functions for that table. 
     *
     * @param name the name of the table, may not be {@code null}
     * @throws SQLException if anything unexpected fails
     */
    public void enableTableCDC(String name) throws SQLException {
        Objects.requireNonNull(name);
        String enableCdcForTableStmt = ENABLE_TABLE_CDC.replace(STATEMENTS_PLACEHOLDER, name);
        String generateWrapperFunctionsStmts = CDC_WRAPPERS_DML.replaceAll(STATEMENTS_PLACEHOLDER, name);
        execute(enableCdcForTableStmt, generateWrapperFunctionsStmts);
    }
}
