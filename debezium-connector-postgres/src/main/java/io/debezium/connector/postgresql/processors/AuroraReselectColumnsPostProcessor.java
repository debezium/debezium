/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.processors;

import java.sql.SQLException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.bean.spi.BeanRegistry;
import io.debezium.common.annotation.Incubating;
import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.processors.reselect.ReselectColumnsPostProcessor;

/**
 * An extended implementation of the Debezium {@link ReselectColumnsPostProcessor}
 * using reader instance in Aurora to reselect columns.
 *
 * @author Gaurav Miglani
 */
@Incubating
public class AuroraReselectColumnsPostProcessor extends ReselectColumnsPostProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(AuroraReselectColumnsPostProcessor.class);

    private static final String READER_HOST = "reader.host";

    private static final String READER_PORT = "reader.port";

    private String readerHost;

    private int readerPort;

    @Override
    public void configure(Map<String, ?> properties) {
        final Configuration config = Configuration.from(properties);
        super.configure(properties);
        this.readerHost = config.getString(READER_HOST);
        if (this.readerHost == null || this.readerHost.isEmpty()) {
            throw new IllegalArgumentException("Reader host cannot be null or empty");
        }

        Integer port = config.getInteger(READER_PORT);
        if (port == null || port <= 0) {
            throw new IllegalArgumentException("Reader port must be a positive integer");
        }
        this.readerPort = port;
    }

    @Override
    public void close() {
        super.close();
        try {
            getJdbcConnection().close();
        }
        catch (SQLException e) {
            LOGGER.error("Error closing JDBC connection", e);
        }
    }

    @Override
    protected void resolveJdbcConnection(BeanRegistry beanRegistry) {
        super.resolveJdbcConnection(beanRegistry);
        // create reader connection
        LOGGER.info("Creating reader connection for reselect using reader host: {} and port: {}", readerHost, readerPort);
        JdbcConfiguration newJdbcConfiguration = JdbcConfiguration.copy(getJdbcConnection().config())
                .with(JdbcConfiguration.HOSTNAME, readerHost)
                .with(JdbcConfiguration.PORT, readerPort)
                .build();
        setJdbcConnection(new PostgresConnection(newJdbcConfiguration, PostgresConnection.CONNECTION_AURORA_READER_RESELECT));
    }
}
