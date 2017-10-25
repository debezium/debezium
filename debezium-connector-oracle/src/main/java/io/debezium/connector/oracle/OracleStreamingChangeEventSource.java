/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.sql.Connection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.util.Clock;
import io.debezium.util.Threads;
import oracle.jdbc.OracleConnection;
import oracle.streams.StreamsException;
import oracle.streams.XStreamOut;

public class OracleStreamingChangeEventSource implements StreamingChangeEventSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(OracleStreamingChangeEventSource.class);

    private final JdbcConnection jdbcConnection;
    private final EventDispatcher dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final OracleDatabaseSchema schema;
    private final ExecutorService executor;
    private final OracleOffsetContext offsetContext;
    private final String xStreamServerName;

    public OracleStreamingChangeEventSource(OracleConnectorConfig connectorConfig, OracleOffsetContext offsetContext, JdbcConnection jdbcConnection, EventDispatcher dispatcher, ErrorHandler errorHandler, Clock clock, OracleDatabaseSchema schema) {
        this.jdbcConnection = jdbcConnection;
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.schema = schema;
        this.offsetContext = offsetContext;
        this.xStreamServerName = connectorConfig.getXoutServerName();
        this.executor = Threads.newSingleThreadExecutor(OracleConnector.class, connectorConfig.getLogicalName(), "xstream-handler");
    }

    @Override
    public void start() {
        Connection connection;

        try {
            connection = jdbcConnection.connection();
        }
        catch(Exception e) {
            throw new RuntimeException(e);
        }

        LcrEventHandler hdlr = new LcrEventHandler(errorHandler, dispatcher, clock, schema, offsetContext);

        executor.execute(() -> {
            XStreamOut xsOut = null;

            try {
                xsOut = XStreamOut.attach((OracleConnection) connection, xStreamServerName, offsetContext.getPosition(), 1, 1, XStreamOut.DEFAULT_MODE);

                while(isRunning()) {
                    LOGGER.trace("Receiving LCR");
                    xsOut.receiveLCRCallback(hdlr, XStreamOut.DEFAULT_MODE);
                }
            }
            catch (Exception e) {
                errorHandler.setProducerThrowable(e);
            }
            finally {
                if (xsOut != null) {
                    try {
                        xsOut.detach(XStreamOut.DEFAULT_MODE);
                    }
                    catch (StreamsException e) {
                        LOGGER.error("Couldn't detach from XStream outbound server " + xStreamServerName, e);
                    }
                }
            }
        });
    }

    private boolean isRunning() {
        return !Thread.currentThread().isInterrupted();
    }

    @Override
    public void stop() throws InterruptedException {
        executor.shutdownNow();
        executor.awaitTermination(60, TimeUnit.SECONDS);
    };
}
