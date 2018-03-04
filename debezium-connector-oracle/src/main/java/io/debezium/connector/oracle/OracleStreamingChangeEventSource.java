/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.util.Clock;
import oracle.jdbc.OracleConnection;
import oracle.sql.NUMBER;
import oracle.streams.StreamsException;
import oracle.streams.XStreamOut;
import oracle.streams.XStreamUtility;

/**
 * A {@link StreamingChangeEventSource} based on Oracle's XStream API. The XStream event handler loop is executed in a
 * separate executor.
 *
 * @author Gunnar Morling
 */
public class OracleStreamingChangeEventSource implements StreamingChangeEventSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(OracleStreamingChangeEventSource.class);

    private final JdbcConnection jdbcConnection;
    private final EventDispatcher dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final OracleDatabaseSchema schema;
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
    }

    @Override
    public void execute(ChangeEventSourceContext context) throws InterruptedException {
        XStreamOut xsOut = null;

        try {
            // 1. connect
            xsOut = XStreamOut.attach((OracleConnection) jdbcConnection.connection(), xStreamServerName,
                    convertScnToPosition(offsetContext.getScn()), 1, 1, XStreamOut.DEFAULT_MODE);

            LcrEventHandler handler = new LcrEventHandler(errorHandler, dispatcher, clock, schema, offsetContext);

            // 2. receive events while running
            while(context.isRunning()) {
                LOGGER.trace("Receiving LCR");
                xsOut.receiveLCRCallback(handler, XStreamOut.DEFAULT_MODE);
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        finally {
            // 3. disconnect
            if (xsOut != null) {
                try {
                    xsOut.detach(XStreamOut.DEFAULT_MODE);
                }
                catch (StreamsException e) {
                    LOGGER.error("Couldn't detach from XStream outbound server " + xStreamServerName, e);
                }
            }
        }
    }

    private byte[] convertScnToPosition(long scn) {
        try {
            return XStreamUtility.convertSCNToPosition(new NUMBER(scn), XStreamUtility.POS_VERSION_V2);
        }
        catch (StreamsException e) {
            throw new RuntimeException(e);
        }
    }
}
