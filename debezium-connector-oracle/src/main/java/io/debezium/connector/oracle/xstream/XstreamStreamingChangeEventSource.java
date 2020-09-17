/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.xstream;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.SourceInfo;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
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
public class XstreamStreamingChangeEventSource implements StreamingChangeEventSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(XstreamStreamingChangeEventSource.class);

    private final JdbcConnection jdbcConnection;
    private final EventDispatcher<TableId> dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final OracleDatabaseSchema schema;
    private final OracleOffsetContext offsetContext;
    private final String xStreamServerName;
    private volatile XStreamOut xsOut;
    private final boolean tablenameCaseInsensitive;
    private final int posVersion;

    public XstreamStreamingChangeEventSource(OracleConnectorConfig connectorConfig, OracleOffsetContext offsetContext, JdbcConnection jdbcConnection,
                                             EventDispatcher<TableId> dispatcher, ErrorHandler errorHandler, Clock clock, OracleDatabaseSchema schema) {
        this.jdbcConnection = jdbcConnection;
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.schema = schema;
        this.offsetContext = offsetContext;
        this.xStreamServerName = connectorConfig.getXoutServerName();
        this.tablenameCaseInsensitive = connectorConfig.getTablenameCaseInsensitive();
        this.posVersion = connectorConfig.getOracleVersion().getPosVersion();
    }

    @Override
    public void execute(ChangeEventSourceContext context) throws InterruptedException {
        try {
            // 1. connect
            final byte[] startPosition = offsetContext.getLcrPosition() != null ? offsetContext.getLcrPosition().getRawPosition()
                    : convertScnToPosition(offsetContext.getScn());
            xsOut = XStreamOut.attach((OracleConnection) jdbcConnection.connection(), xStreamServerName,
                    startPosition, 1, 1, XStreamOut.DEFAULT_MODE);

            LcrEventHandler handler = new LcrEventHandler(errorHandler, dispatcher, clock, schema, offsetContext, this.tablenameCaseInsensitive);

            // 2. receive events while running
            while (context.isRunning()) {
                LOGGER.trace("Receiving LCR");
                xsOut.receiveLCRCallback(handler, XStreamOut.DEFAULT_MODE);
            }
        }
        catch (Throwable e) {
            errorHandler.setProducerThrowable(e);
        }
        finally {
            // 3. disconnect
            if (this.xsOut != null) {
                try {
                    XStreamOut xsOut = this.xsOut;
                    this.xsOut = null;
                    xsOut.detach(XStreamOut.DEFAULT_MODE);
                }
                catch (StreamsException e) {
                    LOGGER.error("Couldn't detach from XStream outbound server " + xStreamServerName, e);
                }
            }
        }
    }

    @Override
    public void commitOffset(Map<String, ?> offset) {
        if (xsOut != null) {
            try {
                LOGGER.debug("Recording offsets to Oracle");
                final LcrPosition lcrPosition = LcrPosition.valueOf((String) offset.get(SourceInfo.LCR_POSITION_KEY));
                final Long scn = (Long) offset.get(SourceInfo.SCN_KEY);
                if (lcrPosition != null) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Recording position {}", lcrPosition);
                    }
                    xsOut.setProcessedLowWatermark(
                            lcrPosition.getRawPosition(),
                            XStreamOut.DEFAULT_MODE);
                }
                else if (scn != null) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Recording position with SCN {}", scn);
                    }
                    xsOut.setProcessedLowWatermark(
                            convertScnToPosition(scn),
                            XStreamOut.DEFAULT_MODE);
                }
                else {
                    LOGGER.warn("Nothing in offsets could be recorded to Oracle");
                    return;
                }
                LOGGER.trace("Offsets recorded to Oracle");
            }
            catch (StreamsException e) {
                throw new RuntimeException("Couldn't set processed low watermark", e);
            }
        }
    }

    private byte[] convertScnToPosition(long scn) {
        try {
            return XStreamUtility.convertSCNToPosition(new NUMBER(scn), this.posVersion);
        }
        catch (StreamsException e) {
            throw new RuntimeException(e);
        }
    }
}
