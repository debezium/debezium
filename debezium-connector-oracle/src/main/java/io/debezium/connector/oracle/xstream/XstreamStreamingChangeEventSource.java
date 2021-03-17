/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.xstream;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.SourceInfo;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

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

    private final OracleConnection jdbcConnection;
    private final EventDispatcher<TableId> dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final OracleDatabaseSchema schema;
    private final OracleOffsetContext offsetContext;
    private final String xStreamServerName;
    private volatile XStreamOut xsOut;
    private final boolean tablenameCaseInsensitive;
    private final int posVersion;
    /**
     * A message box between thread that is informed about committed offsets and the XStream thread.
     * When the last offset is committed its value is passed to the XStream thread and a watermark is
     * set to signal which events were safely processed.
     * This is important as setting watermark in a concurrent thread can lead to a deadlock due to an
     * internal Oracle code locking.
     */
    private final AtomicReference<PositionAndScn> lcrMessage = new AtomicReference<>();

    public XstreamStreamingChangeEventSource(OracleConnectorConfig connectorConfig, OracleOffsetContext offsetContext, OracleConnection jdbcConnection,
                                             EventDispatcher<TableId> dispatcher, ErrorHandler errorHandler, Clock clock, OracleDatabaseSchema schema) {
        this.jdbcConnection = jdbcConnection;
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.schema = schema;
        this.offsetContext = offsetContext;
        this.xStreamServerName = connectorConfig.getXoutServerName();
        this.tablenameCaseInsensitive = jdbcConnection.getTablenameCaseInsensitivity(connectorConfig);
        this.posVersion = connectorConfig.getOracleVersion().getPosVersion();
    }

    @Override
    public void execute(ChangeEventSourceContext context) throws InterruptedException {
        try {
            // 1. connect
            final byte[] startPosition = offsetContext.getLcrPosition() != null ? offsetContext.getLcrPosition().getRawPosition()
                    : convertScnToPosition(offsetContext.getScn());
            xsOut = XStreamOut.attach((oracle.jdbc.OracleConnection) jdbcConnection.connection(), xStreamServerName,
                    startPosition, 1, 1, XStreamOut.DEFAULT_MODE);

            LcrEventHandler handler = new LcrEventHandler(errorHandler, dispatcher, clock, schema, offsetContext, this.tablenameCaseInsensitive, this);

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
            LOGGER.debug("Sending message to request recording of offsets to Oracle");
            final LcrPosition lcrPosition = LcrPosition.valueOf((String) offset.get(SourceInfo.LCR_POSITION_KEY));
            final Long scn = (Long) offset.get(SourceInfo.SCN_KEY);
            // We can safely overwrite the message even if it was not processed. The watermarked will be set to the highest
            // (last) delivered value in a single step instead of incrementally
            sendPublishedPosition(lcrPosition, scn);
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

    public static class PositionAndScn {
        public final LcrPosition position;
        public final byte[] scn;

        public PositionAndScn(LcrPosition position, byte[] scn) {
            this.position = position;
            this.scn = scn;
        }
    }

    XStreamOut getXsOut() {
        return xsOut;
    }

    private void sendPublishedPosition(final LcrPosition lcrPosition, final Long scn) {
        lcrMessage.set(new PositionAndScn(lcrPosition, (scn != null) ? convertScnToPosition(scn) : null));
    }

    PositionAndScn receivePublishedPosition() {
        return lcrMessage.getAndSet(null);
    }
}
