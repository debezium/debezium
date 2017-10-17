/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql.connection;

import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.postgresql.PGConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.jdbc.JdbcConnectionException;

/**
 * Implementation of a {@link ReplicationConnection} for Postgresql. Note that replication connections in PG cannot execute
 * regular statements but only a limited number of replication-related commands.
 *
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public class PostgresReplicationConnection extends JdbcConnection implements ReplicationConnection {
    private static Logger LOGGER = LoggerFactory.getLogger(PostgresReplicationConnection.class);

    private final String slotName;
    private final PostgresConnectorConfig.LogicalDecoder plugin;
    private final boolean dropSlotOnClose;
    private final Configuration originalConfig;
    private final Integer statusUpdateIntervalMillis;
    private final MessageDecoder messageDecoder;

    private long defaultStartingPos;

    /**
     * Creates a new replication connection with the given params.
     *
     * @param config the JDBC configuration for the connection; may not be null
     * @param slotName the name of the DB slot for logical replication; may not be null
     * @param plugin decoder matching the server side plug-in used for streaming changes; may not be null
     * @param dropSlotOnClose whether the replication slot should be dropped once the connection is closed
     * @param statusUpdateIntervalMillis the number of milli-seconds at which the replication connection should periodically send status
     * updates to the server
     */
    private PostgresReplicationConnection(Configuration config,
                                         String slotName,
                                         PostgresConnectorConfig.LogicalDecoder plugin,
                                         boolean dropSlotOnClose,
                                         Integer statusUpdateIntervalMillis) {
        super(config, PostgresConnection.FACTORY, null ,PostgresReplicationConnection::defaultSettings);

        this.originalConfig = config;
        this.slotName = slotName;
        this.plugin = plugin;
        this.dropSlotOnClose = dropSlotOnClose;
        this.statusUpdateIntervalMillis = statusUpdateIntervalMillis;
        this.messageDecoder = plugin.messageDecoder();

        try {
            initReplicationSlot();
        } catch (SQLException e) {
            throw new JdbcConnectionException("Cannot create replication connection", e);
        }
    }

    protected void initReplicationSlot() throws SQLException {
        ServerInfo.ReplicationSlot slotInfo;
        try (PostgresConnection connection = new PostgresConnection(originalConfig)) {
            slotInfo = connection.readReplicationSlotInfo(slotName, plugin.getValue());
        }

        boolean shouldCreateSlot = ServerInfo.ReplicationSlot.INVALID == slotInfo;
        try {
            if (shouldCreateSlot) {
                LOGGER.debug("Creating new replication slot '{}' for plugin '{}'", slotName, plugin);
                // there's no info for this plugin and slot so create a new slot
                pgConnection().getReplicationAPI()
                              .createReplicationSlot()
                              .logical()
                              .withSlotName(slotName)
                              .withOutputPlugin(plugin.getValue())
                              .make();
            } else if (slotInfo.active()) {
                LOGGER.error(
                        "A logical replication slot named '{}' for plugin '{}' and database '{}' is already active on the server." +
                        "You cannot have multiple slots with the same name active for the same database",
                        slotName, plugin.getValue(), database());
                throw new IllegalStateException();
            }

            AtomicLong xlogStart = new AtomicLong();
            execute(statement -> {
                String identifySystemStatement = "IDENTIFY_SYSTEM";
                LOGGER.debug("running '{}' to validate replication connection", identifySystemStatement);
                try (ResultSet rs = statement.executeQuery(identifySystemStatement)) {
                    if (!rs.next()) {
                        throw new IllegalStateException("The DB connection is not a valid replication connection");
                    }
                    String xlogpos = rs.getString("xlogpos");
                    LOGGER.debug("received latest xlogpos '{}'", xlogpos);
                    xlogStart.compareAndSet(0, LogSequenceNumber.valueOf(xlogpos).asLong());
                }
            });

            if (shouldCreateSlot || !slotInfo.hasValidFlushedLSN()) {
                // this is a new slot or we weren't able to read a valid flush LSN pos, so we always start from the xlog pos that was reported
                this.defaultStartingPos = xlogStart.get();
            } else {
                Long latestFlushedLSN = slotInfo.latestFlushedLSN();
                this.defaultStartingPos = latestFlushedLSN < xlogStart.get() ? latestFlushedLSN : xlogStart.get();
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("found previous flushed LSN '{}'", ReplicationConnection.format(latestFlushedLSN));
                }
            }
        } catch (SQLException e) {
            throw new JdbcConnectionException(e);
        }
    }

    @Override
    public ReplicationStream startStreaming() throws SQLException {
        return startStreaming(defaultStartingPos);
    }

    @Override
    public ReplicationStream startStreaming(Long offset) throws SQLException {
        connect();
        if (offset == null || offset <= 0) {
            offset = defaultStartingPos;
        }
        LogSequenceNumber lsn = LogSequenceNumber.valueOf(offset);
        LOGGER.debug("starting streaming from LSN '{}'", lsn.asString());
        return createReplicationStream(lsn);
    }

    protected PGConnection pgConnection() throws SQLException {
        return (PGConnection) connection();
    }

    private ReplicationStream createReplicationStream(final LogSequenceNumber lsn) throws SQLException {
        assert lsn != null;
        ChainedLogicalStreamBuilder streamBuilder = pgConnection()
                .getReplicationAPI()
                .replicationStream()
                .logical()
                .withSlotName(slotName)
                .withStartPosition(lsn);
        streamBuilder = messageDecoder.options(streamBuilder);

        if (statusUpdateIntervalMillis != null && statusUpdateIntervalMillis > 0) {
            streamBuilder.withStatusInterval(statusUpdateIntervalMillis, TimeUnit.MILLISECONDS);
        }

        PGReplicationStream stream = streamBuilder.start();
        final long lsnLong = lsn.asLong();
        return new ReplicationStream() {
            private static final int CHECK_WARNINGS_AFTER_COUNT = 100;
            private int warningCheckCounter = CHECK_WARNINGS_AFTER_COUNT;

            // make sure this is volatile since multiple threads may be interested in this value
            private volatile LogSequenceNumber lastReceivedLSN;

            @Override
            public void read(ReplicationMessageProcessor processor) throws SQLException {
                ByteBuffer read = stream.read();
                // the lsn we started from is inclusive, so we need to avoid sending back the same message twice
                if (lsnLong >= stream.getLastReceiveLSN().asLong()) {
                    return;
                }
                deserializeMessages(read, processor);
            }

            @Override
            public void readPending(ReplicationMessageProcessor processor) throws SQLException {
                ByteBuffer read = stream.readPending();
                // the lsn we started from is inclusive, so we need to avoid sending back the same message twice
                if (read == null ||  lsnLong >= stream.getLastReceiveLSN().asLong()) {
                    return;
                }
                deserializeMessages(read, processor);
            }

            private void deserializeMessages(ByteBuffer buffer, ReplicationMessageProcessor processor) throws SQLException {
                lastReceivedLSN = stream.getLastReceiveLSN();
                messageDecoder.processMessage(buffer, processor);
            }

            @Override
            public void close() throws SQLException {
                processWarnings(true);
                stream.close();
            }

            @Override
            public void flushLSN() throws SQLException {
                if (lastReceivedLSN == null) {
                    // nothing to flush yet, since we haven't read anything...
                    return;
                }
                stream.setFlushedLSN(lastReceivedLSN);
                stream.setAppliedLSN(lastReceivedLSN);
                stream.forceUpdateStatus();
            }

            @Override
            public Long lastReceivedLSN() {
                return lastReceivedLSN != null ? lastReceivedLSN.asLong() : null;
            }

            private void processWarnings(final boolean forced) throws SQLException {
                if (--warningCheckCounter == 0 || forced) {
                    warningCheckCounter = CHECK_WARNINGS_AFTER_COUNT;
                    for (SQLWarning w = connection().getWarnings(); w != null; w = w.getNextWarning()) {
                        LOGGER.debug("Server-side message: '{}', state = {}, code = {}",
                                w.getMessage(), w.getSQLState(), w.getErrorCode());
                    }
                }
            }
        };
    }

    @Override
    public synchronized void close() {
        try {
            super.close();
        } catch (SQLException e) {
            LOGGER.error("Unexpected error while closing Postgres connection", e);
        }
        if (dropSlotOnClose) {
            // we're dropping the replication slot via a regular - i.e. not a replication - connection
            try (PostgresConnection connection = new PostgresConnection(originalConfig)) {
                connection.dropReplicationSlot(slotName);
            }
        }
    }

    protected static void defaultSettings(Configuration.Builder builder) {
        // first copy the parent's default settings...
        PostgresConnection.defaultSettings(builder);
        // then set some additional replication specific settings
        builder.with("replication", "database")
               .with("preferQueryMode", "simple"); // replication protocol only supports simple query mode
    }

    protected static class ReplicationConnectionBuilder implements Builder {

        private final Configuration config;
        private String slotName = DEFAULT_SLOT_NAME;
        private PostgresConnectorConfig.LogicalDecoder plugin = PostgresConnectorConfig.LogicalDecoder.DECODERBUFS;
        private boolean dropSlotOnClose = DEFAULT_DROP_SLOT_ON_CLOSE;
        private Integer statusUpdateIntervalMillis;

        protected ReplicationConnectionBuilder(Configuration config) {
            assert config != null;
            this.config = config;
        }

        @Override
        public ReplicationConnectionBuilder withSlot(final String slotName) {
            assert slotName != null;
            this.slotName = slotName;
            return this;
        }

        @Override
        public ReplicationConnectionBuilder withPlugin(final PostgresConnectorConfig.LogicalDecoder plugin) {
            assert plugin != null;
            this.plugin = plugin;
            return this;
        }

        @Override
        public ReplicationConnectionBuilder dropSlotOnClose(final boolean dropSlotOnClose) {
            this.dropSlotOnClose = dropSlotOnClose;
            return this;
        }

        @Override
        public ReplicationConnectionBuilder statusUpdateIntervalMillis(final Integer statusUpdateIntervalMillis) {
            this.statusUpdateIntervalMillis = statusUpdateIntervalMillis;
            return this;
        }

        @Override
        public ReplicationConnection build() {
            assert plugin != null : "Decoding plugin name is not set";
            return new PostgresReplicationConnection(config, slotName, plugin, dropSlotOnClose, statusUpdateIntervalMillis);
        }
    }
}
