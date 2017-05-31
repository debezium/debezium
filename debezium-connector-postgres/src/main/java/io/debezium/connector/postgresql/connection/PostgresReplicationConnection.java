/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql.connection;

import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.postgresql.PGConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.protobuf.InvalidProtocolBufferException;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.proto.PgProto;
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
    private final String pluginName;
    private final boolean dropSlotOnClose;
    private final Configuration originalConfig;
    private final int statusUpdateIntervalSeconds;
    
    private long defaultStartingPos;
    
    /**
     * Creates a new replication connection with the given params.
     * 
     * @param config the JDBC configuration for the connection; may not be null
     * @param slotName the name of the DB slot for logical replication; may not be null
     * @param pluginName the name of the server side plugin used for streaming changes; may not be null;
     * @param dropSlotOnClose whether the replication slot should be dropped once the connection is closed
     * @param statusUpdateIntervalSeconds the number of seconds at which the replication connection should periodically send status
     * updates to the server
     */
    private PostgresReplicationConnection(Configuration config,
                                         String slotName,
                                         String pluginName,
                                         boolean dropSlotOnClose,
                                         int statusUpdateIntervalSeconds) {
        super(config, PostgresConnection.FACTORY, null ,PostgresReplicationConnection::defaultSettings);
        
        this.originalConfig = config;
        this.slotName = slotName;
        this.pluginName = pluginName;
        this.dropSlotOnClose = dropSlotOnClose;
        this.statusUpdateIntervalSeconds = statusUpdateIntervalSeconds;
    
        try {
            initReplicationSlot();
        } catch (SQLException e) {
            throw new JdbcConnectionException("Cannot create replication connection", e);
        }
    }
    
    protected void initReplicationSlot() throws SQLException {
        ServerInfo.ReplicationSlot slotInfo;
        try (PostgresConnection connection = new PostgresConnection(originalConfig)) {
            slotInfo = connection.readReplicationSlotInfo(slotName, pluginName);
        }
        
        boolean shouldCreateSlot = ServerInfo.ReplicationSlot.INVALID == slotInfo;
        try {
            if (shouldCreateSlot) {
                LOGGER.debug("Creating new replication slot '{}' for plugin '{}'", slotName, pluginName);
                // there's no info for this plugin and slot so create a new slot
                pgConnection().getReplicationAPI()
                              .createReplicationSlot()
                              .logical()
                              .withSlotName(slotName)
                              .withOutputPlugin(pluginName)
                              .make();
            } else if (slotInfo.active()) {
                LOGGER.error(
                        "A logical replication slot named '{}' for plugin '{}' and database '{}' is already active on the server." +
                        "You cannot have multiple slots with the same name active for the same database",
                        slotName, pluginName, database());
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
        if (statusUpdateIntervalSeconds > 0) {
            streamBuilder.withStatusInterval(statusUpdateIntervalSeconds, TimeUnit.SECONDS);
        }
        PGReplicationStream stream = streamBuilder.start();
        final long lsnLong = lsn.asLong(); 
        return new ReplicationStream() {
            // make sure this is volatile since multiple threads may be interested in this value
            private volatile LogSequenceNumber lastReceivedLSN;
    
            @Override
            public PgProto.RowMessage read() throws SQLException {
                ByteBuffer read = stream.read();
                // the lsn we started from is inclusive, so we need to avoid sending back the same message twice
                if (lsnLong >= stream.getLastReceiveLSN().asLong()) {
                    return null;
                }
                return deserializeMessage(read);
            }
    
            @Override
            public PgProto.RowMessage readPending() throws SQLException {
                ByteBuffer read = stream.readPending();
                // the lsn we started from is inclusive, so we need to avoid sending back the same message twice
                if (read == null ||  lsnLong >= stream.getLastReceiveLSN().asLong()) {
                    return null;
                }
                return deserializeMessage(read);
            }
    
            private PgProto.RowMessage deserializeMessage(ByteBuffer buffer) {
                try {
                    if (!buffer.hasArray()) {
                        throw new IllegalStateException(
                                "Invalid buffer received from PG server during streaming replication");
                    }
                    byte[] source = buffer.array();
                    byte[] content = Arrays.copyOfRange(source, buffer.arrayOffset(), source.length);
                    lastReceivedLSN = stream.getLastReceiveLSN();
                    return PgProto.RowMessage.parseFrom(content);
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                }
            }
    
            @Override
            public void close() throws SQLException {
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
    
        private Configuration config;
        private String slotName = DEFAULT_SLOT_NAME;
        private String pluginName = DEFAULT_PLUGIN_NAME;
        private boolean dropSlotOnClose = DEFAULT_DROP_SLOT_ON_CLOSE;
        private int statusUpdateIntervalSeconds = DEFAULT_STATUS_UPDATE_SECONDS;
    
        protected ReplicationConnectionBuilder(Configuration config) {
            assert config != null;
            this.config = config;
        }
    
        public ReplicationConnectionBuilder withSlot(final String slotName) {
            assert slotName != null;
            this.slotName = slotName;
            return this;
        }
    
        public ReplicationConnectionBuilder withPlugin(final String pluginName) {
            assert pluginName != null;
            this.pluginName = pluginName;
            return this;
        }
    
        public ReplicationConnectionBuilder dropSlotOnClose(final boolean dropSlotOnClose) {
            this.dropSlotOnClose = dropSlotOnClose;
            return this;
        }
    
        public ReplicationConnectionBuilder statusUpdateIntervalSeconds(final int statusUpdateIntervalSeconds) {
            this.statusUpdateIntervalSeconds = statusUpdateIntervalSeconds;
            return this;
        }
        
        public ReplicationConnection build() {
            return new PostgresReplicationConnection(config, slotName, pluginName, dropSlotOnClose, statusUpdateIntervalSeconds);
        }
    }
}
