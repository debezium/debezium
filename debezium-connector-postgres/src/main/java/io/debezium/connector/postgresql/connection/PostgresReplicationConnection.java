/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql.connection;

import static java.lang.Math.toIntExact;

import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.time.Duration;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.apache.kafka.connect.errors.ConnectException;
import org.postgresql.core.BaseConnection;
import org.postgresql.core.ServerVersion;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;
import org.postgresql.util.PSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresSchema;
import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.connector.postgresql.spi.SlotCreationResult;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.jdbc.JdbcConnectionException;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;

/**
 * Implementation of a {@link ReplicationConnection} for Postgresql. Note that replication connections in PG cannot execute
 * regular statements but only a limited number of replication-related commands.
 *
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public class PostgresReplicationConnection extends JdbcConnection implements ReplicationConnection {

    private static Logger LOGGER = LoggerFactory.getLogger(PostgresReplicationConnection.class);

    private final String slotName;
    private final String publicationName;
    private final PostgresConnectorConfig.LogicalDecoder plugin;
    private final boolean dropSlotOnClose;
    private final boolean exportSnapshot;
    private final Configuration originalConfig;
    private final Duration statusUpdateInterval;
    private final MessageDecoder messageDecoder;
    private final TypeRegistry typeRegistry;
    private final Properties streamParams;

    private long defaultStartingPos;
    private SlotCreationResult slotCreationInfo;
    private boolean hasInitedSlot;

    /**
     * Creates a new replication connection with the given params.
     *
     * @param config the JDBC configuration for the connection; may not be null
     * @param slotName the name of the DB slot for logical replication; may not be null
     * @param publicationName the name of the DB publication for logical replication; may not be null
     * @param plugin decoder matching the server side plug-in used for streaming changes; may not be null
     * @param dropSlotOnClose whether the replication slot should be dropped once the connection is closed
     * @param statusUpdateInterval the interval at which the replication connection should periodically send status
     * @param exportSnapshot whether the replication should export a snapshot when created
     * @param typeRegistry registry with PostgreSQL types
     * @param streamParams additional parameters to pass to the replication stream
     * @param schema the schema; must not be null
     *
     * updates to the server
     */
    private PostgresReplicationConnection(Configuration config,
                                          String slotName,
                                          String publicationName,
                                          PostgresConnectorConfig.LogicalDecoder plugin,
                                          boolean dropSlotOnClose,
                                          boolean exportSnapshot,
                                          Duration statusUpdateInterval,
                                          TypeRegistry typeRegistry,
                                          Properties streamParams,
                                          PostgresSchema schema) {
        super(config, PostgresConnection.FACTORY, null, PostgresReplicationConnection::defaultSettings);

        this.originalConfig = config;
        this.slotName = slotName;
        this.publicationName = publicationName;
        this.plugin = plugin;
        this.dropSlotOnClose = dropSlotOnClose;
        this.statusUpdateInterval = statusUpdateInterval;
        this.exportSnapshot = exportSnapshot;
        this.messageDecoder = plugin.messageDecoder(new MessageDecoderConfig(config, schema, publicationName));
        this.typeRegistry = typeRegistry;
        this.streamParams = streamParams;
        this.slotCreationInfo = null;
        this.hasInitedSlot = false;
    }

    private ServerInfo.ReplicationSlot getSlotInfo() throws SQLException, InterruptedException {
        try (PostgresConnection connection = new PostgresConnection(originalConfig)) {
            return connection.readReplicationSlotInfo(slotName, plugin.getPostgresPluginName());
        }
    }

    protected void initPublication() {
        if (PostgresConnectorConfig.LogicalDecoder.PGOUTPUT.equals(plugin)) {
            LOGGER.info("Initializing PgOutput logical decoder publication");
            try {
                String selectPublication = String.format("SELECT COUNT(1) FROM pg_publication WHERE pubname = '%s'", publicationName);
                try (Statement stmt = pgConnection().createStatement(); ResultSet rs = stmt.executeQuery(selectPublication)) {
                    if (rs.next()) {
                        Long count = rs.getLong(1);
                        if (count == 0L) {
                            LOGGER.info("Creating new publication '{}' for plugin '{}'", publicationName, plugin);
                            // Publication doesn't exist, create it.
                            // todo: DBZ-766 - Change this to be restricted based on configured whitelist tables?
                            // For situations where no publication exists, we likely cannot create it for all tables.
                            // This is because postgres requires certain super user permissions to use "ALL TABLES".
                            // We should restrict this to the configured tables here.
                            stmt.execute(String.format("CREATE PUBLICATION %s FOR ALL TABLES;", publicationName));
                        }
                        else {
                            LOGGER.trace(
                                    "A logical publication named '{}' for plugin '{}' and database '{}' is already active on the server " +
                                            "and will be used by the plugin",
                                    publicationName, plugin, database());
                        }
                    }
                }
            }
            catch (SQLException e) {
                throw new JdbcConnectionException(e);
            }

            // This is what ties the publication definition to the replication stream
            streamParams.put("proto_version", 1);
            streamParams.put("publication_names", publicationName);
        }
    }

    protected void initReplicationSlot() throws SQLException, InterruptedException {
        ServerInfo.ReplicationSlot slotInfo = getSlotInfo();

        boolean shouldCreateSlot = ServerInfo.ReplicationSlot.INVALID == slotInfo;
        try {
            // there's no info for this plugin and slot so create a new slot
            if (shouldCreateSlot) {
                this.createReplicationSlot();
            }

            AtomicLong xlogStart = new AtomicLong();
            // replication connection does not support parsing of SQL statements so we need to create
            // the connection without executing on connect statements - see JDBC opt preferQueryMode=simple
            pgConnection();
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

            if (slotCreationInfo != null) {
                this.defaultStartingPos = slotCreationInfo.startLsn();
            }
            else if (shouldCreateSlot || !slotInfo.hasValidFlushedLsn()) {
                // this is a new slot or we weren't able to read a valid flush LSN pos, so we always start from the xlog pos that was reported
                this.defaultStartingPos = xlogStart.get();
            }
            else {
                Long latestFlushedLsn = slotInfo.latestFlushedLsn();
                this.defaultStartingPos = latestFlushedLsn < xlogStart.get() ? latestFlushedLsn : xlogStart.get();
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("found previous flushed LSN '{}'", ReplicationConnection.format(latestFlushedLsn));
                }
            }
            hasInitedSlot = true;
        }
        catch (SQLException e) {
            throw new JdbcConnectionException(e);
        }
    }

    // Temporary replication slots is a new feature of PostgreSQL 10
    private boolean useTemporarySlot() throws SQLException {
        return dropSlotOnClose && pgConnection().haveMinimumServerVersion(ServerVersion.v10);
    }

    /**
     * creating a replication connection and starting to stream involves a few steps:
     * 1. we create the connection and ensure that
     * a. the slot exists
     * b. the slot isn't currently being used
     * 2. we query to get our potential start position in the slot (lsn)
     * 3. we try and start streaming, depending on our options (such as in wal2json)
     *    this may fail, which can result in the connection being killed and we need to start
     *    the process over if we are using a temporary slot
     * 4. actually start the streamer
     *
     * This method takes care of all of these and this method queries for a default starting position
     * If you know where you are starting from you should call {@link #startStreaming(Long)}, this method
     * delegates to that method
     * @return
     * @throws SQLException
     * @throws InterruptedException
     */
    @Override
    public ReplicationStream startStreaming() throws SQLException, InterruptedException {
        return startStreaming(null);
    }

    @Override
    public ReplicationStream startStreaming(Long offset) throws SQLException, InterruptedException {
        boolean skipFirstFlushRecord = true;
        initConnection();

        connect();
        if (offset == null || offset <= 0) {
            offset = defaultStartingPos;
            skipFirstFlushRecord = false;
        }
        LogSequenceNumber lsn = LogSequenceNumber.valueOf(offset);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("starting streaming from LSN '{}'", lsn.asString());
        }

        try {
            return createReplicationStream(lsn, skipFirstFlushRecord);
        }
        catch (Exception e) {
            throw new ConnectException("Failed to start replication stream at " + lsn, e);
        }
    }

    @Override
    public void initConnection() throws SQLException, InterruptedException {
        // See https://www.postgresql.org/docs/current/logical-replication-quick-setup.html
        // For pgoutput specifically, the publication must be created before the slot.
        initPublication();
        if (!hasInitedSlot) {
            initReplicationSlot();
        }
    }

    @Override
    public Optional<SlotCreationResult> createReplicationSlot() throws SQLException {
        // note that some of these options are only supported in pg94+, additionally
        // the options are not yet exported by the jdbc api wrapper, therefore, we just do this ourselves
        // but eventually this should be moved back to the jdbc API
        // see https://github.com/pgjdbc/pgjdbc/issues/1305

        LOGGER.debug("Creating new replication slot '{}' for plugin '{}'", slotName, plugin);
        String tempPart = "";
        // Exported snapshots are supported in PostgreSQL 9.4+
        Boolean canExportSnapshot = pgConnection().haveMinimumServerVersion(ServerVersion.v9_4);
        if ((dropSlotOnClose || exportSnapshot) && !canExportSnapshot) {
            LOGGER.warn("A slot marked as temporary or with an exported snapshot was created, " +
                    "but not on a supported version of Postgres, ignoring!");
        }
        if (useTemporarySlot()) {
            tempPart = "TEMPORARY";
        }

        // See https://www.postgresql.org/docs/current/logical-replication-quick-setup.html
        // For pgoutput specifically, the publication must be created prior to the slot.
        initPublication();

        try (Statement stmt = pgConnection().createStatement()) {
            String createCommand = String.format(
                    "CREATE_REPLICATION_SLOT %s %s LOGICAL %s",
                    slotName,
                    tempPart,
                    plugin.getPostgresPluginName());
            LOGGER.info("Creating replication slot with command {}", createCommand);
            stmt.execute(createCommand);
            // when we are in pg94+, we can parse the slot creation info, otherwise, it returns
            // nothing
            if (canExportSnapshot) {
                this.slotCreationInfo = parseSlotCreation(stmt.getResultSet());
            }

            return Optional.ofNullable(slotCreationInfo);
        }
    }

    protected BaseConnection pgConnection() throws SQLException {
        return (BaseConnection) connection(false);
    }

    private SlotCreationResult parseSlotCreation(ResultSet rs) {
        try {
            if (rs.next()) {
                String slotName = rs.getString("slot_name");
                String startPoint = rs.getString("consistent_point");
                String snapName = rs.getString("snapshot_name");
                String pluginName = rs.getString("output_plugin");

                return new SlotCreationResult(slotName, startPoint, snapName, pluginName);
            }
            else {
                throw new ConnectException("expected response to create_replication_slot");
            }
        }
        catch (SQLException ex) {
            throw new ConnectException("unable to parse create_replication_slot", ex);
        }
    }

    private ReplicationStream createReplicationStream(final LogSequenceNumber startLsn, boolean skipFirstFlushRecord) throws SQLException, InterruptedException {
        PGReplicationStream s;

        try {
            try {
                s = startPgReplicationStream(startLsn,
                        plugin.forceRds()
                                ? messageDecoder::optionsWithoutMetadata
                                : messageDecoder::optionsWithMetadata);
                messageDecoder.setContainsMetadata(plugin.forceRds() ? false : true);
            }
            catch (PSQLException e) {
                LOGGER.debug("Could not register for streaming, retrying without optional options", e);

                // re-init the slot after a failed start of slot, as this
                // may have closed the slot
                if (useTemporarySlot()) {
                    initReplicationSlot();
                }

                s = startPgReplicationStream(startLsn, plugin.forceRds() ? messageDecoder::optionsWithoutMetadata : messageDecoder::optionsWithMetadata);
                messageDecoder.setContainsMetadata(plugin.forceRds() ? false : true);
            }
        }
        catch (PSQLException e) {
            if (e.getMessage().matches("(?s)ERROR: option .* is unknown.*")) {
                // It is possible we are connecting to an old wal2json plug-in
                LOGGER.warn("Could not register for streaming with metadata in messages, falling back to messages without metadata");

                // re-init the slot after a failed start of slot, as this
                // may have closed the slot
                if (useTemporarySlot()) {
                    initReplicationSlot();
                }

                s = startPgReplicationStream(startLsn, messageDecoder::optionsWithoutMetadata);
                messageDecoder.setContainsMetadata(false);
            }
            else if (e.getMessage().matches("(?s)ERROR: requested WAL segment .* has already been removed.*")) {
                LOGGER.error("Cannot rewind to last processed WAL position", e);
                throw new ConnectException(
                        "The offset to start reading from has been removed from the database write-ahead log. Create a new snapshot and consider setting of PostgreSQL parameter wal_keep_segments = 0.");
            }
            else {
                throw e;
            }
        }

        final PGReplicationStream stream = s;
        final long startingLsn = startLsn.asLong();

        return new ReplicationStream() {

            private static final int CHECK_WARNINGS_AFTER_COUNT = 100;
            private int warningCheckCounter = CHECK_WARNINGS_AFTER_COUNT;
            private ExecutorService keepAliveExecutor = null;
            private AtomicBoolean keepAliveRunning;
            private final Metronome metronome = Metronome.sleeper(statusUpdateInterval, Clock.SYSTEM);

            // make sure this is volatile since multiple threads may be interested in this value
            private volatile LogSequenceNumber lastReceivedLsn;

            @Override
            public void read(ReplicationMessageProcessor processor) throws SQLException, InterruptedException {
                ByteBuffer read = stream.read();
                final long lastReceiveLsn = stream.getLastReceiveLSN().asLong();
                LOGGER.trace("Streaming requested from LSN {}, received LSN {}", startingLsn, lastReceiveLsn);
                if (messageDecoder.shouldMessageBeSkipped(read, lastReceiveLsn, startingLsn, skipFirstFlushRecord)) {
                    return;
                }
                deserializeMessages(read, processor);
            }

            @Override
            public boolean readPending(ReplicationMessageProcessor processor) throws SQLException, InterruptedException {
                ByteBuffer read = stream.readPending();
                final long lastReceiveLsn = stream.getLastReceiveLSN().asLong();
                LOGGER.trace("Streaming requested from LSN {}, received LSN {}", startingLsn, lastReceiveLsn);

                if (read == null || messageDecoder.shouldMessageBeSkipped(read, lastReceiveLsn, startingLsn, skipFirstFlushRecord)) {
                    return false;
                }

                deserializeMessages(read, processor);

                return true;
            }

            private void deserializeMessages(ByteBuffer buffer, ReplicationMessageProcessor processor) throws SQLException, InterruptedException {
                lastReceivedLsn = stream.getLastReceiveLSN();
                messageDecoder.processMessage(buffer, processor, typeRegistry);
            }

            @Override
            public void close() throws SQLException {
                processWarnings(true);
                stream.close();
            }

            @Override
            public void flushLsn(long lsn) throws SQLException {
                doFlushLsn(LogSequenceNumber.valueOf(lsn));
            }

            private void doFlushLsn(LogSequenceNumber lsn) throws SQLException {
                stream.setFlushedLSN(lsn);
                stream.setAppliedLSN(lsn);

                stream.forceUpdateStatus();
            }

            @Override
            public Long lastReceivedLsn() {
                return lastReceivedLsn != null ? lastReceivedLsn.asLong() : null;
            }

            @Override
            public void startKeepAlive(ExecutorService service) {
                if (keepAliveExecutor == null) {
                    keepAliveExecutor = service;
                    keepAliveRunning = new AtomicBoolean(true);
                    keepAliveExecutor.submit(() -> {
                        while (keepAliveRunning.get()) {
                            try {
                                LOGGER.trace("Forcing status update with replication stream");
                                stream.forceUpdateStatus();

                                metronome.pause();
                            }
                            catch (Exception exp) {
                                throw new RuntimeException("received unexpected exception will perform keep alive", exp);
                            }
                        }
                    });
                }
            }

            @Override
            public void stopKeepAlive() {
                if (keepAliveExecutor != null) {
                    keepAliveRunning.set(false);
                    keepAliveExecutor.shutdownNow();
                    keepAliveExecutor = null;
                }
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

            @Override
            public Long startLsn() {
                return startingLsn;
            }
        };
    }

    private PGReplicationStream startPgReplicationStream(final LogSequenceNumber lsn, Function<ChainedLogicalStreamBuilder, ChainedLogicalStreamBuilder> configurator)
            throws SQLException {
        assert lsn != null;
        ChainedLogicalStreamBuilder streamBuilder = pgConnection()
                .getReplicationAPI()
                .replicationStream()
                .logical()
                .withSlotName(slotName)
                .withStartPosition(lsn)
                .withSlotOptions(streamParams);
        streamBuilder = configurator.apply(streamBuilder);

        if (statusUpdateInterval != null && statusUpdateInterval.toMillis() > 0) {
            streamBuilder.withStatusInterval(toIntExact(statusUpdateInterval.toMillis()), TimeUnit.MILLISECONDS);
        }

        PGReplicationStream stream = streamBuilder.start();

        // TODO DBZ-508 get rid of this
        // Needed by tests when connections are opened and closed in a fast sequence
        try {
            Thread.sleep(10);
        }
        catch (Exception e) {
        }
        stream.forceUpdateStatus();
        return stream;
    }

    @Override
    public synchronized void close() {
        try {
            LOGGER.debug("Closing replication connection");
            super.close();
        }
        catch (Throwable e) {
            LOGGER.error("Unexpected error while closing Postgres connection", e);
        }
        if (dropSlotOnClose) {
            // we're dropping the replication slot via a regular - i.e. not a replication - connection
            try (PostgresConnection connection = new PostgresConnection(originalConfig)) {
                connection.dropReplicationSlot(slotName);
            }
            catch (Throwable e) {
                LOGGER.error("Unexpected error while dropping replication slot", e);
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
        private String publicationName = DEFAULT_PUBLICATION_NAME;
        private PostgresConnectorConfig.LogicalDecoder plugin = PostgresConnectorConfig.LogicalDecoder.DECODERBUFS;
        private boolean dropSlotOnClose = DEFAULT_DROP_SLOT_ON_CLOSE;
        private Duration statusUpdateIntervalVal;
        private boolean exportSnapshot = DEFAULT_EXPORT_SNAPSHOT;
        private TypeRegistry typeRegistry;
        private PostgresSchema schema;
        private Properties slotStreamParams = new Properties();

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
        public Builder withPublication(String publicationName) {
            assert publicationName != null;
            this.publicationName = publicationName;
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
        public ReplicationConnectionBuilder streamParams(final String slotStreamParams) {
            if (slotStreamParams != null && !slotStreamParams.isEmpty()) {
                this.slotStreamParams = new Properties();
                String[] paramsWithValues = slotStreamParams.split(";");
                for (String paramsWithValue : paramsWithValues) {
                    String[] paramAndValue = paramsWithValue.split("=");
                    if (paramAndValue.length == 2) {
                        this.slotStreamParams.setProperty(paramAndValue[0], paramAndValue[1]);
                    }
                    else {
                        LOGGER.warn("The following STREAM_PARAMS value is invalid: {}", paramsWithValue);
                    }
                }
            }
            return this;
        }

        @Override
        public ReplicationConnectionBuilder statusUpdateInterval(final Duration statusUpdateInterval) {
            this.statusUpdateIntervalVal = statusUpdateInterval;
            return this;
        }

        @Override
        public Builder exportSnapshotOnCreate(boolean exportSnapshot) {
            this.exportSnapshot = exportSnapshot;
            return this;
        }

        @Override
        public ReplicationConnection build() {
            assert plugin != null : "Decoding plugin name is not set";
            return new PostgresReplicationConnection(config, slotName, publicationName, plugin, dropSlotOnClose, exportSnapshot,
                    statusUpdateIntervalVal, typeRegistry, slotStreamParams, schema);
        }

        @Override
        public Builder withTypeRegistry(TypeRegistry typeRegistry) {
            this.typeRegistry = typeRegistry;
            return this;
        }

        @Override
        public Builder withSchema(PostgresSchema schema) {
            this.schema = schema;
            return this;
        }
    }
}
