/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.ConfigDefinition;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.config.Field.ValidationOutput;
import io.debezium.config.Instantiator;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.connector.oracle.logminer.logwriter.LogWriterFlushStrategy;
import io.debezium.connector.oracle.logminer.processor.LogMinerEventProcessor;
import io.debezium.connector.oracle.logminer.processor.infinispan.EmbeddedInfinispanLogMinerEventProcessor;
import io.debezium.connector.oracle.logminer.processor.infinispan.RemoteInfinispanLogMinerEventProcessor;
import io.debezium.connector.oracle.logminer.processor.memory.MemoryLogMinerEventProcessor;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.relational.ColumnFilterMode;
import io.debezium.relational.HistorizedRelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.util.Strings;

/**
 * Connector configuration for Oracle.
 *
 * @author Gunnar Morling
 */
public class OracleConnectorConfig extends HistorizedRelationalDatabaseConnectorConfig {

    protected static final int DEFAULT_PORT = 1528;

    protected static final int DEFAULT_VIEW_FETCH_SIZE = 10_000;

    protected final static int DEFAULT_BATCH_SIZE = 20_000;
    protected final static int MIN_BATCH_SIZE = 1_000;
    protected final static int MAX_BATCH_SIZE = 100_000;

    protected final static int DEFAULT_SCN_GAP_SIZE = 1_000_000;
    protected final static int DEFAULT_SCN_GAP_TIME_INTERVAL = 20_000;

    protected final static Duration MAX_SLEEP_TIME = Duration.ofMillis(3_000);
    protected final static Duration DEFAULT_SLEEP_TIME = Duration.ofMillis(1_000);
    protected final static Duration MIN_SLEEP_TIME = Duration.ZERO;
    protected final static Duration SLEEP_TIME_INCREMENT = Duration.ofMillis(200);

    protected final static Duration ARCHIVE_LOG_ONLY_POLL_TIME = Duration.ofMillis(10_000);

    public static final Field PORT = RelationalDatabaseConnectorConfig.PORT
            .withDefault(DEFAULT_PORT);

    public static final Field HOSTNAME = RelationalDatabaseConnectorConfig.HOSTNAME
            .withNoValidation()
            .withValidation(OracleConnectorConfig::requiredWhenNoUrl);

    public static final Field PDB_NAME = Field.create(DATABASE_CONFIG_PREFIX + "pdb.name")
            .withDisplayName("PDB name")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.HIGH)
            .withDescription("Name of the pluggable database when working with a multi-tenant set-up. "
                    + "The CDB name must be given via " + DATABASE_NAME.name() + " in this case.");

    public static final Field XSTREAM_SERVER_NAME = Field.create(DATABASE_CONFIG_PREFIX + "out.server.name")
            .withDisplayName("XStream out server name")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.HIGH)
            .withValidation(OracleConnectorConfig::validateOutServerName)
            .withDescription("Name of the XStream Out server to connect to.");

    public static final Field SNAPSHOT_MODE = Field.create("snapshot.mode")
            .withDisplayName("Snapshot mode")
            .withEnum(SnapshotMode.class, SnapshotMode.INITIAL)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("The criteria for running a snapshot upon startup of the connector. "
                    + "Options include: "
                    + "'initial' (the default) to specify the connector should run a snapshot only when no offsets are available for the logical server name; "
                    + "'schema_only' to specify the connector should run a snapshot of the schema when no offsets are available for the logical server name. ");

    public static final Field SNAPSHOT_LOCKING_MODE = Field.create("snapshot.locking.mode")
            .withDisplayName("Snapshot locking mode")
            .withEnum(SnapshotLockingMode.class, SnapshotLockingMode.SHARED)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("Controls how the connector holds locks on tables while performing the schema snapshot. The default is 'shared', "
                    + "which means the connector will hold a table lock that prevents exclusive table access for just the initial portion of the snapshot "
                    + "while the database schemas and other metadata are being read. The remaining work in a snapshot involves selecting all rows from "
                    + "each table, and this is done using a flashback query that requires no locks. However, in some cases it may be desirable to avoid "
                    + "locks entirely which can be done by specifying 'none'. This mode is only safe to use if no schema changes are happening while the "
                    + "snapshot is taken.");

    public static final Field ORACLE_VERSION = Field.createInternal("database.oracle.version")
            .withDisplayName("Oracle version, 11 or 12+")
            .withType(Type.STRING)
            .withImportance(Importance.LOW)
            .withDescription("Deprecated: For default Oracle 12+, use default pos_version value v2, for Oracle 11, use pos_version value v1.");

    public static final Field SERVER_NAME = RelationalDatabaseConnectorConfig.SERVER_NAME
            .withValidation(CommonConnectorConfig::validateServerNameIsDifferentFromHistoryTopicName);

    public static final Field CONNECTOR_ADAPTER = Field.create(DATABASE_CONFIG_PREFIX + "connection.adapter")
            .withDisplayName("Connector adapter")
            .withEnum(ConnectorAdapter.class, ConnectorAdapter.LOG_MINER)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.HIGH)
            .withDescription("The adapter to use when capturing changes from the database. "
                    + "Options include: "
                    + "'logminer': (the default) to capture changes using native Oracle LogMiner; "
                    + "'xstream' to capture changes using Oracle XStreams");

    public static final Field LOG_MINING_STRATEGY = Field.create("log.mining.strategy")
            .withDisplayName("Log Mining Strategy")
            .withEnum(LogMiningStrategy.class, LogMiningStrategy.CATALOG_IN_REDO)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.HIGH)
            .withDescription("There are strategies: Online catalog with faster mining but no captured DDL. Another - with data dictionary loaded into REDO LOG files");

    // this option could be true up to Oracle 18c version. Starting from Oracle 19c this option cannot be true todo should we do it?
    public static final Field CONTINUOUS_MINE = Field.create("log.mining.continuous.mine")
            .withDisplayName("Should log mining session configured with CONTINUOUS_MINE setting?")
            .withType(Type.BOOLEAN)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(false)
            .withValidation(Field::isBoolean)
            .withDescription("If true, CONTINUOUS_MINE option will be added to the log mining session. This will manage log files switches seamlessly.");

    public static final Field SNAPSHOT_ENHANCEMENT_TOKEN = Field.create("snapshot.enhance.predicate.scn")
            .withDisplayName("A string to replace on snapshot predicate enhancement")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.HIGH)
            .withDescription("A token to replace on snapshot predicate template");

    public static final Field LOG_MINING_TRANSACTION_RETENTION = Field.create("log.mining.transaction.retention.hours")
            .withDisplayName("Log Mining long running transaction retention")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDefault(0)
            .withValidation(Field::isNonNegativeInteger)
            .withDescription("Hours to keep long running transactions in transaction buffer between log mining sessions.  By default, all transactions are retained.");

    public static final Field RAC_NODES = Field.create("rac.nodes")
            .withDisplayName("Oracle RAC nodes")
            .withType(Type.STRING)
            .withWidth(Width.SHORT)
            .withImportance(Importance.HIGH)
            .withValidation(OracleConnectorConfig::validateRacNodes)
            .withDescription("A comma-separated list of RAC node hostnames or ip addresses");

    public static final Field URL = Field.create(DATABASE_CONFIG_PREFIX + "url")
            .withDisplayName("Complete JDBC URL")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .withValidation(OracleConnectorConfig::requiredWhenNoHostname)
            .withDescription("Complete JDBC URL as an alternative to specifying hostname, port and database provided "
                    + "as a way to support alternative connection scenarios.");

    public static final Field LOG_MINING_ARCHIVE_LOG_HOURS = Field.create("log.mining.archive.log.hours")
            .withDisplayName("Log Mining Archive Log Hours")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(0)
            .withDescription("The number of hours in the past from SYSDATE to mine archive logs.  Using 0 mines all available archive logs");

    public static final Field LOG_MINING_BATCH_SIZE_MIN = Field.create("log.mining.batch.size.min")
            .withDisplayName("Minimum batch size for reading redo/archive logs.")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(MIN_BATCH_SIZE)
            .withDescription(
                    "The minimum SCN interval size that this connector will try to read from redo/archive logs. Active batch size will be also increased/decreased by this amount for tuning connector throughput when needed.");

    public static final Field LOG_MINING_BATCH_SIZE_DEFAULT = Field.create("log.mining.batch.size.default")
            .withDisplayName("Default batch size for reading redo/archive logs.")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(DEFAULT_BATCH_SIZE)
            .withDescription("The starting SCN interval size that the connector will use for reading data from redo/archive logs.");

    public static final Field LOG_MINING_BATCH_SIZE_MAX = Field.create("log.mining.batch.size.max")
            .withDisplayName("Maximum batch size for reading redo/archive logs.")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(MAX_BATCH_SIZE)
            .withDescription("The maximum SCN interval size that this connector will use when reading from redo/archive logs.");

    public static final Field LOG_MINING_VIEW_FETCH_SIZE = Field.create("log.mining.view.fetch.size")
            .withDisplayName("Number of content records that will be fetched.")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(DEFAULT_VIEW_FETCH_SIZE)
            .withDescription("The number of content records that will be fetched from the LogMiner content view.");

    public static final Field LOG_MINING_SLEEP_TIME_MIN_MS = Field.create("log.mining.sleep.time.min.ms")
            .withDisplayName("Minimum sleep time in milliseconds when reading redo/archive logs.")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(MIN_SLEEP_TIME.toMillis())
            .withDescription(
                    "The minimum amount of time that the connector will sleep after reading data from redo/archive logs and before starting reading data again. Value is in milliseconds.");

    public static final Field LOG_MINING_SLEEP_TIME_DEFAULT_MS = Field.create("log.mining.sleep.time.default.ms")
            .withDisplayName("Default sleep time in milliseconds when reading redo/archive logs.")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(DEFAULT_SLEEP_TIME.toMillis())
            .withDescription(
                    "The amount of time that the connector will sleep after reading data from redo/archive logs and before starting reading data again. Value is in milliseconds.");

    public static final Field LOG_MINING_SLEEP_TIME_MAX_MS = Field.create("log.mining.sleep.time.max.ms")
            .withDisplayName("Maximum sleep time in milliseconds when reading redo/archive logs.")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(MAX_SLEEP_TIME.toMillis())
            .withDescription(
                    "The maximum amount of time that the connector will sleep after reading data from redo/archive logs and before starting reading data again. Value is in milliseconds.");

    public static final Field LOG_MINING_SLEEP_TIME_INCREMENT_MS = Field.create("log.mining.sleep.time.increment.ms")
            .withDisplayName("The increment in sleep time in milliseconds used to tune auto-sleep behavior.")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(SLEEP_TIME_INCREMENT.toMillis())
            .withDescription(
                    "The maximum amount of time that the connector will use to tune the optimal sleep time when reading data from LogMiner. Value is in milliseconds.");

    public static final Field LOG_MINING_ARCHIVE_LOG_ONLY_MODE = Field.create("log.mining.archive.log.only.mode")
            .withDisplayName("Specifies whether log mining should only target archive logs or both archive and redo logs")
            .withType(Type.BOOLEAN)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(false)
            .withDescription("When set to `false`, the default, the connector will mine both archive log and redo logs to emit change events. " +
                    "When set to `true`, the connector will only mine archive logs. There are circumstances where its advantageous to only " +
                    "mine archive logs and accept latency in event emission due to frequent revolving redo logs.");

    public static final Field LOG_MINING_ARCHIVE_LOG_ONLY_SCN_POLL_INTERVAL_MS = Field.create("log.mining.archive.log.only.scn.poll.interval.ms")
            .withDisplayName("The interval in milliseconds to wait between polls when SCN is not yet in the archive logs")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(ARCHIVE_LOG_ONLY_POLL_TIME.toMillis())
            .withDescription("The interval in milliseconds to wait between polls checking to see if the SCN is in the archive logs.");

    public static final Field LOB_ENABLED = Field.create("lob.enabled")
            .withDisplayName("Specifies whether the connector supports mining LOB fields and operations")
            .withType(Type.BOOLEAN)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(false)
            .withDescription("When set to `false`, the default, LOB fields will not be captured nor emitted. When set to `true`, the connector " +
                    "will capture LOB fields and emit changes for those fields like any other column type.");

    public static final Field LOG_MINING_USERNAME_EXCLUDE_LIST = Field.create("log.mining.username.exclude.list")
            .withDisplayName("List of users to exclude from LogMiner query")
            .withType(Type.STRING)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("Comma separated list of usernames to exclude from LogMiner query.");

    public static final Field LOG_MINING_ARCHIVE_DESTINATION_NAME = Field.create("log.mining.archive.destination.name")
            .withDisplayName("Name of the archive log destination to be used for reading archive logs")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withDescription("Sets the specific archive log destination as the source for reading archive logs." +
                    "When not set, the connector will automatically select the first LOCAL and VALID destination.");

    public static final Field LOG_MINING_BUFFER_TYPE = Field.create("log.mining.buffer.type")
            .withDisplayName("Controls which buffer type implementation to be used")
            .withEnum(LogMiningBufferType.class, LogMiningBufferType.MEMORY)
            .withValidation(OracleConnectorConfig::validateLogMiningBufferType)
            .withImportance(Importance.LOW)
            .withDescription("The buffer type controls how the connector manages buffering transaction data." + System.lineSeparator() +
                    System.lineSeparator() +
                    "memory - Uses the JVM process' heap to buffer all transaction data." + System.lineSeparator() +
                    System.lineSeparator() +
                    "infinispan_embedded - This option uses an embedded Infinispan cache to buffer transaction data and persist it to disk." + System.lineSeparator() +
                    System.lineSeparator() +
                    "infinispan_remote - This option uses a remote Infinispan cluster to buffer transaction data and persist it to disk.");

    @Deprecated
    public static final Field LOG_MINING_BUFFER_LOCATION = Field.create("log.mining.buffer.location")
            .withDisplayName("Location where Infinispan stores buffer caches")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withValidation(OracleConnectorConfig::validateBufferLocation)
            .withDescription("(Deprecated) Path to location where Infinispan will store buffer caches");

    public static final Field LOG_MINING_BUFFER_INFINISPAN_CACHE_TRANSACTIONS = Field.create("log.mining.buffer.infinispan.cache.transactions")
            .withDisplayName("Infinispan 'transactions' cache configuration")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withValidation(OracleConnectorConfig::validateLogMiningInfinispanCacheConfiguration)
            .withDescription("Specifies the XML configuration for the Infinispan 'transactions' cache");

    public static final Field LOG_MINING_BUFFER_INFINISPAN_CACHE_PROCESSED_TRANSACTIONS = Field.create("log.mining.buffer.infinispan.cache.processed_transactions")
            .withDisplayName("Infinispan 'processed-transactions' cache configuration")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withValidation(OracleConnectorConfig::validateLogMiningInfinispanCacheConfiguration)
            .withDescription("Specifies the XML configuration for the Infinispan 'processed-transactions' cache");

    public static final Field LOG_MINING_BUFFER_INFINISPAN_CACHE_EVENTS = Field.create("log.mining.buffer.infinispan.cache.events")
            .withDisplayName("Infinispan 'events' cache configurations")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withValidation(OracleConnectorConfig::validateLogMiningInfinispanCacheConfiguration)
            .withDescription("Specifies the XML configuration for the Infinispan 'events' cache");

    public static final Field LOG_MINING_BUFFER_INFINISPAN_CACHE_SCHEMA_CHANGES = Field.create("log.mining.buffer.infinispan.cache.schema_changes")
            .withDisplayName("Infinispan 'schema-changes' cache configuration")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withValidation(OracleConnectorConfig::validateLogMiningInfinispanCacheConfiguration)
            .withDescription("Specifies the XML configuration for the Infinispan 'schema-changes' cache");

    public static final Field LOG_MINING_BUFFER_DROP_ON_STOP = Field.create("log.mining.buffer.drop.on.stop")
            .withDisplayName("Controls whether the buffer cache is dropped when connector is stopped")
            .withType(Type.BOOLEAN)
            .withDefault(false)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("When set to true the underlying buffer cache is not retained when the connector is stopped. " +
                    "When set to false (the default), the buffer cache is retained across restarts.");

    public static final Field LOG_MINING_SCN_GAP_DETECTION_GAP_SIZE_MIN = Field.create("log.mining.scn.gap.detection.gap.size.min")
            .withDisplayName("SCN gap size used to detect SCN gap")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(DEFAULT_SCN_GAP_SIZE)
            .withDescription("Used for SCN gap detection, if the difference between current SCN and previous end SCN is " +
                    "bigger than this value, and the time difference of current SCN and previous end SCN is smaller than " +
                    "log.mining.scn.gap.detection.time.interval.max.ms, consider it a SCN gap.");

    public static final Field LOG_MINING_SCN_GAP_DETECTION_TIME_INTERVAL_MAX_MS = Field.create("log.mining.scn.gap.detection.time.interval.max.ms")
            .withDisplayName("Timer interval used to detect SCN gap")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(DEFAULT_SCN_GAP_TIME_INTERVAL)
            .withDescription("Used for SCN gap detection, if the difference between current SCN and previous end SCN is " +
                    "bigger than log.mining.scn.gap.detection.gap.size.min, and the time difference of current SCN and previous end SCN is smaller than " +
                    " this value, consider it a SCN gap.");

    private static final ConfigDefinition CONFIG_DEFINITION = HistorizedRelationalDatabaseConnectorConfig.CONFIG_DEFINITION.edit()
            .name("Oracle")
            .excluding(
                    SCHEMA_WHITELIST,
                    SCHEMA_INCLUDE_LIST,
                    SCHEMA_BLACKLIST,
                    SCHEMA_EXCLUDE_LIST,
                    RelationalDatabaseConnectorConfig.TABLE_IGNORE_BUILTIN,
                    SERVER_NAME)
            .type(
                    HOSTNAME,
                    PORT,
                    USER,
                    PASSWORD,
                    SERVER_NAME,
                    DATABASE_NAME,
                    PDB_NAME,
                    XSTREAM_SERVER_NAME,
                    SNAPSHOT_MODE,
                    CONNECTOR_ADAPTER,
                    LOG_MINING_STRATEGY,
                    URL,
                    ORACLE_VERSION)
            .connector(
                    SNAPSHOT_ENHANCEMENT_TOKEN,
                    SNAPSHOT_LOCKING_MODE,
                    RAC_NODES,
                    LOG_MINING_ARCHIVE_LOG_HOURS,
                    LOG_MINING_BATCH_SIZE_DEFAULT,
                    LOG_MINING_BATCH_SIZE_MIN,
                    LOG_MINING_BATCH_SIZE_MAX,
                    LOG_MINING_SLEEP_TIME_DEFAULT_MS,
                    LOG_MINING_SLEEP_TIME_MIN_MS,
                    LOG_MINING_SLEEP_TIME_MAX_MS,
                    LOG_MINING_SLEEP_TIME_INCREMENT_MS,
                    LOG_MINING_TRANSACTION_RETENTION,
                    LOG_MINING_ARCHIVE_LOG_ONLY_MODE,
                    LOB_ENABLED,
                    LOG_MINING_USERNAME_EXCLUDE_LIST,
                    LOG_MINING_ARCHIVE_DESTINATION_NAME,
                    LOG_MINING_BUFFER_TYPE,
                    LOG_MINING_BUFFER_LOCATION,
                    LOG_MINING_BUFFER_DROP_ON_STOP,
                    LOG_MINING_BUFFER_INFINISPAN_CACHE_TRANSACTIONS,
                    LOG_MINING_BUFFER_INFINISPAN_CACHE_EVENTS,
                    LOG_MINING_BUFFER_INFINISPAN_CACHE_PROCESSED_TRANSACTIONS,
                    LOG_MINING_BUFFER_INFINISPAN_CACHE_SCHEMA_CHANGES,
                    LOG_MINING_ARCHIVE_LOG_ONLY_SCN_POLL_INTERVAL_MS,
                    LOG_MINING_SCN_GAP_DETECTION_GAP_SIZE_MIN,
                    LOG_MINING_SCN_GAP_DETECTION_TIME_INTERVAL_MAX_MS,
                    UNAVAILABLE_VALUE_PLACEHOLDER)
            .create();

    /**
     * The set of {@link Field}s defined as part of this configuration.
     */
    public static Field.Set ALL_FIELDS = Field.setOf(CONFIG_DEFINITION.all());

    public static ConfigDef configDef() {
        return CONFIG_DEFINITION.configDef();
    }

    public static final List<String> EXCLUDED_SCHEMAS = Collections.unmodifiableList(Arrays.asList("appqossys", "audsys",
            "ctxsys", "dvsys", "dbsfwuser", "dbsnmp", "gsmadmin_internal", "lbacsys", "mdsys", "ojvmsys", "olapsys",
            "orddata", "ordsys", "outln", "sys", "system", "wmsys", "xdb"));

    private static final Logger LOGGER = LoggerFactory.getLogger(OracleConnectorConfig.class);

    private final String databaseName;
    private final String pdbName;
    private final String xoutServerName;
    private final SnapshotMode snapshotMode;

    private final String oracleVersion;
    private ConnectorAdapter connectorAdapter;
    private final StreamingAdapter streamingAdapter;
    private final String snapshotEnhancementToken;
    private final SnapshotLockingMode snapshotLockingMode;

    // LogMiner options
    private final LogMiningStrategy logMiningStrategy;
    private final Set<String> racNodes;
    private final boolean logMiningContinuousMine;
    private final Duration logMiningArchiveLogRetention;
    private final int logMiningBatchSizeMin;
    private final int logMiningBatchSizeMax;
    private final int logMiningBatchSizeDefault;
    private final int logMiningViewFetchSize;
    private final Duration logMiningSleepTimeMin;
    private final Duration logMiningSleepTimeMax;
    private final Duration logMiningSleepTimeDefault;
    private final Duration logMiningSleepTimeIncrement;
    private final Duration logMiningTransactionRetention;
    private final boolean archiveLogOnlyMode;
    private final Duration archiveLogOnlyScnPollTime;
    private final boolean lobEnabled;
    private final Set<String> logMiningUsernameExcludes;
    private final String logMiningArchiveDestinationName;
    private final LogMiningBufferType logMiningBufferType;
    private final boolean logMiningBufferDropOnStop;
    private final int logMiningScnGapDetectionGapSizeMin;
    private final int logMiningScnGapDetectionTimeIntervalMaxMs;

    public OracleConnectorConfig(Configuration config) {
        super(OracleConnector.class, config, config.getString(SERVER_NAME), new SystemTablesPredicate(config), x -> x.schema() + "." + x.table(), true,
                ColumnFilterMode.SCHEMA);

        this.databaseName = toUpperCase(config.getString(DATABASE_NAME));
        this.pdbName = toUpperCase(config.getString(PDB_NAME));
        this.xoutServerName = config.getString(XSTREAM_SERVER_NAME);
        this.snapshotMode = SnapshotMode.parse(config.getString(SNAPSHOT_MODE));
        this.oracleVersion = config.getString(ORACLE_VERSION);
        this.snapshotEnhancementToken = config.getString(SNAPSHOT_ENHANCEMENT_TOKEN);
        this.connectorAdapter = ConnectorAdapter.parse(config.getString(CONNECTOR_ADAPTER));
        this.snapshotLockingMode = SnapshotLockingMode.parse(config.getString(SNAPSHOT_LOCKING_MODE), SNAPSHOT_LOCKING_MODE.defaultValueAsString());
        this.lobEnabled = config.getBoolean(LOB_ENABLED);

        this.streamingAdapter = this.connectorAdapter.getInstance(this);
        if (this.streamingAdapter == null) {
            throw new DebeziumException("Unable to instantiate the connector adapter implementation");
        }

        // LogMiner
        this.logMiningStrategy = LogMiningStrategy.parse(config.getString(LOG_MINING_STRATEGY));
        this.racNodes = resolveRacNodes(config);
        this.logMiningContinuousMine = config.getBoolean(CONTINUOUS_MINE);
        this.logMiningArchiveLogRetention = Duration.ofHours(config.getLong(LOG_MINING_ARCHIVE_LOG_HOURS));
        this.logMiningBatchSizeMin = config.getInteger(LOG_MINING_BATCH_SIZE_MIN);
        this.logMiningBatchSizeMax = config.getInteger(LOG_MINING_BATCH_SIZE_MAX);
        this.logMiningBatchSizeDefault = config.getInteger(LOG_MINING_BATCH_SIZE_DEFAULT);
        this.logMiningViewFetchSize = config.getInteger(LOG_MINING_VIEW_FETCH_SIZE);
        this.logMiningSleepTimeMin = Duration.ofMillis(config.getInteger(LOG_MINING_SLEEP_TIME_MIN_MS));
        this.logMiningSleepTimeMax = Duration.ofMillis(config.getInteger(LOG_MINING_SLEEP_TIME_MAX_MS));
        this.logMiningSleepTimeDefault = Duration.ofMillis(config.getInteger(LOG_MINING_SLEEP_TIME_DEFAULT_MS));
        this.logMiningSleepTimeIncrement = Duration.ofMillis(config.getInteger(LOG_MINING_SLEEP_TIME_INCREMENT_MS));
        this.logMiningTransactionRetention = Duration.ofHours(config.getInteger(LOG_MINING_TRANSACTION_RETENTION));
        this.archiveLogOnlyMode = config.getBoolean(LOG_MINING_ARCHIVE_LOG_ONLY_MODE);
        this.logMiningUsernameExcludes = Strings.setOf(config.getString(LOG_MINING_USERNAME_EXCLUDE_LIST), String::new);
        this.logMiningArchiveDestinationName = config.getString(LOG_MINING_ARCHIVE_DESTINATION_NAME);
        this.logMiningBufferType = LogMiningBufferType.parse(config.getString(LOG_MINING_BUFFER_TYPE));
        this.logMiningBufferDropOnStop = config.getBoolean(LOG_MINING_BUFFER_DROP_ON_STOP);
        this.archiveLogOnlyScnPollTime = Duration.ofMillis(config.getInteger(LOG_MINING_ARCHIVE_LOG_ONLY_SCN_POLL_INTERVAL_MS));
        this.logMiningScnGapDetectionGapSizeMin = config.getInteger(LOG_MINING_SCN_GAP_DETECTION_GAP_SIZE_MIN);
        this.logMiningScnGapDetectionTimeIntervalMaxMs = config.getInteger(LOG_MINING_SCN_GAP_DETECTION_TIME_INTERVAL_MAX_MS);
    }

    private static String toUpperCase(String property) {
        return property == null ? null : property.toUpperCase();
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getPdbName() {
        return pdbName;
    }

    public String getCatalogName() {
        return pdbName != null ? pdbName : databaseName;
    }

    public String getXoutServerName() {
        return xoutServerName;
    }

    public SnapshotMode getSnapshotMode() {
        return snapshotMode;
    }

    public SnapshotLockingMode getSnapshotLockingMode() {
        return snapshotLockingMode;
    }

    public String getOracleVersion() {
        return oracleVersion;
    }

    @Override
    protected HistoryRecordComparator getHistoryRecordComparator() {
        return getAdapter().getHistoryRecordComparator();
    }

    /**
     * The set of predefined SnapshotMode options or aliases.
     */
    public enum SnapshotMode implements EnumeratedValue {

        /**
         * Perform a snapshot of data and schema upon initial startup of a connector.
         */
        INITIAL("initial", true, false),

        /**
         * Perform a snapshot of the schema but no data upon initial startup of a connector.
         */
        SCHEMA_ONLY("schema_only", false, false),

        /**
         * Perform a snapshot of only the database schemas (without data) and then begin reading the redo log at the current redo log position.
         * This can be used for recovery only if the connector has existing offsets and the database.history.kafka.topic does not exist (deleted).
         * This recovery option should be used with care as it assumes there have been no schema changes since the connector last stopped,
         * otherwise some events during the gap may be processed with an incorrect schema and corrupted.
         */
        SCHEMA_ONLY_RECOVERY("schema_only_recovery", false, true);

        private final String value;
        private final boolean includeData;
        private final boolean shouldSnapshotOnSchemaError;

        private SnapshotMode(String value, boolean includeData, boolean shouldSnapshotOnSchemaError) {
            this.value = value;
            this.includeData = includeData;
            this.shouldSnapshotOnSchemaError = shouldSnapshotOnSchemaError;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Whether this snapshotting mode should include the actual data or just the
         * schema of captured tables.
         */
        public boolean includeData() {
            return includeData;
        }

        /**
         * Whether the schema can be recovered if database history is corrupted.
         */
        public boolean shouldSnapshotOnSchemaError() {
            return shouldSnapshotOnSchemaError;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static SnapshotMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();

            for (SnapshotMode option : SnapshotMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }

            return null;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @param defaultValue the default value; may be null
         * @return the matching option, or null if no match is found and the non-null default is invalid
         */
        public static SnapshotMode parse(String value, String defaultValue) {
            SnapshotMode mode = parse(value);

            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }

            return mode;
        }
    }

    public enum SnapshotLockingMode implements EnumeratedValue {
        /**
         * This mode will allow concurrent access to the table during the snapshot but prevents any
         * session from acquiring any table-level exclusive lock.
         */
        SHARED("shared"),

        /**
         * This mode will avoid using ANY table locks during the snapshot process.
         * This mode should be used carefully only when no schema changes are to occur.
         */
        NONE("none");

        private final String value;

        private SnapshotLockingMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        public boolean usesLocking() {
            return !value.equals(NONE.value);
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be {@code null}
         * @return the matching option, or null if no match is found
         */
        public static SnapshotLockingMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (SnapshotLockingMode option : SnapshotLockingMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be {@code null}
         * @param defaultValue the default value; may be {@code null}
         * @return the matching option, or null if no match is found and the non-null default is invalid
         */
        public static SnapshotLockingMode parse(String value, String defaultValue) {
            SnapshotLockingMode mode = parse(value);
            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }
            return mode;
        }
    }

    public enum ConnectorAdapter implements EnumeratedValue {

        /**
         * This is based on XStream API.
         */
        XSTREAM("XStream") {
            @Override
            public String getConnectionUrl() {
                return "jdbc:oracle:oci:@${" + JdbcConfiguration.HOSTNAME + "}:${" + JdbcConfiguration.PORT + "}/${" + JdbcConfiguration.DATABASE + "}";
            }

            @Override
            public StreamingAdapter getInstance(OracleConnectorConfig connectorConfig) {
                return Instantiator.getInstanceWithProvidedConstructorType(
                        "io.debezium.connector.oracle.xstream.XStreamAdapter",
                        StreamingAdapter.class::getClassLoader,
                        OracleConnectorConfig.class,
                        connectorConfig);
            }
        },

        /**
         * This is based on LogMiner utility.
         */
        LOG_MINER("LogMiner") {
            @Override
            public String getConnectionUrl() {
                return "jdbc:oracle:thin:@${" + JdbcConfiguration.HOSTNAME + "}:${" + JdbcConfiguration.PORT + "}/${" + JdbcConfiguration.DATABASE + "}";
            }

            @Override
            public StreamingAdapter getInstance(OracleConnectorConfig connectorConfig) {
                return Instantiator.getInstanceWithProvidedConstructorType(
                        "io.debezium.connector.oracle.logminer.LogMinerAdapter",
                        StreamingAdapter.class::getClassLoader,
                        OracleConnectorConfig.class,
                        connectorConfig);
            }
        };

        public abstract String getConnectionUrl();

        public abstract StreamingAdapter getInstance(OracleConnectorConfig connectorConfig);

        private final String value;

        ConnectorAdapter(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static ConnectorAdapter parse(String value) {
            if (value == null) {
                return ConnectorAdapter.LOG_MINER;
            }
            value = value.trim();
            for (ConnectorAdapter adapter : ConnectorAdapter.values()) {
                if (adapter.getValue().equalsIgnoreCase(value)) {
                    return adapter;
                }
            }
            return null;
        }

        public static ConnectorAdapter parse(String value, String defaultValue) {
            ConnectorAdapter mode = parse(value);

            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }

            return mode;
        }
    }

    public enum LogMiningStrategy implements EnumeratedValue {

        /**
         * This strategy uses LogMiner with data dictionary in online catalog.
         * This option will not capture DDL , but acts fast on REDO LOG switch events
         * This option does not use CONTINUOUS_MINE option
         */
        ONLINE_CATALOG("online_catalog"),

        /**
         * This strategy uses LogMiner with data dictionary in REDO LOG files.
         * This option will capture DDL, but will develop some lag on REDO LOG switch event and will eventually catch up
         * This option does not use CONTINUOUS_MINE option
         * This is default value
         */
        CATALOG_IN_REDO("redo_log_catalog");

        private final String value;

        LogMiningStrategy(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static LogMiningStrategy parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (LogMiningStrategy adapter : LogMiningStrategy.values()) {
                if (adapter.getValue().equalsIgnoreCase(value)) {
                    return adapter;
                }
            }
            return null;
        }

        public static LogMiningStrategy parse(String value, String defaultValue) {
            LogMiningStrategy mode = parse(value);

            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }

            return mode;
        }
    }

    public enum LogMiningBufferType implements EnumeratedValue {
        MEMORY("memory") {
            @Override
            public LogMinerEventProcessor createProcessor(ChangeEventSourceContext context,
                                                          OracleConnectorConfig connectorConfig,
                                                          OracleConnection connection,
                                                          EventDispatcher<TableId> dispatcher,
                                                          OraclePartition partition,
                                                          OracleOffsetContext offsetContext,
                                                          OracleDatabaseSchema schema,
                                                          OracleStreamingChangeEventSourceMetrics metrics) {
                return new MemoryLogMinerEventProcessor(context, connectorConfig, connection, dispatcher, partition,
                        offsetContext, schema, metrics);
            }
        },

        /**
         * @deprecated use either {@link #INFINISPAN_EMBEDDED} or {@link #INFINISPAN_REMOTE}.
         */
        @Deprecated
        INFINISPAN("infinispan") {
            @Override
            public LogMinerEventProcessor createProcessor(ChangeEventSourceContext context,
                                                          OracleConnectorConfig connectorConfig,
                                                          OracleConnection connection,
                                                          EventDispatcher<TableId> dispatcher,
                                                          OraclePartition partition,
                                                          OracleOffsetContext offsetContext,
                                                          OracleDatabaseSchema schema,
                                                          OracleStreamingChangeEventSourceMetrics metrics) {
                return new EmbeddedInfinispanLogMinerEventProcessor(context, connectorConfig, connection, dispatcher,
                        partition, offsetContext, schema, metrics);
            }
        },

        INFINISPAN_EMBEDDED("infinispan_embedded") {
            @Override
            public LogMinerEventProcessor createProcessor(ChangeEventSourceContext context,
                                                          OracleConnectorConfig connectorConfig,
                                                          OracleConnection connection,
                                                          EventDispatcher<TableId> dispatcher,
                                                          OraclePartition partition,
                                                          OracleOffsetContext offsetContext,
                                                          OracleDatabaseSchema schema,
                                                          OracleStreamingChangeEventSourceMetrics metrics) {
                return new EmbeddedInfinispanLogMinerEventProcessor(context, connectorConfig, connection, dispatcher,
                        partition, offsetContext, schema, metrics);
            }
        },

        INFINISPAN_REMOTE("infinispan_remote") {
            @Override
            public LogMinerEventProcessor createProcessor(ChangeEventSourceContext context,
                                                          OracleConnectorConfig connectorConfig,
                                                          OracleConnection connection,
                                                          EventDispatcher<TableId> dispatcher,
                                                          OraclePartition partition,
                                                          OracleOffsetContext offsetContext,
                                                          OracleDatabaseSchema schema,
                                                          OracleStreamingChangeEventSourceMetrics metrics) {
                return new RemoteInfinispanLogMinerEventProcessor(context, connectorConfig, connection, dispatcher,
                        partition, offsetContext, schema, metrics);
            }
        };

        private final String value;

        /**
         * Creates the buffer type's specific processor implementation
         */
        public abstract LogMinerEventProcessor createProcessor(ChangeEventSourceContext context, OracleConnectorConfig connectorConfig,
                                                               OracleConnection connection, EventDispatcher<TableId> dispatcher, OraclePartition partition,
                                                               OracleOffsetContext offsetContext, OracleDatabaseSchema schema,
                                                               OracleStreamingChangeEventSourceMetrics metrics);

        LogMiningBufferType(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        public boolean isInfinispan() {
            return !MEMORY.equals(this);
        }

        public boolean isInfinispanEmbedded() {
            return isInfinispan() && (INFINISPAN.equals(this) || INFINISPAN_EMBEDDED.equals(this));
        }

        public static LogMiningBufferType parse(String value) {
            if (value != null) {
                value = value.trim();
                for (LogMiningBufferType option : LogMiningBufferType.values()) {
                    if (option.getValue().equalsIgnoreCase(value)) {
                        return option;
                    }
                }
            }
            return null;
        }

        private static LogMiningBufferType parse(String value, String defaultValue) {
            LogMiningBufferType type = parse(value);
            if (type == null && defaultValue != null) {
                type = parse(defaultValue);
            }
            return type;
        }
    }

    /**
     * A {@link TableFilter} that excludes all Oracle system tables.
     *
     * @author Gunnar Morling
     */
    private static class SystemTablesPredicate implements TableFilter {

        private final Configuration config;

        SystemTablesPredicate(Configuration config) {
            this.config = config;
        }

        @Override
        public boolean isIncluded(TableId t) {
            return !isExcludedSchema(t) && !isFlushTable(t);
        }

        private boolean isExcludedSchema(TableId id) {
            return EXCLUDED_SCHEMAS.contains(id.schema().toLowerCase());
        }

        private boolean isFlushTable(TableId id) {
            return LogWriterFlushStrategy.isFlushTable(id, config.getString(USER));
        }
    }

    @Override
    protected SourceInfoStructMaker<? extends AbstractSourceInfo> getSourceInfoStructMaker(Version version) {
        return new OracleSourceInfoStructMaker(Module.name(), Module.version(), this);
    }

    @Override
    public String getContextName() {
        return Module.contextName();
    }

    /**
     * @return the streaming adapter implementation
     */
    public StreamingAdapter getAdapter() {
        return streamingAdapter;
    }

    /**
     * @return Log Mining strategy
     */
    public LogMiningStrategy getLogMiningStrategy() {
        return logMiningStrategy;
    }

    /**
     * @return whether Oracle is using RAC
     */
    public Boolean isRacSystem() {
        return !racNodes.isEmpty();
    }

    /**
     * @return set of node hosts or ip addresses used in Oracle RAC
     */
    public Set<String> getRacNodes() {
        return racNodes;
    }

    /**
     * @return String token to replace
     */
    public String getTokenToReplaceInSnapshotPredicate() {
        return snapshotEnhancementToken;
    }

    /**
     * @return whether continuous log mining is enabled
     */
    public boolean isContinuousMining() {
        return logMiningContinuousMine;
    }

    /**
     * @return the duration that archive logs are scanned for log mining
     */
    public Duration getLogMiningArchiveLogRetention() {
        return logMiningArchiveLogRetention;
    }

    /**
     *
     * @return int The minimum SCN interval used when mining redo/archive logs
     */
    public int getLogMiningBatchSizeMin() {
        return logMiningBatchSizeMin;
    }

    /**
     *
     * @return int Number of actual records that will be fetched from the log mining contents view
     */
    public int getLogMiningViewFetchSize() {
        return logMiningViewFetchSize;
    }

    /**
     *
     * @return int The maximum SCN interval used when mining redo/archive logs
     */
    public int getLogMiningBatchSizeMax() {
        return logMiningBatchSizeMax;
    }

    /**
     *
     * @return int Scn gap size for SCN gap detection
     */
    public int getLogMiningScnGapDetectionGapSizeMin() {
        return logMiningScnGapDetectionGapSizeMin;
    }

    /**
     *
     * @return int Time interval for SCN gap detection
     */
    public int getLogMiningScnGapDetectionTimeIntervalMaxMs() {
        return logMiningScnGapDetectionTimeIntervalMaxMs;
    }

    /**
     *
     * @return int The minimum sleep time used when mining redo/archive logs
     */
    public Duration getLogMiningSleepTimeMin() {
        return logMiningSleepTimeMin;
    }

    /**
     *
     * @return int The maximum sleep time used when mining redo/archive logs
     */
    public Duration getLogMiningSleepTimeMax() {
        return logMiningSleepTimeMax;
    }

    /**
     *
     * @return int The default sleep time used when mining redo/archive logs
     */
    public Duration getLogMiningSleepTimeDefault() {
        return logMiningSleepTimeDefault;
    }

    /**
     *
     * @return int The increment in sleep time when doing auto-tuning while mining redo/archive logs
     */
    public Duration getLogMiningSleepTimeIncrement() {
        return logMiningSleepTimeIncrement;
    }

    /**
     * @return the duration for which long running transactions are permitted in the transaction buffer between log switches
     */
    public Duration getLogMiningTransactionRetention() {
        return logMiningTransactionRetention;
    }

    /**
     * @return true if the connector is to mine archive logs only, false to mine all logs.
     */
    public boolean isArchiveLogOnlyMode() {
        return archiveLogOnlyMode;
    }

    /**
     * @return the duration that archive log only will use to wait between polling scn availability
     */
    public Duration getArchiveLogOnlyScnPollTime() {
        return archiveLogOnlyScnPollTime;
    }

    /**
     * @return true if LOB fields are to be captured; false otherwise to not capture LOB fields.
     */
    public boolean isLobEnabled() {
        return lobEnabled;
    }

    /**
     * @return User names to exclude from the LogMiner query
     */
    public Set<String> getLogMiningUsernameExcludes() {
        return logMiningUsernameExcludes;
    }

    /**
     * @return name of the archive destination configuration to use
     */
    public String getLogMiningArchiveDestinationName() {
        return logMiningArchiveDestinationName;
    }

    /**
     * @return the log mining buffer type implementation to be used
     */
    public LogMiningBufferType getLogMiningBufferType() {
        return logMiningBufferType;
    }

    /**
     * @return whether buffer cache should be dropped on connector stop.
     */
    public boolean isLogMiningBufferDropOnStop() {
        return logMiningBufferDropOnStop;
    }

    /**
     *
     * @return int The default SCN interval used when mining redo/archive logs
     */
    public int getLogMiningBatchSizeDefault() {
        return logMiningBatchSizeDefault;
    }

    @Override
    public String getConnectorName() {
        return Module.name();
    }

    private Set<String> resolveRacNodes(Configuration config) {
        final boolean portProvided = config.hasKey(PORT.name());
        final Set<String> nodes = Strings.setOf(config.getString(RAC_NODES), String::new);
        return nodes.stream().map(node -> {
            if (portProvided && !node.contains(":")) {
                return node + ":" + config.getInteger(PORT);
            }
            else {
                if (!portProvided && !node.contains(":")) {
                    throw new DebeziumException("RAC node '" + node + "' must specify a port.");
                }
                return node;
            }
        }).collect(Collectors.toSet());
    }

    public static int validateOutServerName(Configuration config, Field field, ValidationOutput problems) {
        if (ConnectorAdapter.XSTREAM.equals(ConnectorAdapter.parse(config.getString(CONNECTOR_ADAPTER)))) {
            return Field.isRequired(config, field, problems);
        }
        return 0;
    }

    public static int requiredWhenNoUrl(Configuration config, Field field, ValidationOutput problems) {

        // Validates that the field is required but only when an URL field is not present
        if (config.getString(URL) == null) {
            return Field.isRequired(config, field, problems);
        }
        return 0;
    }

    public static int requiredWhenNoHostname(Configuration config, Field field, ValidationOutput problems) {

        // Validates that the field is required but only when an URL field is not present
        if (config.getString(HOSTNAME) == null) {
            return Field.isRequired(config, field, problems);
        }
        return 0;
    }

    public static int validateBufferLocation(Configuration config, Field field, ValidationOutput problems) {
        // Log error if this field is provided in the configuration since its no longer valid and has been removed
        final LogMiningBufferType bufferType = LogMiningBufferType.parse(config.getString(LOG_MINING_BUFFER_TYPE));
        if (bufferType.isInfinispan()) {
            final String location = config.getString(LOG_MINING_BUFFER_LOCATION);
            if (!Strings.isNullOrEmpty(location)) {
                problems.accept(field, location, "Configuration option is no longer valid, please use 'log.mining.buffer.infinispan.cache.*' options");
                return 1;
            }
        }
        return 0;
    }

    public static int validateRacNodes(Configuration config, Field field, ValidationOutput problems) {
        int errors = 0;
        if (ConnectorAdapter.LOG_MINER.equals(ConnectorAdapter.parse(config.getString(CONNECTOR_ADAPTER)))) {
            // If no "database.port" is specified, guarantee that "rac.nodes" (if not empty) specifies the
            // port designation for each comma-delimited value.
            final boolean portProvided = config.hasKey(PORT.name());
            if (!portProvided) {
                final Set<String> racNodes = Strings.setOf(config.getString(RAC_NODES), String::new);
                for (String racNode : racNodes) {
                    String[] parts = racNode.split(":");
                    if (parts.length == 1) {
                        problems.accept(field, racNode, "Must be specified as 'ip/hostname:port' since no 'database.port' is provided");
                        errors++;
                    }
                }
            }
        }
        return errors;
    }

    private static int validateLogMiningBufferType(Configuration config, Field field, ValidationOutput problems) {
        final LogMiningBufferType bufferType = LogMiningBufferType.parse(config.getString(LOG_MINING_BUFFER_TYPE));
        if (LogMiningBufferType.INFINISPAN.equals(bufferType)) {
            LOGGER.warn("Value '{}' of configuration option '{}' is deprecated and should be replaced with '{}'",
                    LogMiningBufferType.INFINISPAN.getValue(),
                    LOG_MINING_BUFFER_TYPE.name(),
                    LogMiningBufferType.INFINISPAN_EMBEDDED.getValue());
        }
        if (LogMiningBufferType.INFINISPAN_REMOTE.equals(bufferType)) {
            // Must supply the Hotrod server list property as a minimum when using Infinispan cluster mode
            final String serverList = config.getString(RemoteInfinispanLogMinerEventProcessor.HOTROD_SERVER_LIST);
            if (Strings.isNullOrEmpty(serverList)) {
                LOGGER.error("The option '{}' must be supplied when using the buffer type '{}'",
                        RemoteInfinispanLogMinerEventProcessor.HOTROD_SERVER_LIST,
                        bufferType.name());
                return 1;
            }
        }
        return 0;
    }

    public static int validateLogMiningInfinispanCacheConfiguration(Configuration config, Field field, ValidationOutput problems) {
        final LogMiningBufferType bufferType = LogMiningBufferType.parse(config.getString(LOG_MINING_BUFFER_TYPE));
        int errors = 0;
        if (bufferType.isInfinispan()) {
            errors = Field.isRequired(config, field, problems);
        }
        return errors;
    }
}
