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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

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
import io.debezium.connector.oracle.logminer.HistoryRecorder;
import io.debezium.connector.oracle.logminer.NeverHistoryRecorder;
import io.debezium.connector.oracle.logminer.SqlUtils;
import io.debezium.jdbc.JdbcConfiguration;
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

    protected final static Duration MAX_SLEEP_TIME = Duration.ofMillis(3_000);
    protected final static Duration DEFAULT_SLEEP_TIME = Duration.ofMillis(1_000);
    protected final static Duration MIN_SLEEP_TIME = Duration.ZERO;
    protected final static Duration SLEEP_TIME_INCREMENT = Duration.ofMillis(200);

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

    @Deprecated
    public static final Field LOG_MINING_HISTORY_RECORDER_CLASS = Field.createInternal("log.mining.history.recorder.class")
            .withDisplayName("Log Mining History Recorder Class")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withInvisibleRecommender()
            .withDescription("(Deprecated) Allows connector deployment to capture log mining results");

    @Deprecated
    public static final Field LOG_MINING_HISTORY_RETENTION = Field.createInternal("log.mining.history.retention.hours")
            .withDisplayName("Log Mining history retention")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDefault(0)
            .withDescription(
                    "(Deprecated) When supplying a log.mining.history.recorder.class, this option specifies the number of hours "
                            + "the recorder should keep the history.  The default, 0, indicates that no history should be retained.");

    public static final Field LOG_MINING_TRANSACTION_RETENTION = Field.create("log.mining.transaction.retention.hours")
            .withDisplayName("Log Mining long running transaction retention")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDefault(0)
            .withValidation(Field::isNonNegativeInteger)
            .withDescription("Hours to keep long running transactions in transaction buffer between log mining sessions.  By default, all transactions are retained.");

    public static final Field RAC_SYSTEM = Field.create("database.rac")
            .withDisplayName("Oracle RAC")
            .withType(Type.BOOLEAN)
            .withWidth(Width.SHORT)
            .withImportance(Importance.HIGH)
            .withDefault(false)
            .withDescription("Flag to if it is RAC system");

    public static final Field RAC_NODES = Field.create("rac.nodes")
            .withDisplayName("Oracle RAC nodes")
            .withType(Type.STRING)
            .withWidth(Width.SHORT)
            .withImportance(Importance.HIGH)
            .withDescription("A comma-separated list of RAC node hostnames or ip addresses");

    public static final Field URL = Field.create(DATABASE_CONFIG_PREFIX + "url")
            .withDisplayName("Complete JDBC URL")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .withValidation(OracleConnectorConfig::requiredWhenNoHostname)
            .withDescription("Complete JDBC URL as an alternative to specifying hostname, port and database provided "
                    + "as a way to support alternative connection scenarios.");

    public static final Field LOG_MINING_DML_PARSER = Field.createInternal("log.mining.dml.parser")
            .withDisplayName("Log Mining DML parser implementation")
            .withEnum(LogMiningDmlParser.class, LogMiningDmlParser.FAST)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("The parser implementation to use when parsing DML operations:" +
                    "'legacy': the legacy parser implementation based on JSqlParser; " +
                    "'fast': the robust parser implementation that is streamlined specifically for LogMiner redo format");

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

    public static final Field LOB_ENABLED = Field.create("lob.enabled")
            .withDisplayName("Specifies whether the connector supports mining LOB fields and operations")
            .withType(Type.BOOLEAN)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(false)
            .withDescription("When set to `false`, the default, LOB fields will not be captured nor emitted. When set to `true`, the connector " +
                    "will capture LOB fields and emit changes for those fields like any other column type.");

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
                    RAC_SYSTEM,
                    RAC_NODES,
                    LOG_MINING_HISTORY_RECORDER_CLASS,
                    LOG_MINING_HISTORY_RETENTION,
                    LOG_MINING_ARCHIVE_LOG_HOURS,
                    LOG_MINING_BATCH_SIZE_DEFAULT,
                    LOG_MINING_BATCH_SIZE_MIN,
                    LOG_MINING_BATCH_SIZE_MAX,
                    LOG_MINING_SLEEP_TIME_DEFAULT_MS,
                    LOG_MINING_SLEEP_TIME_MIN_MS,
                    LOG_MINING_SLEEP_TIME_MAX_MS,
                    LOG_MINING_SLEEP_TIME_INCREMENT_MS,
                    LOG_MINING_TRANSACTION_RETENTION,
                    LOG_MINING_DML_PARSER,
                    LOG_MINING_ARCHIVE_LOG_ONLY_MODE,
                    LOB_ENABLED)
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

    private final String databaseName;
    private final String pdbName;
    private final String xoutServerName;
    private final SnapshotMode snapshotMode;

    private final String oracleVersion;
    private final HistoryRecorder logMiningHistoryRecorder;
    private final Configuration jdbcConfig;
    private ConnectorAdapter connectorAdapter;
    private final StreamingAdapter streamingAdapter;
    private final String snapshotEnhancementToken;
    private final SnapshotLockingMode snapshotLockingMode;

    // LogMiner options
    private final LogMiningStrategy logMiningStrategy;
    private final long logMiningHistoryRetentionHours;
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
    private final LogMiningDmlParser dmlParser;
    private final boolean archiveLogOnlyMode;
    private final boolean lobEnabled;

    public OracleConnectorConfig(Configuration config) {
        super(OracleConnector.class, config, config.getString(SERVER_NAME), new SystemTablesPredicate(config), x -> x.schema() + "." + x.table(), true,
                ColumnFilterMode.SCHEMA);

        this.databaseName = toUpperCase(config.getString(DATABASE_NAME));
        this.pdbName = toUpperCase(config.getString(PDB_NAME));
        this.xoutServerName = config.getString(XSTREAM_SERVER_NAME);
        this.snapshotMode = SnapshotMode.parse(config.getString(SNAPSHOT_MODE));
        this.oracleVersion = config.getString(ORACLE_VERSION);
        this.logMiningHistoryRecorder = resolveLogMiningHistoryRecorder(config);
        this.jdbcConfig = config.subset(DATABASE_CONFIG_PREFIX, true);
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
        this.logMiningHistoryRetentionHours = config.getLong(LOG_MINING_HISTORY_RETENTION);
        this.racNodes = Strings.setOf(config.getString(RAC_NODES), String::new);
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
        this.dmlParser = LogMiningDmlParser.parse(config.getString(LOG_MINING_DML_PARSER));
        this.archiveLogOnlyMode = config.getBoolean(LOG_MINING_ARCHIVE_LOG_ONLY_MODE);
    }

    private static String toUpperCase(String property) {
        return property == null ? null : property.toUpperCase();
    }

    private static HistoryRecorder resolveLogMiningHistoryRecorder(Configuration config) {
        if (!config.hasKey(LOG_MINING_HISTORY_RECORDER_CLASS.name())) {
            return new NeverHistoryRecorder();
        }
        if (config.getLong(LOG_MINING_HISTORY_RETENTION) == 0L) {
            return new NeverHistoryRecorder();
        }
        return config.getInstance(LOG_MINING_HISTORY_RECORDER_CLASS, HistoryRecorder.class);
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
        INITIAL("initial", true),

        /**
         * Perform a snapshot of the schema but no data upon initial startup of a connector.
         */
        SCHEMA_ONLY("schema_only", false);

        private final String value;
        private final boolean includeData;

        private SnapshotMode(String value, boolean includeData) {
            this.value = value;
            this.includeData = includeData;
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

    public enum LogMiningDmlParser implements EnumeratedValue {
        LEGACY("legacy"),
        FAST("fast");

        private final String value;

        LogMiningDmlParser(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        public static LogMiningDmlParser parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (LogMiningDmlParser parser : LogMiningDmlParser.values()) {
                if (parser.getValue().equalsIgnoreCase(value)) {
                    return parser;
                }
            }
            return null;
        }

        public static LogMiningDmlParser parse(String value, String defaultValue) {
            LogMiningDmlParser mode = parse(value);
            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }
            return mode;
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
            final String schema = config.getString(USER);
            return id.table().equalsIgnoreCase(SqlUtils.LOGMNR_FLUSH_TABLE) && id.schema().equalsIgnoreCase(schema);
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
     * @return whether log mining history is recorded
     */
    public Boolean isLogMiningHistoryRecorded() {
        return logMiningHistoryRetentionHours > 0;
    }

    /**
     * @return the log mining history recorder implementation, may be null
     */
    public HistoryRecorder getLogMiningHistoryRecorder() {
        return logMiningHistoryRecorder;
    }

    /**
     * @return the number of hours log mining history is retained if history is recorded
     */
    public long getLogMinerHistoryRetentionHours() {
        return logMiningHistoryRetentionHours;
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
     * @return int The default SCN interval used when mining redo/archive logs
     */
    public int getLogMiningBatchSizeDefault() {
        return logMiningBatchSizeDefault;
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
     * @return the log mining parser implementation to be used
     */
    public LogMiningDmlParser getLogMiningDmlParser() {
        return dmlParser;
    }

    /**
     * @return true if the connector is to mine archive logs only, false to mine all logs.
     */
    public boolean isArchiveLogOnlyMode() {
        return archiveLogOnlyMode;
    }

    /**
     * @return true if LOB fields are to be captured; false otherwise to not capture LOB fields.
     */
    public boolean isLobEnabled() {
        return lobEnabled;
    }

    public Configuration jdbcConfig() {
        return jdbcConfig;
    }

    @Override
    public String getConnectorName() {
        return Module.name();
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

}
