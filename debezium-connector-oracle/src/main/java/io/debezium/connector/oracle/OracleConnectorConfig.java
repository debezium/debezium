/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.time.Duration;
import java.util.Set;
import java.util.function.Predicate;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.config.Field.ValidationOutput;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.connector.oracle.logminer.HistoryRecorder;
import io.debezium.connector.oracle.logminer.NeverHistoryRecorder;
import io.debezium.connector.oracle.xstream.LcrPosition;
import io.debezium.connector.oracle.xstream.OracleVersion;
import io.debezium.document.Document;
import io.debezium.function.Predicates;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.ColumnId;
import io.debezium.relational.HistorizedRelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.relational.history.KafkaDatabaseHistory;
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

    protected final static Duration DEFAULT_TRANSACTION_RETENTION = Duration.ofHours(4);

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

    public static final Field TABLENAME_CASE_INSENSITIVE = Field.create("database.tablename.case.insensitive")
            .withDisplayName("Case insensitive table names")
            .withType(Type.BOOLEAN)
            .withDefault(false)
            .withImportance(Importance.LOW)
            .withDescription("Case insensitive table names; set to 'true' for Oracle 11g, 'false' (default) otherwise.");

    public static final Field ORACLE_VERSION = Field.create("database.oracle.version")
            .withDisplayName("Oracle version, 11 or 12+")
            .withEnum(OracleVersion.class, OracleVersion.V12Plus)
            .withImportance(Importance.LOW)
            .withDescription("For default Oracle 12+, use default pos_version value v2, for Oracle 11, use pos_version value v1.");

    public static final Field SERVER_NAME = RelationalDatabaseConnectorConfig.SERVER_NAME
            .withValidation(CommonConnectorConfig::validateServerNameIsDifferentFromHistoryTopicName);

    public static final Field CONNECTOR_ADAPTER = Field.create(DATABASE_CONFIG_PREFIX + "connection.adapter")
            .withDisplayName("Connector adapter")
            .withEnum(ConnectorAdapter.class, ConnectorAdapter.XSTREAM)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.HIGH)
            .withDescription("There are two adapters: XStream and LogMiner.");

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

    public static final Field LOG_MINING_HISTORY_RECORDER_CLASS = Field.create("log.mining.history.recorder.class")
            .withDisplayName("Log Mining History Recorder Class")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withInvisibleRecommender()
            .withDescription("Allows connector deployment to capture log mining results");

    public static final Field LOG_MINING_HISTORY_RETENTION = Field.create("database.history.retention.hours")
            .withDisplayName("Log Mining history retention")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDefault(0)
            .withDescription("Hours to keep Log Mining history.  By default, no history is retained.");

    public static final Field LOG_MINING_TRANSACTION_RETENTION = Field.create("log.mining.transaction.retention.hours")
            .withDisplayName("Log Mining long running transaction retention")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDefault(DEFAULT_TRANSACTION_RETENTION.toHours())
            .withValidation(OracleConnectorConfig::isPositiveNonZeroInteger)
            .withDescription("Hours to keep long running transactions in transaction buffer between log mining sessions.");

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

    /**
     * The set of {@link Field}s defined as part of this configuration.
     */
    public static Field.Set ALL_FIELDS = Field.setOf(
            HOSTNAME,
            PORT,
            RelationalDatabaseConnectorConfig.USER,
            RelationalDatabaseConnectorConfig.PASSWORD,
            SERVER_NAME,
            RelationalDatabaseConnectorConfig.DATABASE_NAME,
            PDB_NAME,
            XSTREAM_SERVER_NAME,
            SNAPSHOT_MODE,
            HistorizedRelationalDatabaseConnectorConfig.DATABASE_HISTORY,
            RelationalDatabaseConnectorConfig.TABLE_WHITELIST,
            RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST,
            RelationalDatabaseConnectorConfig.TABLE_BLACKLIST,
            RelationalDatabaseConnectorConfig.TABLE_EXCLUDE_LIST,
            RelationalDatabaseConnectorConfig.TABLE_IGNORE_BUILTIN,
            RelationalDatabaseConnectorConfig.MSG_KEY_COLUMNS,
            CommonConnectorConfig.POLL_INTERVAL_MS,
            CommonConnectorConfig.MAX_BATCH_SIZE,
            CommonConnectorConfig.MAX_QUEUE_SIZE,
            CommonConnectorConfig.SNAPSHOT_DELAY_MS,
            CommonConnectorConfig.SNAPSHOT_FETCH_SIZE,
            CommonConnectorConfig.PROVIDE_TRANSACTION_METADATA,
            Heartbeat.HEARTBEAT_INTERVAL,
            Heartbeat.HEARTBEAT_TOPICS_PREFIX,
            TABLENAME_CASE_INSENSITIVE,
            ORACLE_VERSION,
            CONNECTOR_ADAPTER,
            LOG_MINING_STRATEGY,
            SNAPSHOT_ENHANCEMENT_TOKEN,
            LOG_MINING_HISTORY_RECORDER_CLASS,
            LOG_MINING_HISTORY_RETENTION,
            RAC_SYSTEM,
            RAC_NODES,
            CommonConnectorConfig.EVENT_PROCESSING_FAILURE_HANDLING_MODE,
            URL,
            LOG_MINING_ARCHIVE_LOG_HOURS,
            LOG_MINING_BATCH_SIZE_DEFAULT,
            LOG_MINING_BATCH_SIZE_MIN,
            LOG_MINING_BATCH_SIZE_MAX,
            LOG_MINING_SLEEP_TIME_DEFAULT_MS,
            LOG_MINING_SLEEP_TIME_MIN_MS,
            LOG_MINING_SLEEP_TIME_MAX_MS,
            LOG_MINING_SLEEP_TIME_INCREMENT_MS,
            LOG_MINING_DML_PARSER);

    private final String databaseName;
    private final String pdbName;
    private final String xoutServerName;
    private final SnapshotMode snapshotMode;

    private final boolean tablenameCaseInsensitive;
    private final OracleVersion oracleVersion;
    private final Tables.ColumnNameFilter columnFilter;
    private final HistoryRecorder logMiningHistoryRecorder;
    private final Configuration jdbcConfig;
    private final ConnectorAdapter connectorAdapter;
    private final String snapshotEnhancementToken;

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

    public OracleConnectorConfig(Configuration config) {
        super(OracleConnector.class, config, config.getString(SERVER_NAME), new SystemTablesPredicate(), x -> x.schema() + "." + x.table(), true);

        this.databaseName = toUpperCase(config.getString(DATABASE_NAME));
        this.pdbName = toUpperCase(config.getString(PDB_NAME));
        this.xoutServerName = config.getString(XSTREAM_SERVER_NAME);
        this.snapshotMode = SnapshotMode.parse(config.getString(SNAPSHOT_MODE));
        this.tablenameCaseInsensitive = config.getBoolean(TABLENAME_CASE_INSENSITIVE);
        this.oracleVersion = OracleVersion.parse(config.getString(ORACLE_VERSION));
        String blacklistedColumns = toUpperCase(config.getString(RelationalDatabaseConnectorConfig.COLUMN_BLACKLIST));
        this.columnFilter = getColumnNameFilter(blacklistedColumns);
        this.logMiningHistoryRecorder = resolveLogMiningHistoryRecorder(config);
        this.jdbcConfig = config.subset(DATABASE_CONFIG_PREFIX, true);
        this.snapshotEnhancementToken = config.getString(SNAPSHOT_ENHANCEMENT_TOKEN);

        // LogMiner
        this.connectorAdapter = ConnectorAdapter.parse(config.getString(CONNECTOR_ADAPTER));
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
    }

    private static String toUpperCase(String property) {
        return property == null ? null : property.toUpperCase();
    }

    private static HistoryRecorder resolveLogMiningHistoryRecorder(Configuration config) {
        if (!config.hasKey(LOG_MINING_HISTORY_RECORDER_CLASS.name())) {
            return new NeverHistoryRecorder();
        }
        return config.getInstance(LOG_MINING_HISTORY_RECORDER_CLASS, HistoryRecorder.class);
    }

    protected Tables.ColumnNameFilter getColumnNameFilter(String excludedColumnPatterns) {
        return new Tables.ColumnNameFilter() {

            Predicate<ColumnId> delegate = Predicates.excludes(excludedColumnPatterns, ColumnId::toString);

            @Override
            public boolean matches(String catalogName, String schemaName, String tableName, String columnName) {
                // ignore database name and schema name, we are supposed to capture from one database and one schema
                return delegate.test(new ColumnId(new TableId(null, null, tableName), columnName));
            }
        };
    }

    public static ConfigDef configDef() {
        ConfigDef config = new ConfigDef();

        Field.group(config, "Oracle", HOSTNAME, PORT, RelationalDatabaseConnectorConfig.USER,
                RelationalDatabaseConnectorConfig.PASSWORD, SERVER_NAME, RelationalDatabaseConnectorConfig.DATABASE_NAME, PDB_NAME,
                XSTREAM_SERVER_NAME, SNAPSHOT_MODE, CONNECTOR_ADAPTER, LOG_MINING_STRATEGY, URL, TABLENAME_CASE_INSENSITIVE, ORACLE_VERSION);
        Field.group(config, "History Storage", KafkaDatabaseHistory.BOOTSTRAP_SERVERS,
                KafkaDatabaseHistory.TOPIC, KafkaDatabaseHistory.RECOVERY_POLL_ATTEMPTS,
                KafkaDatabaseHistory.RECOVERY_POLL_INTERVAL_MS, HistorizedRelationalDatabaseConnectorConfig.DATABASE_HISTORY);
        Field.group(config, "Events", RelationalDatabaseConnectorConfig.TABLE_WHITELIST,
                RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST,
                RelationalDatabaseConnectorConfig.TABLE_BLACKLIST,
                RelationalDatabaseConnectorConfig.TABLE_EXCLUDE_LIST,
                RelationalDatabaseConnectorConfig.MSG_KEY_COLUMNS,
                RelationalDatabaseConnectorConfig.COLUMN_BLACKLIST,
                RelationalDatabaseConnectorConfig.TABLE_IGNORE_BUILTIN,
                CommonConnectorConfig.PROVIDE_TRANSACTION_METADATA,
                Heartbeat.HEARTBEAT_INTERVAL, Heartbeat.HEARTBEAT_TOPICS_PREFIX,
                CommonConnectorConfig.EVENT_PROCESSING_FAILURE_HANDLING_MODE);
        Field.group(config, "Connector", CommonConnectorConfig.POLL_INTERVAL_MS, CommonConnectorConfig.MAX_BATCH_SIZE,
                CommonConnectorConfig.MAX_QUEUE_SIZE, CommonConnectorConfig.SNAPSHOT_DELAY_MS, CommonConnectorConfig.SNAPSHOT_FETCH_SIZE,
                SNAPSHOT_ENHANCEMENT_TOKEN, LOG_MINING_HISTORY_RECORDER_CLASS, LOG_MINING_HISTORY_RETENTION, RAC_SYSTEM, RAC_NODES,
                LOG_MINING_ARCHIVE_LOG_HOURS, LOG_MINING_BATCH_SIZE_DEFAULT, LOG_MINING_BATCH_SIZE_MIN, LOG_MINING_BATCH_SIZE_MAX,
                LOG_MINING_SLEEP_TIME_DEFAULT_MS, LOG_MINING_SLEEP_TIME_MIN_MS, LOG_MINING_SLEEP_TIME_MAX_MS, LOG_MINING_SLEEP_TIME_INCREMENT_MS,
                LOG_MINING_TRANSACTION_RETENTION, LOG_MINING_DML_PARSER);

        return config;
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

    public boolean getTablenameCaseInsensitive() {
        return tablenameCaseInsensitive;
    }

    public OracleVersion getOracleVersion() {
        return oracleVersion;
    }

    public Tables.ColumnNameFilter getColumnFilter() {
        return columnFilter;
    }

    @Override
    protected HistoryRecordComparator getHistoryRecordComparator() {
        return new HistoryRecordComparator() {
            @Override
            protected boolean isPositionAtOrBefore(Document recorded, Document desired) {
                Long recordedScn;
                Long desiredScn;
                if (getAdapter() == OracleConnectorConfig.ConnectorAdapter.XSTREAM) {
                    final LcrPosition recordedPosition = LcrPosition.valueOf(recorded.getString(SourceInfo.LCR_POSITION_KEY));
                    final LcrPosition desiredPosition = LcrPosition.valueOf(desired.getString(SourceInfo.LCR_POSITION_KEY));
                    recordedScn = recordedPosition != null ? recordedPosition.getScn() : recorded.getLong(SourceInfo.SCN_KEY);
                    desiredScn = desiredPosition != null ? desiredPosition.getScn() : desired.getLong(SourceInfo.SCN_KEY);
                    return (recordedPosition != null && desiredPosition != null)
                            ? recordedPosition.compareTo(desiredPosition) < 1
                            : recordedScn.compareTo(desiredScn) < 1;
                }
                else {
                    recordedScn = recorded.getLong(SourceInfo.SCN_KEY);
                    desiredScn = desired.getLong(SourceInfo.SCN_KEY);
                    return recordedScn.compareTo(desiredScn) < 1;
                }

            }
        };
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

    public enum ConnectorAdapter implements EnumeratedValue {

        /**
         * This is based on XStream API.
         */
        XSTREAM("XStream") {
            @Override
            public String getConnectionUrl() {
                return "jdbc:oracle:oci:@${" + JdbcConfiguration.HOSTNAME + "}:${" + JdbcConfiguration.PORT + "}/${" + JdbcConfiguration.DATABASE + "}";
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
        };

        public abstract String getConnectionUrl();

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
                return ConnectorAdapter.XSTREAM;
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

        @Override
        public boolean isIncluded(TableId t) {
            return !t.schema().toLowerCase().equals("appqossys") &&
                    !t.schema().toLowerCase().equals("audsys") &&
                    !t.schema().toLowerCase().equals("ctxsys") &&
                    !t.schema().toLowerCase().equals("dvsys") &&
                    !t.schema().toLowerCase().equals("dbsfwuser") &&
                    !t.schema().toLowerCase().equals("dbsnmp") &&
                    !t.schema().toLowerCase().equals("gsmadmin_internal") &&
                    !t.schema().toLowerCase().equals("lbacsys") &&
                    !t.schema().toLowerCase().equals("mdsys") &&
                    !t.schema().toLowerCase().equals("ojvmsys") &&
                    !t.schema().toLowerCase().equals("olapsys") &&
                    !t.schema().toLowerCase().equals("orddata") &&
                    !t.schema().toLowerCase().equals("ordsys") &&
                    !t.schema().toLowerCase().equals("outln") &&
                    !t.schema().toLowerCase().equals("sys") &&
                    !t.schema().toLowerCase().equals("system") &&
                    !t.schema().toLowerCase().equals("wmsys") &&
                    !t.schema().toLowerCase().equals("xdb");
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
     * @return connection adapter
     */
    public ConnectorAdapter getAdapter() {
        return connectorAdapter;
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

    public static int isPositiveNonZeroInteger(Configuration config, Field field, ValidationOutput problems) {
        Integer value = config.getInteger(field);
        if (value == 0) {
            problems.accept(field, value, "The value must be non-zero.");
            return 1;
        }
        return Field.isPositiveInteger(config, field, problems);
    }
}
