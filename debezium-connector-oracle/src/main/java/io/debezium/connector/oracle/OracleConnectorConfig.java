/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
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
import io.debezium.connector.oracle.logminer.LogMinerStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.logminer.logwriter.LogWriterFlushStrategy;
import io.debezium.connector.oracle.logminer.processor.LogMinerEventProcessor;
import io.debezium.connector.oracle.logminer.processor.ehcache.EhcacheLogMinerEventProcessor;
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
    protected static final int DEFAULT_LOG_FILE_QUERY_MAX_RETRIES = 5;

    protected final static int DEFAULT_BATCH_SIZE = 20_000;
    protected final static int DEFAULT_BATCH_INCREMENT_SIZE = 20_000;
    protected final static int MIN_BATCH_SIZE = 1_000;
    protected final static int MAX_BATCH_SIZE = 100_000;

    protected final static int DEFAULT_SCN_GAP_SIZE = 1_000_000;
    protected final static int DEFAULT_SCN_GAP_TIME_INTERVAL = 20_000;

    protected final static int DEFAULT_TRANSACTION_EVENTS_THRESHOLD = 0;

    protected final static int DEFAULT_QUERY_FETCH_SIZE = 10_000;

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
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 8))
            .withDescription("Name of the pluggable database when working with a multi-tenant set-up. "
                    + "The CDB name must be given via " + DATABASE_NAME.name() + " in this case.");

    public static final Field XSTREAM_SERVER_NAME = Field.create(DATABASE_CONFIG_PREFIX + "out.server.name")
            .withDisplayName("XStream out server name")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.HIGH)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 9))
            .withValidation(OracleConnectorConfig::validateOutServerName)
            .withDescription("Name of the XStream Out server to connect to.");

    public static final Field INTERVAL_HANDLING_MODE = Field.create("interval.handling.mode")
            .withDisplayName("Interval Handling")
            .withEnum(IntervalHandlingMode.class, IntervalHandlingMode.NUMERIC)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 6))
            .withDescription("Specify how INTERVAL columns should be represented in change events, including: "
                    + "'string' represents values as an exact ISO formatted string; "
                    + "'numeric' (default) represents values using the inexact conversion into microseconds");

    public static final Field SNAPSHOT_MODE = Field.create("snapshot.mode")
            .withDisplayName("Snapshot mode")
            .withEnum(SnapshotMode.class, SnapshotMode.INITIAL)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_SNAPSHOT, 0))
            .withDescription("The criteria for running a snapshot upon startup of the connector. "
                    + "Select one of the following snapshot options: "
                    + "'always': The connector runs a snapshot every time that it starts. After the snapshot completes, the connector begins to stream changes from the redo logs.; "
                    + "'initial' (default): If the connector does not detect any offsets for the logical server name, it runs a snapshot that captures the current full state of the configured tables. After the snapshot completes, the connector begins to stream changes from the redo logs. "
                    + "'initial_only': The connector performs a snapshot as it does for the 'initial' option, but after the connector completes the snapshot, it stops, and does not stream changes from the redo logs.; "
                    + "'schema_only': If the connector does not detect any offsets for the logical server name, it runs a snapshot that captures only the schema (table structures), but not any table data. After the snapshot completes, the connector begins to stream changes from the redo logs.; "
                    + "'schema_only_recovery': The connector performs a snapshot that captures only the database schema history. The connector then transitions to streaming from the redo logs. Use this setting to restore a corrupted or lost database schema history topic. Do not use if the database schema was modified after the connector stopped.");

    public static final Field SNAPSHOT_LOCKING_MODE = Field.create("snapshot.locking.mode")
            .withDisplayName("Snapshot locking mode")
            .withEnum(SnapshotLockingMode.class, SnapshotLockingMode.SHARED)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_SNAPSHOT, 1))
            .withDescription("Controls how the connector holds locks on tables while performing the schema snapshot. The default is 'shared', "
                    + "which means the connector will hold a table lock that prevents exclusive table access for just the initial portion of the snapshot "
                    + "while the database schemas and other metadata are being read. The remaining work in a snapshot involves selecting all rows from "
                    + "each table, and this is done using a flashback query that requires no locks. However, in some cases it may be desirable to avoid "
                    + "locks entirely which can be done by specifying 'none'. This mode is only safe to use if no schema changes are happening while the "
                    + "snapshot is taken.");

    public static final Field CONNECTOR_ADAPTER = Field.create(DATABASE_CONFIG_PREFIX + "connection.adapter")
            .withDisplayName("Connector adapter")
            .withEnum(ConnectorAdapter.class, ConnectorAdapter.LOG_MINER)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.HIGH)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 7))
            .withDescription("The adapter to use when capturing changes from the database. "
                    + "Options include: "
                    + "'logminer': (the default) to capture changes using native Oracle LogMiner; "
                    + "'xstream' to capture changes using Oracle XStreams");

    public static final Field LOG_MINING_STRATEGY = Field.create("log.mining.strategy")
            .withDisplayName("Log Mining Strategy")
            .withEnum(LogMiningStrategy.class, LogMiningStrategy.ONLINE_CATALOG)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.HIGH)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 8))
            .withValidation(OracleConnectorConfig::validateLogMiningStrategy)
            .withDescription("There are strategies: Online catalog with faster mining but no captured DDL. Another - with data dictionary loaded into REDO LOG files");

    public static final Field SNAPSHOT_ENHANCEMENT_TOKEN = Field.createInternal("snapshot.enhance.predicate.scn")
            .withDisplayName("A string to replace on snapshot predicate enhancement")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.HIGH)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_SNAPSHOT, 11))
            .withDescription("A token to replace on snapshot predicate template");

    public static final Field LOG_MINING_TRANSACTION_RETENTION_MS = Field.create("log.mining.transaction.retention.ms")
            .withDisplayName("Log Mining long running transaction retention")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDefault(0L)
            .withValidation(Field::isNonNegativeLong)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 18))
            .withDescription("Duration in milliseconds to keep long running transactions in transaction buffer between log mining " +
                    "sessions. By default, all transactions are retained.");

    public static final Field RAC_NODES = Field.create("rac.nodes")
            .withDisplayName("Oracle RAC nodes")
            .withType(Type.STRING)
            .withWidth(Width.SHORT)
            .withImportance(Importance.HIGH)
            .withValidation(OracleConnectorConfig::validateRacNodes)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 11))
            .withDescription("A comma-separated list of RAC node hostnames or ip addresses");

    public static final Field URL = Field.create(DATABASE_CONFIG_PREFIX + "url")
            .withDisplayName("Complete JDBC URL")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .withValidation(OracleConnectorConfig::requiredWhenNoHostname)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 10))
            .withDescription("Complete JDBC URL as an alternative to specifying hostname, port and database provided "
                    + "as a way to support alternative connection scenarios.");

    public static final Field LOG_MINING_BATCH_SIZE_MIN = Field.create("log.mining.batch.size.min")
            .withDisplayName("Minimum batch size for reading redo/archive logs.")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 12))
            .withDefault(MIN_BATCH_SIZE)
            .withDescription(
                    "The minimum SCN interval size that this connector will try to read from redo/archive logs.");

    public static final Field LOG_MINING_BATCH_SIZE_INCREMENT = Field.create("log.mining.batch.size.increment")
            .withDisplayName("Increment/Decrement batch size for reading redo/archive logs.")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 12))
            .withDefault(DEFAULT_BATCH_INCREMENT_SIZE)
            .withDescription("Active batch size will be also increased/decreased by this amount for tuning connector throughput when needed.");

    public static final Field LOG_MINING_BATCH_SIZE_DEFAULT = Field.create("log.mining.batch.size.default")
            .withDisplayName("Default batch size for reading redo/archive logs.")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 11))
            .withDefault(DEFAULT_BATCH_SIZE)
            .withDescription("The starting SCN interval size that the connector will use for reading data from redo/archive logs.");

    public static final Field LOG_MINING_BATCH_SIZE_MAX = Field.create("log.mining.batch.size.max")
            .withDisplayName("Maximum batch size for reading redo/archive logs.")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 13))
            .withDefault(MAX_BATCH_SIZE)
            .withDescription("The maximum SCN interval size that this connector will use when reading from redo/archive logs.");

    public static final Field LOG_MINING_SLEEP_TIME_MIN_MS = Field.create("log.mining.sleep.time.min.ms")
            .withDisplayName("Minimum sleep time in milliseconds when reading redo/archive logs.")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 15))
            .withDefault(MIN_SLEEP_TIME.toMillis())
            .withDescription(
                    "The minimum amount of time that the connector will sleep after reading data from redo/archive logs and before starting reading data again. Value is in milliseconds.");

    public static final Field LOG_MINING_SLEEP_TIME_DEFAULT_MS = Field.create("log.mining.sleep.time.default.ms")
            .withDisplayName("Default sleep time in milliseconds when reading redo/archive logs.")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 14))
            .withDefault(DEFAULT_SLEEP_TIME.toMillis())
            .withDescription(
                    "The amount of time that the connector will sleep after reading data from redo/archive logs and before starting reading data again. Value is in milliseconds.");

    public static final Field LOG_MINING_SLEEP_TIME_MAX_MS = Field.create("log.mining.sleep.time.max.ms")
            .withDisplayName("Maximum sleep time in milliseconds when reading redo/archive logs.")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 16))
            .withDefault(MAX_SLEEP_TIME.toMillis())
            .withDescription(
                    "The maximum amount of time that the connector will sleep after reading data from redo/archive logs and before starting reading data again. Value is in milliseconds.");

    public static final Field LOG_MINING_SLEEP_TIME_INCREMENT_MS = Field.create("log.mining.sleep.time.increment.ms")
            .withDisplayName("The increment in sleep time in milliseconds used to tune auto-sleep behavior.")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 17))
            .withDefault(SLEEP_TIME_INCREMENT.toMillis())
            .withDescription(
                    "The maximum amount of time that the connector will use to tune the optimal sleep time when reading data from LogMiner. Value is in milliseconds.");

    public static final Field LOG_MINING_ARCHIVE_LOG_ONLY_MODE = Field.create("log.mining.archive.log.only.mode")
            .withDisplayName("Specifies whether log mining should only target archive logs or both archive and redo logs")
            .withType(Type.BOOLEAN)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 9))
            .withDefault(false)
            .withDescription("When set to 'false', the default, the connector will mine both archive log and redo logs to emit change events. " +
                    "When set to 'true', the connector will only mine archive logs. There are circumstances where its advantageous to only " +
                    "mine archive logs and accept latency in event emission due to frequent revolving redo logs.");

    public static final Field LOG_MINING_ARCHIVE_LOG_ONLY_SCN_POLL_INTERVAL_MS = Field.create("log.mining.archive.log.only.scn.poll.interval.ms")
            .withDisplayName("The interval in milliseconds to wait between polls when SCN is not yet in the archive logs")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 28))
            .withDefault(ARCHIVE_LOG_ONLY_POLL_TIME.toMillis())
            .withDescription("The interval in milliseconds to wait between polls checking to see if the SCN is in the archive logs.");

    public static final Field LOB_ENABLED = Field.create("lob.enabled")
            .withDisplayName("Specifies whether the connector supports mining LOB fields and operations")
            .withType(Type.BOOLEAN)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_ADVANCED, 21))
            .withDefault(false)
            .withDescription("When set to 'false', the default, LOB fields will not be captured nor emitted. When set to 'true', the connector " +
                    "will capture LOB fields and emit changes for those fields like any other column type.");

    public static final Field LOG_MINING_USERNAME_INCLUDE_LIST = Field.create("log.mining.username.include.list")
            .withDisplayName("List of users to include from LogMiner query")
            .withType(Type.STRING)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("Comma separated list of usernames to include from LogMiner query.");

    public static final Field LOG_MINING_USERNAME_EXCLUDE_LIST = Field.create("log.mining.username.exclude.list")
            .withDisplayName("List of users to exclude from LogMiner query")
            .withType(Type.STRING)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 19))
            .withValidation(OracleConnectorConfig::validateUsernameExcludeList)
            .withDescription("Comma separated list of usernames to exclude from LogMiner query.");

    public static final Field ARCHIVE_DESTINATION_NAME = Field.create("archive.destination.name")
            .withDisplayName("Name of the archive log destination to be used for reading archive logs")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 32))
            .withDescription("Sets the specific archive log destination as the source for reading archive logs." +
                    "When not set, the connector will automatically select the first LOCAL and VALID destination.");

    public static final Field ARCHIVE_LOG_HOURS = Field.create("archive.log.hours")
            .withDisplayName("Archive Log Hours")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 31))
            .withDefault(0)
            .withDescription("The number of hours in the past from SYSDATE to mine archive logs. Using 0 mines all available archive logs");

    public static final Field LOG_MINING_BUFFER_TYPE = Field.create("log.mining.buffer.type")
            .withDisplayName("Controls which buffer type implementation to be used")
            .withEnum(LogMiningBufferType.class, LogMiningBufferType.MEMORY)
            .withValidation(OracleConnectorConfig::validateLogMiningBufferType)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 21))
            .withDescription("The buffer type controls how the connector manages buffering transaction data." + System.lineSeparator() +
                    System.lineSeparator() +
                    "memory - Uses the JVM process' heap to buffer all transaction data." + System.lineSeparator() +
                    System.lineSeparator() +
                    "infinispan_embedded - This option uses an embedded Infinispan cache to buffer transaction data and persist it to disk." + System.lineSeparator() +
                    System.lineSeparator() +
                    "infinispan_remote - This option uses a remote Infinispan cluster to buffer transaction data and persist it to disk." + System.lineSeparator() +
                    System.lineSeparator() +
                    "ehcache - Use ehcache in embedded mode to buffer transaction data and persist it to disk.");

    public static final Field LOG_MINING_BUFFER_TRANSACTION_EVENTS_THRESHOLD = Field.create("log.mining.buffer.transaction.events.threshold")
            .withDisplayName("The maximum number of events a transaction can have before being discarded.")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(DEFAULT_TRANSACTION_EVENTS_THRESHOLD)
            .withValidation(Field::isNonNegativeLong)
            .withDescription("The number of events a transaction can include before the transaction is discarded. " +
                    "This is useful for managing buffer memory and/or space when dealing with very large transactions. " +
                    "Defaults to 0, meaning that no threshold is applied and transactions can have unlimited events.");

    public static final Field LOG_MINING_BUFFER_INFINISPAN_CACHE_GLOBAL = Field.create("log.mining.buffer.infinispan.cache.global")
            .withDisplayName("Infinispan 'global' cache configuration")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 27))
            .withValidation(OracleConnectorConfig::validateLogMiningInfinispanCacheConfiguration)
            .withDescription("Specifies the XML configuration for the Infinispan 'global' configuration");

    public static final Field LOG_MINING_BUFFER_INFINISPAN_CACHE_TRANSACTIONS = Field.create("log.mining.buffer.infinispan.cache.transactions")
            .withDisplayName("Infinispan 'transactions' cache configuration")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 23))
            .withValidation(OracleConnectorConfig::validateLogMiningInfinispanCacheConfiguration)
            .withDescription("Specifies the XML configuration for the Infinispan 'transactions' cache");

    public static final Field LOG_MINING_BUFFER_INFINISPAN_CACHE_PROCESSED_TRANSACTIONS = Field.create("log.mining.buffer.infinispan.cache.processed_transactions")
            .withDisplayName("Infinispan 'processed-transactions' cache configuration")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 25))
            .withValidation(OracleConnectorConfig::validateLogMiningInfinispanCacheConfiguration)
            .withDescription("Specifies the XML configuration for the Infinispan 'processed-transactions' cache");

    public static final Field LOG_MINING_BUFFER_INFINISPAN_CACHE_EVENTS = Field.create("log.mining.buffer.infinispan.cache.events")
            .withDisplayName("Infinispan 'events' cache configurations")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 24))
            .withValidation(OracleConnectorConfig::validateLogMiningInfinispanCacheConfiguration)
            .withDescription("Specifies the XML configuration for the Infinispan 'events' cache");

    public static final Field LOG_MINING_BUFFER_INFINISPAN_CACHE_SCHEMA_CHANGES = Field.create("log.mining.buffer.infinispan.cache.schema_changes")
            .withDisplayName("Infinispan 'schema-changes' cache configuration")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 26))
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
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 29))
            .withDefault(DEFAULT_SCN_GAP_SIZE)
            .withDescription("Used for SCN gap detection, if the difference between current SCN and previous end SCN is " +
                    "bigger than this value, and the time difference of current SCN and previous end SCN is smaller than " +
                    "log.mining.scn.gap.detection.time.interval.max.ms, consider it a SCN gap.");

    public static final Field LOG_MINING_SCN_GAP_DETECTION_TIME_INTERVAL_MAX_MS = Field.create("log.mining.scn.gap.detection.time.interval.max.ms")
            .withDisplayName("Timer interval used to detect SCN gap")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 30))
            .withDefault(DEFAULT_SCN_GAP_TIME_INTERVAL)
            .withDescription("Used for SCN gap detection, if the difference between current SCN and previous end SCN is " +
                    "bigger than log.mining.scn.gap.detection.gap.size.min, and the time difference of current SCN and previous end SCN is smaller than " +
                    " this value, consider it a SCN gap.");

    public static final Field LOG_MINING_LOG_QUERY_MAX_RETRIES = Field.createInternal("log.mining.log.query.max.retries")
            .withDisplayName("Maximum number of retries before failing to locate redo logs")
            .withType(Type.INT)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(DEFAULT_LOG_FILE_QUERY_MAX_RETRIES)
            .withValidation(Field::isPositiveInteger)
            .withDescription("The maximum number of log query retries before throwing an exception that logs cannot be found.");

    public static final Field LOG_MINING_LOG_BACKOFF_INITIAL_DELAY_MS = Field.createInternal("log.mining.log.backoff.initial.delay.ms")
            .withDisplayName("Initial delay when logs cannot yet be found (ms)")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(TimeUnit.SECONDS.toMillis(1))
            .withValidation(Field::isPositiveInteger)
            .withDescription("The initial delay when trying to query database redo logs, given in milliseconds. Defaults to 1 second (1,000 ms).");

    public static final Field LOG_MINING_LOG_BACKOFF_MAX_DELAY_MS = Field.createInternal("log.mining.log.backoff.max.delay.ms")
            .withDisplayName("Maximum delay when logs cannot yet be found (ms)")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(TimeUnit.MINUTES.toMillis(1))
            .withValidation(Field::isPositiveInteger)
            .withDescription("The maximum delay when trying to query database redo logs, given in milliseconds. Defaults to 60 seconds (60,000 ms).");

    public static final Field LOG_MINING_SESSION_MAX_MS = Field.create("log.mining.session.max.ms")
            .withDisplayName("Maximum number of milliseconds of a single LogMiner session")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(TimeUnit.MINUTES.toMillis(0))
            .withValidation(Field::isNonNegativeInteger)
            .withDescription(
                    "The maximum number of milliseconds that a LogMiner session lives for before being restarted. Defaults to 0 (indefinite until a log switch occurs)");

    public static final Field LOG_MINING_RESTART_CONNECTION = Field.create("log.mining.restart.connection")
            .withDisplayName("Restarts Oracle database connection when reaching maximum session time or database log switch")
            .withType(Type.BOOLEAN)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(false)
            .withDescription("Debezium opens a database connection and keeps that connection open throughout the entire streaming phase. " +
                    "In some situations, this can lead to excessive SGA memory usage. " +
                    "By setting this option to 'true' (the default is 'false'), the connector will close and re-open a database connection " +
                    "after every detected log switch or if the log.mining.session.max.ms has been reached.");

    public static final Field LOG_MINING_TRANSACTION_SNAPSHOT_BOUNDARY_MODE = Field.createInternal("log.mining.transaction.snapshot.boundary.mode")
            .withEnum(TransactionSnapshotBoundaryMode.class, TransactionSnapshotBoundaryMode.SKIP)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("Specifies how in-progress transactions are to be handled when resolving the snapshot SCN. " + System.lineSeparator() +
                    "all - Captures in-progress transactions from both V$TRANSACTION and starting a LogMiner session near the snapshot SCN." + System.lineSeparator() +
                    "transaction_view_only - Captures in-progress transactions based on data in V$TRANSACTION only. " +
                    "Recently committed transactions near the flashback query SCN won't be included in the snapshot nor streaming." + System.lineSeparator() +
                    "skip - Skips gathering any in-progress transactions.");

    public static final Field LOG_MINING_QUERY_FILTER_MODE = Field.create("log.mining.query.filter.mode")
            .withDisplayName("Specifies how the filter configuration is applied to the LogMiner database query")
            .withEnum(LogMiningQueryFilterMode.class, LogMiningQueryFilterMode.NONE)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("Specifies how the filter configuration is applied to the LogMiner database query. " + System.lineSeparator() +
                    "none - The query does not apply any schema or table filters, all filtering is at runtime by the connector." + System.lineSeparator() +
                    "in - The query uses SQL in-clause expressions to specify the schema or table filters." + System.lineSeparator() +
                    "regex - The query uses Oracle REGEXP_LIKE expressions to specify the schema or table filters." + System.lineSeparator());

    public static final Field LOG_MINING_READ_ONLY = Field.createInternal("log.mining.read.only")
            .withDisplayName("Runs the connector in read-only mode")
            .withType(Type.BOOLEAN)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(Boolean.FALSE)
            .withValidation(OracleConnectorConfig::validateLogMiningReadOnly)
            .withDescription("When set to 'true', the connector will not attempt to flush the LGWR buffer to disk, allowing connecting to read-only databases.");

    public static final Field LOG_MINING_FLUSH_TABLE_NAME = Field.create("log.mining.flush.table.name")
            .withDisplayName("Specifies the name of the flush table used by the connector")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withDefault("LOG_MINING_FLUSH")
            .withValidation(OracleConnectorConfig::validateLogMiningFlushTableName)
            .withDescription("The name of the flush table used by the connector, defaults to LOG_MINING_FLUSH.");

    public static final Field SOURCE_INFO_STRUCT_MAKER = CommonConnectorConfig.SOURCE_INFO_STRUCT_MAKER
            .withDefault(OracleSourceInfoStructMaker.class.getName());

    public static final Field QUERY_FETCH_SIZE = CommonConnectorConfig.QUERY_FETCH_SIZE
            .withDescription(
                    "The maximum number of records that should be loaded into memory while streaming. A value of '0' uses the default JDBC fetch size, defaults to '2000'.")
            .withDefault(DEFAULT_QUERY_FETCH_SIZE);

    public static final Field LOG_MINING_MAX_SCN_DEVIATION_MS = Field.createInternal("log.mining.max.scn.deviation.ms")
            .withDisplayName("Allows applying a time-based deviation to the max mining scn")
            .withType(Type.LONG)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withDefault(0)
            .withValidation(Field::isNonNegativeLong)
            .withDescription("By default, LogMiner will apply no deviation, meaning that the connector can mine up to the CURRENT_SCN. " +
                    "There are situations where this could be problematic if perhaps when asynchronous IO operations are at play. " +
                    "By applying a time-based deviation, for example 3000, the connector will only mine up the SCN that is a result of " +
                    "the formula of TIMESTAMP_TO_SCN(SCN_TO_TIMESTAMP(CURRENT_SCN)-(3000/86400000)). If this SCN is not available, the " +
                    "connector will log a warning and proceed to use the CURRENT_SCN or previously calculated upper SCN regardless. " +
                    "NOTE: This option is internal and should not be used for general use. Using this option will create a net latency " +
                    "on change events increased by the deviation value specified.");

    public static final Field OLR_SOURCE = Field.create("openlogreplicator.source")
            .withDisplayName("The logical source to stream changes from")
            .withType(Type.STRING)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withValidation(OracleConnectorConfig::validateRequiredWhenUsingOpenLogReplicator)
            .withDescription("The configured logical source name in the OpenLogReplicator configuration that is to stream changes");

    public static final Field OLR_HOST = Field.create("openlogreplicator.host")
            .withDisplayName("The hostname of the OpenLogReplicator network service")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withValidation(OracleConnectorConfig::validateRequiredWhenUsingOpenLogReplicator)
            .withDescription("The hostname of the OpenLogReplicator network service");

    public static final Field OLR_PORT = Field.create("openlogreplicator.port")
            .withDisplayName("The port of the OpenLogReplicator network service")
            .withType(Type.INT)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withValidation(OracleConnectorConfig::validateRequiredWhenUsingOpenLogReplicator)
            .withDescription("The port of the OpenLogReplicator network service");

    public static final Field LOG_MINING_SCHEMA_CHANGES_USERNAME_EXCLUDE_LIST = Field.createInternal("log.mining.schema_changes.username.exclude.list")
            .withDisplayName("Username exclusion list for schema changes")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withDescription("A comma-separated list of usernames that schema changes will be skipped for. Defaults to 'SYS,SYSTEM'.")
            .withDefault("SYS,SYSTEM");

    public static final Field LOG_MINING_INCLUDE_REDO_SQL = Field.create("log.mining.include.redo.sql")
            .withDisplayName("Include the transaction log SQL")
            .withType(Type.BOOLEAN)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("When enabled, the transaction log REDO SQL will be included in the source information block.")
            .withDefault(false)
            .withValidation(OracleConnectorConfig::validateLogMiningIncludeRedoSql);

    public static final Field SNAPSHOT_DATABASE_ERRORS_MAX_RETRIES = Field.create("snapshot.database.errors.max.retries")
            .withDisplayName("The maximum number of retries before snapshot database errors are not retried")
            .withType(Type.INT)
            .withDefault(0)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withValidation(Field::isNonNegativeInteger)
            .withDescription("The number of attempts to retry database errors during snapshots before failing.");

    public static final Field LOG_MINING_BUFFER_EHCACHE_GLOBAL_CONFIG = Field.create("log.mining.buffer.ehcache.global.config")
            .withDisplayName("Defines any global configuration for the Ehcache transaction buffer")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withValidation(OracleConnectorConfig::validateEhCacheGlobalConfigField)
            .withDescription("Specifies any Ehcache global configurations such as services or persistence. " +
                    "This cannot include <cache/> nor <default-serializers/> tags as these are managed by Debezium.");

    public static final Field LOG_MINING_BUFFER_EHCACHE_TRANSACTIONS_CONFIG = Field.create("log.mining.buffer.ehcache.transactions.config")
            .withDisplayName("Defines the partial ehcache configuration for the transaction cache")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withValidation(OracleConnectorConfig::validateEhcacheConfigFieldRequired)
            .withDescription("Specifies the inner body the Ehcache <cache/> tag for the transaction cache, but " +
                    "should not include the <key-type/> nor the <value-type/> attributes as these are managed by Debezium.");

    public static final Field LOG_MINING_BUFFER_EHCACHE_PROCESSED_TRANSACTIONS_CONFIG = Field.create("log.mining.buffer.ehcache.processedtransactions.config")
            .withDisplayName("Defines the partial ehcache configuration for the processed transaction cache")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withValidation(OracleConnectorConfig::validateEhcacheConfigFieldRequired)
            .withDescription("Specifies the inner body the Ehcache <cache/> tag for the processed transaction cache, but " +
                    "should not include the <key-type/> nor the <value-type/> attributes as these are managed by Debezium.");

    public static final Field LOG_MINING_BUFFER_EHCACHE_SCHEMA_CHANGES_CONFIG = Field.create("log.mining.buffer.ehcache.schemachanges.config")
            .withDisplayName("Defines the partial ehcache configuration for the schema changes cache")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withValidation(OracleConnectorConfig::validateEhcacheConfigFieldRequired)
            .withDescription("Specifies the inner body the Ehcache <cache/> tag for the schema changes cache, but " +
                    "should not include the <key-type/> nor the <value-type/> attributes as these are managed by Debezium.");

    public static final Field LOG_MINING_BUFFER_EHCACHE_EVENTS_CONFIG = Field.create("log.mining.buffer.ehcache.events.config")
            .withDisplayName("Defines the partial ehcache configuration for the events cache")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withValidation(OracleConnectorConfig::validateEhcacheConfigFieldRequired)
            .withDescription("Specifies the inner body the Ehcache <cache/> tag for the events cache, but " +
                    "should not include the <key-type/> nor the <value-type/> attributes as these are managed by Debezium.");

    @Deprecated
    public static final Field LOG_MINING_CONTINUOUS_MINE = Field.create("log.mining.continuous.mine")
            .withDisplayName("Should log mining session configured with CONTINUOUS_MINE setting?")
            .withType(Type.BOOLEAN)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(false)
            .withValidation(Field::isBoolean)
            .withDescription("(Deprecated) if true, CONTINUOUS_MINE option will be added to the log mining session. " +
                    "This will manage log files switches seamlessly.");

    public static final Field LOG_MINING_PATH_DICTIONARY = Field.create("log.mining.path.dictionary")
            .withDisplayName("Defines the dictionary path for the mining session")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withValidation(OracleConnectorConfig::validateDictionaryFromFile)
            .withDescription("This is required when using the connector against a read-only database replica.");

    public static final Field LOG_MINING_READONLY_HOSTNAME = Field.create("log.mining.readonly.hostname")
            .withDisplayName("Read-only connector hostname.")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withDescription("The hostname the connector will use to connect and perform read-only operations for the the replica.");

    public static final Field OBJECT_ID_CACHE_SIZE = Field.createInternal("object.id.cache.size")
            .withDisplayName("Controls the maximum size of the object ID cache")
            .withType(Type.INT)
            .withWidth(Width.SHORT)
            .withDefault(256)
            .withImportance(Importance.LOW)
            .withValidation(OracleConnectorConfig::validateObjectIdCacheSize)
            .withDescription("The connector maintains a least-recently used cache of database table object ID to name mappings. "
                    + "This controls the maximum capacity of this cache.");

    public static final Field LOG_MINING_SQL_RELAXED_QUOTE_DETECTION = Field.createInternal("log.mining.sql.relaxed.quote.detection")
            .withDisplayName("Controls whether single-quote detection is relaxed")
            .withType(Type.BOOLEAN)
            .withWidth(Width.SHORT)
            .withDefault(false)
            .withImportance(Importance.LOW)
            .withDescription("When Oracle is configured to use EXTENDED string sizes, there are some use cases where LogMiner will " +
                    "not escape single quotes within a column value, which will lead to value truncation.");

    private static final ConfigDefinition CONFIG_DEFINITION = HistorizedRelationalDatabaseConnectorConfig.CONFIG_DEFINITION.edit()
            .name("Oracle")
            .excluding(
                    SCHEMA_INCLUDE_LIST,
                    SCHEMA_EXCLUDE_LIST,
                    RelationalDatabaseConnectorConfig.TABLE_IGNORE_BUILTIN,
                    CommonConnectorConfig.QUERY_FETCH_SIZE)
            .type(
                    HOSTNAME,
                    PORT,
                    USER,
                    PASSWORD,
                    DATABASE_NAME,
                    QUERY_TIMEOUT_MS,
                    PDB_NAME,
                    XSTREAM_SERVER_NAME,
                    SNAPSHOT_MODE,
                    CONNECTOR_ADAPTER,
                    LOG_MINING_STRATEGY,
                    URL)
            .connector(
                    QUERY_FETCH_SIZE,
                    SNAPSHOT_ENHANCEMENT_TOKEN,
                    SNAPSHOT_LOCKING_MODE,
                    RAC_NODES,
                    INTERVAL_HANDLING_MODE,
                    ARCHIVE_LOG_HOURS,
                    LOG_MINING_BATCH_SIZE_DEFAULT,
                    LOG_MINING_BATCH_SIZE_MIN,
                    LOG_MINING_BATCH_SIZE_MAX,
                    LOG_MINING_BATCH_SIZE_INCREMENT,
                    LOG_MINING_SLEEP_TIME_DEFAULT_MS,
                    LOG_MINING_SLEEP_TIME_MIN_MS,
                    LOG_MINING_SLEEP_TIME_MAX_MS,
                    LOG_MINING_SLEEP_TIME_INCREMENT_MS,
                    LOG_MINING_TRANSACTION_RETENTION_MS,
                    LOG_MINING_ARCHIVE_LOG_ONLY_MODE,
                    LOB_ENABLED,
                    LOG_MINING_USERNAME_INCLUDE_LIST,
                    LOG_MINING_USERNAME_EXCLUDE_LIST,
                    ARCHIVE_DESTINATION_NAME,
                    LOG_MINING_BUFFER_TYPE,
                    LOG_MINING_BUFFER_DROP_ON_STOP,
                    LOG_MINING_BUFFER_INFINISPAN_CACHE_GLOBAL,
                    LOG_MINING_BUFFER_INFINISPAN_CACHE_TRANSACTIONS,
                    LOG_MINING_BUFFER_INFINISPAN_CACHE_EVENTS,
                    LOG_MINING_BUFFER_INFINISPAN_CACHE_PROCESSED_TRANSACTIONS,
                    LOG_MINING_BUFFER_INFINISPAN_CACHE_SCHEMA_CHANGES,
                    LOG_MINING_BUFFER_TRANSACTION_EVENTS_THRESHOLD,
                    LOG_MINING_ARCHIVE_LOG_ONLY_SCN_POLL_INTERVAL_MS,
                    LOG_MINING_SCN_GAP_DETECTION_GAP_SIZE_MIN,
                    LOG_MINING_SCN_GAP_DETECTION_TIME_INTERVAL_MAX_MS,
                    UNAVAILABLE_VALUE_PLACEHOLDER,
                    BINARY_HANDLING_MODE,
                    SCHEMA_NAME_ADJUSTMENT_MODE,
                    LOG_MINING_LOG_QUERY_MAX_RETRIES,
                    LOG_MINING_LOG_BACKOFF_INITIAL_DELAY_MS,
                    LOG_MINING_LOG_BACKOFF_MAX_DELAY_MS,
                    LOG_MINING_SESSION_MAX_MS,
                    LOG_MINING_TRANSACTION_SNAPSHOT_BOUNDARY_MODE,
                    LOG_MINING_READ_ONLY,
                    LOG_MINING_FLUSH_TABLE_NAME,
                    LOG_MINING_QUERY_FILTER_MODE,
                    LOG_MINING_RESTART_CONNECTION,
                    LOG_MINING_MAX_SCN_DEVIATION_MS,
                    LOG_MINING_SCHEMA_CHANGES_USERNAME_EXCLUDE_LIST,
                    LOG_MINING_INCLUDE_REDO_SQL,
                    OLR_SOURCE,
                    OLR_HOST,
                    OLR_PORT,
                    SNAPSHOT_DATABASE_ERRORS_MAX_RETRIES,
                    LOG_MINING_CONTINUOUS_MINE,
                    LOG_MINING_BUFFER_EHCACHE_GLOBAL_CONFIG,
                    LOG_MINING_BUFFER_EHCACHE_TRANSACTIONS_CONFIG,
                    LOG_MINING_BUFFER_EHCACHE_PROCESSED_TRANSACTIONS_CONFIG,
                    LOG_MINING_BUFFER_EHCACHE_SCHEMA_CHANGES_CONFIG,
                    LOG_MINING_BUFFER_EHCACHE_EVENTS_CONFIG,
                    OBJECT_ID_CACHE_SIZE,
                    LOG_MINING_SQL_RELAXED_QUOTE_DETECTION,
                    OBJECT_ID_CACHE_SIZE,
                    LOG_MINING_PATH_DICTIONARY,
                    LOG_MINING_READONLY_HOSTNAME)
            .events(SOURCE_INFO_STRUCT_MAKER)
            .create();

    /**
     * The set of {@link Field}s defined as part of this configuration.
     */
    public static Field.Set ALL_FIELDS = Field.setOf(CONFIG_DEFINITION.all());

    public static ConfigDef configDef() {
        return CONFIG_DEFINITION.configDef();
    }

    public static final List<String> EXCLUDED_SCHEMAS = Collections.unmodifiableList(Arrays.asList("appqossys", "audsys",
            "ctxsys", "dvsys", "dbsfwuser", "dbsnmp", "ggsharedcap", "gsmadmin_internal", "lbacsys", "mdsys", "ojvmsys", "olapsys",
            "orddata", "ordsys", "outln", "sys", "system", "vecsys", "wmsys", "xdb"));

    private static final Logger LOGGER = LoggerFactory.getLogger(OracleConnectorConfig.class);

    private final String databaseName;
    private final String pdbName;
    private final String xoutServerName;
    private final IntervalHandlingMode intervalHandlingMode;
    private final SnapshotMode snapshotMode;

    private ConnectorAdapter connectorAdapter;
    private final StreamingAdapter streamingAdapter;
    private final String snapshotEnhancementToken;
    private final SnapshotLockingMode snapshotLockingMode;
    private final int queryFetchSize;
    private final int snapshotRetryDatabaseErrorsMaxRetries;
    private final int objectIdToTableIdCacheSize;

    // LogMiner options
    private final LogMiningStrategy logMiningStrategy;
    private final Set<String> racNodes;
    private final Duration archiveLogRetention;
    private final int logMiningBatchSizeMin;
    private final int logMiningBatchSizeMax;
    private final int logMiningBatchSizeDefault;
    private final int logMiningBatchSizeIncrement;
    private final Duration logMiningSleepTimeMin;
    private final Duration logMiningSleepTimeMax;
    private final Duration logMiningSleepTimeDefault;
    private final Duration logMiningSleepTimeIncrement;
    private final Duration logMiningTransactionRetention;
    private final boolean archiveLogOnlyMode;
    private final Duration archiveLogOnlyScnPollTime;
    private final boolean lobEnabled;
    private final Set<String> logMiningUsernameIncludes;
    private final Set<String> logMiningUsernameExcludes;
    private final String archiveLogDestinationName;
    private final LogMiningBufferType logMiningBufferType;
    private final long logMiningBufferTransactionEventsThreshold;
    private final boolean logMiningBufferDropOnStop;
    private final int logMiningScnGapDetectionGapSizeMin;
    private final int logMiningScnGapDetectionTimeIntervalMaxMs;
    private final int logMiningLogFileQueryMaxRetries;
    private final Duration logMiningInitialDelay;
    private final Duration logMiningMaxDelay;
    private final Duration logMiningMaximumSession;
    private final TransactionSnapshotBoundaryMode logMiningTransactionSnapshotBoundaryMode;
    private final Boolean logMiningReadOnly;
    private final String logMiningFlushTableName;
    private final LogMiningQueryFilterMode logMiningQueryFilterMode;
    private final Boolean logMiningRestartConnection;
    private final Duration logMiningMaxScnDeviation;
    private final String logMiningInifispanGlobalConfiguration;
    private final Set<String> logMiningSchemaChangesUsernameExcludes;
    private final Boolean logMiningIncludeRedoSql;
    private final boolean logMiningContinuousMining;
    private final Configuration logMiningEhCacheConfiguration;
    private final boolean logMiningUseSqlRelaxedQuoteDetection;
    private final String logMiningPathToDictionary;
    private final String readonlyHostname;

    private final String openLogReplicatorSource;
    private final String openLogReplicatorHostname;
    private final Integer openLogReplicatorPort;

    public OracleConnectorConfig(Configuration config) {
        super(
                OracleConnector.class, config,
                new SystemTablesPredicate(config),
                x -> x.schema() + "." + x.table(),
                true,
                DEFAULT_QUERY_FETCH_SIZE,
                ColumnFilterMode.SCHEMA,
                false);

        this.databaseName = toUpperCase(config.getString(DATABASE_NAME));
        this.pdbName = toUpperCase(config.getString(PDB_NAME));
        this.xoutServerName = config.getString(XSTREAM_SERVER_NAME);
        this.intervalHandlingMode = IntervalHandlingMode.parse(config.getString(INTERVAL_HANDLING_MODE));
        this.snapshotMode = SnapshotMode.parse(config.getString(SNAPSHOT_MODE));
        this.snapshotEnhancementToken = config.getString(SNAPSHOT_ENHANCEMENT_TOKEN);
        this.connectorAdapter = ConnectorAdapter.parse(config.getString(CONNECTOR_ADAPTER));
        this.snapshotLockingMode = SnapshotLockingMode.parse(config.getString(SNAPSHOT_LOCKING_MODE), SNAPSHOT_LOCKING_MODE.defaultValueAsString());
        this.lobEnabled = config.getBoolean(LOB_ENABLED);
        this.objectIdToTableIdCacheSize = config.getInteger(OBJECT_ID_CACHE_SIZE);

        this.streamingAdapter = this.connectorAdapter.getInstance(this);
        if (this.streamingAdapter == null) {
            throw new DebeziumException("Unable to instantiate the connector adapter implementation");
        }

        this.queryFetchSize = config.getInteger(QUERY_FETCH_SIZE);
        this.snapshotRetryDatabaseErrorsMaxRetries = config.getInteger(SNAPSHOT_DATABASE_ERRORS_MAX_RETRIES);

        // LogMiner
        this.logMiningStrategy = LogMiningStrategy.parse(config.getString(LOG_MINING_STRATEGY));
        this.racNodes = resolveRacNodes(config);
        this.archiveLogRetention = config.getDuration(ARCHIVE_LOG_HOURS, ChronoUnit.HOURS);
        this.logMiningBatchSizeMin = config.getInteger(LOG_MINING_BATCH_SIZE_MIN);
        this.logMiningBatchSizeMax = config.getInteger(LOG_MINING_BATCH_SIZE_MAX);
        this.logMiningBatchSizeDefault = config.getInteger(LOG_MINING_BATCH_SIZE_DEFAULT);
        this.logMiningBatchSizeIncrement = config.getInteger(LOG_MINING_BATCH_SIZE_INCREMENT);
        this.logMiningSleepTimeMin = Duration.ofMillis(config.getInteger(LOG_MINING_SLEEP_TIME_MIN_MS));
        this.logMiningSleepTimeMax = Duration.ofMillis(config.getInteger(LOG_MINING_SLEEP_TIME_MAX_MS));
        this.logMiningSleepTimeDefault = Duration.ofMillis(config.getInteger(LOG_MINING_SLEEP_TIME_DEFAULT_MS));
        this.logMiningSleepTimeIncrement = Duration.ofMillis(config.getInteger(LOG_MINING_SLEEP_TIME_INCREMENT_MS));
        this.logMiningTransactionRetention = config.getDuration(LOG_MINING_TRANSACTION_RETENTION_MS, ChronoUnit.MILLIS);
        this.archiveLogOnlyMode = config.getBoolean(LOG_MINING_ARCHIVE_LOG_ONLY_MODE);
        this.logMiningUsernameIncludes = Strings.setOfTrimmed(config.getString(LOG_MINING_USERNAME_INCLUDE_LIST), String::new);
        this.logMiningUsernameExcludes = Strings.setOfTrimmed(config.getString(LOG_MINING_USERNAME_EXCLUDE_LIST), String::new);
        this.archiveLogDestinationName = config.getString(ARCHIVE_DESTINATION_NAME);
        this.logMiningBufferType = LogMiningBufferType.parse(config.getString(LOG_MINING_BUFFER_TYPE));
        this.logMiningBufferTransactionEventsThreshold = config.getLong(LOG_MINING_BUFFER_TRANSACTION_EVENTS_THRESHOLD);
        this.logMiningBufferDropOnStop = config.getBoolean(LOG_MINING_BUFFER_DROP_ON_STOP);
        this.archiveLogOnlyScnPollTime = Duration.ofMillis(config.getInteger(LOG_MINING_ARCHIVE_LOG_ONLY_SCN_POLL_INTERVAL_MS));
        this.logMiningScnGapDetectionGapSizeMin = config.getInteger(LOG_MINING_SCN_GAP_DETECTION_GAP_SIZE_MIN);
        this.logMiningScnGapDetectionTimeIntervalMaxMs = config.getInteger(LOG_MINING_SCN_GAP_DETECTION_TIME_INTERVAL_MAX_MS);
        this.logMiningLogFileQueryMaxRetries = config.getInteger(LOG_MINING_LOG_QUERY_MAX_RETRIES);
        this.logMiningInitialDelay = Duration.ofMillis(config.getLong(LOG_MINING_LOG_BACKOFF_INITIAL_DELAY_MS));
        this.logMiningMaxDelay = Duration.ofMillis(config.getLong(LOG_MINING_LOG_BACKOFF_MAX_DELAY_MS));
        this.logMiningMaximumSession = Duration.ofMillis(config.getLong(LOG_MINING_SESSION_MAX_MS));
        this.logMiningTransactionSnapshotBoundaryMode = TransactionSnapshotBoundaryMode.parse(config.getString(LOG_MINING_TRANSACTION_SNAPSHOT_BOUNDARY_MODE));
        this.logMiningReadOnly = config.getBoolean(LOG_MINING_READ_ONLY);
        this.logMiningFlushTableName = config.getString(LOG_MINING_FLUSH_TABLE_NAME);
        this.logMiningQueryFilterMode = LogMiningQueryFilterMode.parse(config.getString(LOG_MINING_QUERY_FILTER_MODE));
        this.logMiningRestartConnection = config.getBoolean(LOG_MINING_RESTART_CONNECTION);
        this.logMiningMaxScnDeviation = Duration.ofMillis(config.getLong(LOG_MINING_MAX_SCN_DEVIATION_MS));
        this.logMiningInifispanGlobalConfiguration = config.getString(LOG_MINING_BUFFER_INFINISPAN_CACHE_GLOBAL);
        this.logMiningSchemaChangesUsernameExcludes = Strings.setOf(config.getString(LOG_MINING_SCHEMA_CHANGES_USERNAME_EXCLUDE_LIST), String::new);
        this.logMiningIncludeRedoSql = config.getBoolean(LOG_MINING_INCLUDE_REDO_SQL);
        this.logMiningContinuousMining = config.getBoolean(LOG_MINING_CONTINUOUS_MINE);
        this.logMiningUseSqlRelaxedQuoteDetection = config.getBoolean(LOG_MINING_SQL_RELAXED_QUOTE_DETECTION);
        this.logMiningPathToDictionary = config.getString(LOG_MINING_PATH_DICTIONARY);
        this.readonlyHostname = config.getString(LOG_MINING_READONLY_HOSTNAME);

        this.logMiningEhCacheConfiguration = config.subset("log.mining.buffer.ehcache", false);

        // OpenLogReplicator
        this.openLogReplicatorSource = config.getString(OLR_SOURCE);
        this.openLogReplicatorHostname = config.getString(OLR_HOST);
        this.openLogReplicatorPort = config.getInteger(OLR_PORT, 0);
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

    public IntervalHandlingMode getIntervalHandlingMode() {
        return intervalHandlingMode;
    }

    public SnapshotMode getSnapshotMode() {
        return snapshotMode;
    }

    public Optional<SnapshotLockingMode> getSnapshotLockingMode() {
        return Optional.ofNullable(snapshotLockingMode);
    }

    @Override
    public int getQueryFetchSize() {
        return queryFetchSize;
    }

    public int getSnapshotRetryDatabaseErrorsMaxRetries() {
        return snapshotRetryDatabaseErrorsMaxRetries;
    }

    @Override
    protected HistoryRecordComparator getHistoryRecordComparator() {
        return getAdapter().getHistoryRecordComparator();
    }

    /**
     * Defines modes of representation of {@code interval} datatype
     */
    public enum IntervalHandlingMode implements EnumeratedValue {

        /**
         * Represents interval as inexact microseconds count
         */
        NUMERIC("numeric"),

        /**
         * Represents interval as ISO 8601 time interval
         */
        STRING("string");

        private final String value;

        IntervalHandlingMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Convert mode name into the logical value
         *
         * @param value the configuration property value ; may not be null
         * @return the matching option, or null if the match is not found
         */
        public static IntervalHandlingMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (IntervalHandlingMode option : IntervalHandlingMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }

        /**
         * Convert mode name into the logical value
         *
         * @param value the configuration property value ; may not be null
         * @param defaultValue the default value ; may be null
         * @return the matching option or null if the match is not found and non-null default is invalid
         */
        public static IntervalHandlingMode parse(String value, String defaultValue) {
            IntervalHandlingMode mode = parse(value);
            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }
            return mode;
        }
    }

    /**
     * The set of predefined SnapshotMode options or aliases.
     */
    public enum SnapshotMode implements EnumeratedValue {

        /**
         * Performs a snapshot of data and schema upon each connector start.
         */
        ALWAYS("always"),

        /**
         * Perform a snapshot of data and schema upon initial startup of a connector.
         */
        INITIAL("initial"),

        /**
         * Perform a snapshot of data and schema upon initial startup of a connector and stop after initial consistent snapshot.
         */
        INITIAL_ONLY("initial_only"),

        /**
         * Perform a snapshot of the schema but no data upon initial startup of a connector.
         *
         * @deprecated to be removed in Debezium 3.0, replaced by {{@link #NO_DATA}}
         */
        SCHEMA_ONLY("schema_only"),

        /**
         * Perform a snapshot of the schema but no data upon initial startup of a connector.
         */
        NO_DATA("no_data"),

        /**
         * Perform a snapshot of only the database schemas (without data) and then begin reading the redo log at the current redo log position.
         * This can be used for recovery only if the connector has existing offsets and the schema.history.internal.kafka.topic does not exist (deleted).
         * This recovery option should be used with care as it assumes there have been no schema changes since the connector last stopped,
         * otherwise some events during the gap may be processed with an incorrect schema and corrupted.
         *
         * @deprecated to be removed in Debezium 3.0, replaced by {{@link #RECOVERY}}
         */
        SCHEMA_ONLY_RECOVERY("schema_only_recovery"),

        /**
         * Perform a snapshot of only the database schemas (without data) and then begin reading the redo log at the current redo log position.
         * This can be used for recovery only if the connector has existing offsets and the schema.history.internal.kafka.topic does not exist (deleted).
         * This recovery option should be used with care as it assumes there have been no schema changes since the connector last stopped,
         * otherwise some events during the gap may be processed with an incorrect schema and corrupted.
         */
        RECOVERY("recovery"),

        /**
         * Perform a snapshot when it is needed.
         */
        WHEN_NEEDED("when_needed"),

        /**
         * Allows control over snapshots by setting connectors properties prefixed with 'snapshot.mode.configuration.based'.
         */
        CONFIGURATION_BASED("configuration_based"),

        /**
         * Inject a custom snapshotter, which allows for more control over snapshots.
         */
        CUSTOM("custom");

        private final String value;

        SnapshotMode(String value) {
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
        NONE("none"),

        /**
         * Inject a custom mode, which allows for more control over snapshot locking.
         */
        CUSTOM("custom");

        private final String value;

        SnapshotLockingMode(String value) {
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

    /**
     * Controls how in-progress transactions that occur just before and at the snapshot boundary
     * are to be handled by the connector when transitioning to the streaming phase.
     */
    public enum TransactionSnapshotBoundaryMode implements EnumeratedValue {
        /**
         * Specifies that the in-progress transaction support at the snapshot boundary should be
         * skipped and that only transactions committed prior to the snapshot SCN and those that
         * are started after the snapshot SCN will be captured.
         */
        SKIP("skip"),

        /**
         * Specifies that in-progress transactions that are available in the {@code V$TRANSACTION}
         * table will be captured and emitted when streaming begins. If a transaction is not in
         * this view, and its changes were not captured by Oracle Flashback query based on the
         * snapshot SCN, that transaction will not be captured.
         */
        TRANSACTION_VIEW_ONLY("transaction_view_only"),

        /**
         * Specifies that in-progress transactions identified in the {@code V$TRANSACTION} table as
         * well as any in-progress transactions as of the current SCN that may have been committed
         * immediately prior to or at the snapshot SCN will be captured. This is done by starting a
         * special LogMiner session to gather these transactions prior to starting the snapshot.
         */
        ALL("all");

        private final String value;

        TransactionSnapshotBoundaryMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be {@code null}
         * @return the matching option, or null if no match is found
         */
        public static TransactionSnapshotBoundaryMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (TransactionSnapshotBoundaryMode option : TransactionSnapshotBoundaryMode.values()) {
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
        public static TransactionSnapshotBoundaryMode parse(String value, String defaultValue) {
            TransactionSnapshotBoundaryMode mode = parse(value);
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
                        OracleConnectorConfig.class,
                        connectorConfig);
            }
        },

        /**
         * This is based on OpenLogReplicator project.
         */
        OLR("OLR") {
            @Override
            public String getConnectionUrl() {
                return "jdbc:oracle:thin:@${" + JdbcConfiguration.HOSTNAME + "}:${" + JdbcConfiguration.PORT + "}/${" + JdbcConfiguration.DATABASE + "}";
            }

            @Override
            public StreamingAdapter getInstance(OracleConnectorConfig connectorConfig) {
                return Instantiator.getInstanceWithProvidedConstructorType(
                        "io.debezium.connector.oracle.olr.OpenLogReplicatorAdapter",
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
        CATALOG_IN_REDO("redo_log_catalog"),

        /**
         * This strategy uses LogMiner with data dictionary located in ORACLE read-only server.
         * This option need the path location of the dictionary file.
         * This option is a combination with the {@code redo_log_catalog} strategy.
         */
        DICTIONARY_FROM_FILE("dictionary_from_file"),

        /**
         * This strategy combines the performance of {@code online_catalog} with the schema capture capabilities of
         * the {@code redo_log_catalog} strategy. If LogMiner fails to reconstruct a DML event, this strategy will
         * default to using Debezium's schema metadata to reconstruct the DML in-flight when LogMiner cannot.
         */
        HYBRID("hybrid");

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
                                                          EventDispatcher<OraclePartition, TableId> dispatcher,
                                                          OraclePartition partition,
                                                          OracleOffsetContext offsetContext,
                                                          OracleDatabaseSchema schema,
                                                          LogMinerStreamingChangeEventSourceMetrics metrics) {
                return new MemoryLogMinerEventProcessor(context, connectorConfig, connection, dispatcher, partition,
                        offsetContext, schema, metrics);
            }
        },

        INFINISPAN_EMBEDDED("infinispan_embedded") {
            @Override
            public LogMinerEventProcessor createProcessor(ChangeEventSourceContext context,
                                                          OracleConnectorConfig connectorConfig,
                                                          OracleConnection connection,
                                                          EventDispatcher<OraclePartition, TableId> dispatcher,
                                                          OraclePartition partition,
                                                          OracleOffsetContext offsetContext,
                                                          OracleDatabaseSchema schema,
                                                          LogMinerStreamingChangeEventSourceMetrics metrics) {
                return new EmbeddedInfinispanLogMinerEventProcessor(context, connectorConfig, connection, dispatcher,
                        partition, offsetContext, schema, metrics);
            }
        },

        INFINISPAN_REMOTE("infinispan_remote") {
            @Override
            public LogMinerEventProcessor createProcessor(ChangeEventSourceContext context,
                                                          OracleConnectorConfig connectorConfig,
                                                          OracleConnection connection,
                                                          EventDispatcher<OraclePartition, TableId> dispatcher,
                                                          OraclePartition partition,
                                                          OracleOffsetContext offsetContext,
                                                          OracleDatabaseSchema schema,
                                                          LogMinerStreamingChangeEventSourceMetrics metrics) {
                return new RemoteInfinispanLogMinerEventProcessor(context, connectorConfig, connection, dispatcher,
                        partition, offsetContext, schema, metrics);
            }
        },

        EHCACHE("ehcache") {
            @Override
            public LogMinerEventProcessor createProcessor(ChangeEventSourceContext context,
                                                          OracleConnectorConfig connectorConfig,
                                                          OracleConnection connection,
                                                          EventDispatcher<OraclePartition, TableId> dispatcher,
                                                          OraclePartition partition,
                                                          OracleOffsetContext offsetContext,
                                                          OracleDatabaseSchema schema,
                                                          LogMinerStreamingChangeEventSourceMetrics metrics) {
                return new EhcacheLogMinerEventProcessor(context, connectorConfig, connection, dispatcher,
                        partition, offsetContext, schema, metrics);
            }
        };

        private final String value;

        /**
         * Creates the buffer type's specific processor implementation
         */
        public abstract LogMinerEventProcessor createProcessor(ChangeEventSourceContext context, OracleConnectorConfig connectorConfig,
                                                               OracleConnection connection, EventDispatcher<OraclePartition, TableId> dispatcher,
                                                               OraclePartition partition,
                                                               OracleOffsetContext offsetContext, OracleDatabaseSchema schema,
                                                               LogMinerStreamingChangeEventSourceMetrics metrics);

        LogMiningBufferType(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        public boolean isInfinispan() {
            return INFINISPAN_EMBEDDED.equals(this) || INFINISPAN_REMOTE.equals(this);
        }

        public boolean isInfinispanEmbedded() {
            return INFINISPAN_EMBEDDED.equals(this);
        }

        public boolean isEhcache() {
            return EHCACHE.equals(this);
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

    public enum LogMiningQueryFilterMode implements EnumeratedValue {
        /**
         * This filter mode does not add any predicates to the LogMiner query, all filtering of
         * change data is done at runtime in the connector's Java code. This is the default
         * mode.
         */
        NONE("none"),

        /**
         * This filter mode adds predicates to the LogMiner query, using standard SQL in-clause
         * semantics. This mode expects that the include/exclude connector properties specify
         * schemas and tables without regular expressions.
         *
         * This option may be the best performing option when there is substantially more data in
         * the redo logs compared to the data wanting to be captured at the trade-off that the
         * connector configuration is a bit more verbose with include/exclude filters.
         */
        IN("in"),

        /**
         * This filter mode adds predicates to the LogMiner query, using the Oracle REGEXP_LIKE
         * operator. This mode supports the include/exclude connector properties specifying
         * regular expressions.
         *
         * For the best performance, it's generally a good idea to limit the number of REGEXP_LIKE
         * operators in the query as it's treated similar to the LIKE operator which often does
         * not perform well on large data sets. The number of REGEXP_LIKE operators can be reduced
         * by specifying complex regular expressions where a single expression can potentially
         * match multiple schemas or tables.
         */
        REGEX("regex");

        private final String value;

        LogMiningQueryFilterMode(String value) {
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
        public static LogMiningQueryFilterMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (LogMiningQueryFilterMode mode : LogMiningQueryFilterMode.values()) {
                if (mode.getValue().equalsIgnoreCase(value)) {
                    return mode;
                }
            }
            return null;
        }
    }

    /**
     * A {@link TableFilter} that excludes all Oracle system tables.
     *
     * @author Gunnar Morling
     */
    private static class SystemTablesPredicate implements TableFilter {

        /**
         * Pattern that matches temporary analysis tables created by the Compression Advisor subsystem.
         * These tables will be ignored by the connector.
         */
        private final Pattern COMPRESSION_ADVISOR = Pattern.compile("^CMP[3|4]\\$[0-9]+$");

        private final Configuration config;

        SystemTablesPredicate(Configuration config) {
            this.config = config;
        }

        @Override
        public boolean isIncluded(TableId t) {
            return !isExcludedSchema(t) && !isFlushTable(t) && !isCompressionAdvisorTable(t);
        }

        private boolean isExcludedSchema(TableId id) {
            return EXCLUDED_SCHEMAS.contains(id.schema().toLowerCase());
        }

        private boolean isFlushTable(TableId id) {
            return LogWriterFlushStrategy.isFlushTable(id, config.getString(USER), config.getString(LOG_MINING_FLUSH_TABLE_NAME));
        }

        private boolean isCompressionAdvisorTable(TableId id) {
            return COMPRESSION_ADVISOR.matcher(id.table()).matches();
        }
    }

    @Override
    protected SourceInfoStructMaker<? extends AbstractSourceInfo> getSourceInfoStructMaker(Version version) {
        return getSourceInfoStructMaker(SOURCE_INFO_STRUCT_MAKER, Module.name(), Module.version(), this);
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
     * @return the duration that archive logs are scanned for log mining
     */
    public Duration getArchiveLogRetention() {
        return archiveLogRetention;
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
     * @return int The maximum SCN interval used when mining redo/archive logs
     */
    public int getLogMiningBatchSizeMax() {
        return logMiningBatchSizeMax;
    }

    /**
     * @return the size to increment/decrement log mining batches
     */
    public int getLogMiningBatchSizeIncrement() {
        return logMiningBatchSizeIncrement;
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
     * @return User names to include from the LogMiner query
     */
    public Set<String> getLogMiningUsernameIncludes() {
        return logMiningUsernameIncludes;
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
    public String getArchiveLogDestinationName() {
        return archiveLogDestinationName;
    }

    /**
     * @return the log mining buffer type implementation to be used
     */
    public LogMiningBufferType getLogMiningBufferType() {
        return logMiningBufferType;
    }

    /**
     * @return the event count threshold for when a transaction should be discarded in the buffer.
     */
    public long getLogMiningBufferTransactionEventsThreshold() {
        return logMiningBufferTransactionEventsThreshold;
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

    /**
     * @return the maximum number of retries that should be used to resolve log filenames for mining
     */
    public int getMaximumNumberOfLogQueryRetries() {
        return logMiningLogFileQueryMaxRetries;
    }

    /**
     * @return the initial delay for the log query delay strategy
     */
    public Duration getLogMiningInitialDelay() {
        return logMiningInitialDelay;
    }

    /**
     * @return the maximum delay for the log query delay strategy
     */
    public Duration getLogMiningMaxDelay() {
        return logMiningMaxDelay;
    }

    /**
     * @return the infinispan global conifguration.
     */
    public String getLogMiningInifispanGlobalConfiguration() {
        return logMiningInifispanGlobalConfiguration;
    }

    /**
     * @return the maximum duration for a LogMiner session
     */
    public Optional<Duration> getLogMiningMaximumSession() {
        return logMiningMaximumSession.toMillis() == 0L ? Optional.empty() : Optional.of(logMiningMaximumSession);
    }

    /**
     * @return how in-progress transactions are the snapshot boundary are to be handled.
     */
    public TransactionSnapshotBoundaryMode getLogMiningTransactionSnapshotBoundaryMode() {
        return logMiningTransactionSnapshotBoundaryMode;
    }

    /**
     * @return true if log mining should operate in read-only mode.
     */
    public boolean isLogMiningReadOnly() {
        return logMiningReadOnly;
    }

    /**
     * @return the log mining flush table name
     */
    public String getLogMiningFlushTableName() {
        return logMiningFlushTableName;
    }

    /**
     * @return how the LogMiner query include/exclude filters are applied to the query.
     */
    public LogMiningQueryFilterMode getLogMiningQueryFilterMode() {
        return logMiningQueryFilterMode;
    }

    /**
     * @return whether the connector should restart the JDBC connection after log switches or maximum session windows.
     */
    public boolean isLogMiningRestartConnection() {
        return logMiningRestartConnection;
    }

    /**
     * Returns the deviation in milliseconds that should be applied to the end SCN calculation.
     * If this is {@code 0}, then there is no deviation applied.
     *
     * @return the deviation duration.
     */
    public Duration getLogMiningMaxScnDeviation() {
        return logMiningMaxScnDeviation;
    }

    /**
     * Returns the list of usernames that should have schema changes excluded for.
     *
     * @return set of usernames
     */
    public Set<String> getLogMiningSchemaChangesUsernameExcludes() {
        return logMiningSchemaChangesUsernameExcludes;
    }

    /**
     * Returns whether to include the redo SQL in the source information block.
     *
     * @return if redo SQL is included in change events
     */
    public boolean isLogMiningIncludeRedoSql() {
        return logMiningIncludeRedoSql;
    }

    /**
     * Returns whether the LogMiner adapter should use continuous mining or not.
     *
     * @return true continuous mining should be used
     */
    @Deprecated
    public boolean isLogMiningContinuousMining() {
        return logMiningContinuousMining;
    }

    /**
     * Returns the logical source to stream changes from when connecting to OpenLogReplicator.
     *
     * @return the logical source name
     */
    public String getOpenLogReplicatorSource() {
        return openLogReplicatorSource;
    }

    /**
     * Returns the hostname of the OpenLogReplicator network service.
     *
     * @return the hostname of the service
     */
    public String getOpenLogReplicatorHostname() {
        return openLogReplicatorHostname;
    }

    /**
     * Return the port of the OpenLogReplicator network service.
     *
     * @return the port of the service
     */
    public Integer getOpenLogReplicatorPort() {
        return openLogReplicatorPort;
    }

    /**
     * Return the log mining path to dictionary.
     *
     * @return the dictionary path
     */
    public String getLogMiningPathToDictionary() {
        return logMiningPathToDictionary;
    }

    /**
     * Return the read-only database hostname.
     *
     * @return the read-only hostname
     */
    public String getReadonlyHostname() {
        return readonlyHostname;
    }

    /**
     * Get the Ehcache buffer configuration, which is all attributes under the configuration prefix
     * "log.mining.buffer.ehcache" namespace, with the prefix removed.
     *
      * @return the ehcache transaction buffer configuration, never {@code null}
     */
    public Configuration getLogMiningEhcacheConfiguration() {
        return logMiningEhCacheConfiguration;
    }

    /**
     * Return the object id to table id cache size
     *
     * @return the maximum size of the object id to table id cache
     */
    public int getObjectIdToTableIdCacheSize() {
        return objectIdToTableIdCacheSize;
    }

    /**
     * Return whether the DML parser should use relaxed quote detection.
     *
     * @return true to use relaxed quote detection, false uses strict parsing rules
     */
    public boolean getLogMiningUseSqlRelaxedQuoteDetection() {
        return logMiningUseSqlRelaxedQuoteDetection;
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

    public static int validateDictionaryFromFile(Configuration config, Field field, ValidationOutput problems) {
        // Validates that the field is required but only when the LogMiner strategy is set to DICTIONARY_FROM_FILE
        if (LogMiningStrategy.DICTIONARY_FROM_FILE.equals(LogMiningStrategy.parse(config.getString(LOG_MINING_STRATEGY)))) {
            return Field.isRequired(config, field, problems);
        }
        return 0;
    }

    private static int validateLogMiningBufferType(Configuration config, Field field, ValidationOutput problems) {
        final LogMiningBufferType bufferType = LogMiningBufferType.parse(config.getString(LOG_MINING_BUFFER_TYPE));
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

    public static int validateLogMiningReadOnly(Configuration config, Field field, ValidationOutput problems) {
        if (config.getBoolean(LOG_MINING_READ_ONLY)) {
            LOGGER.warn("When using '{}', the LogMiner tablespace requires write access for the Oracle background LogMiner process; however, " +
                    "the connector itself will not perform any write operations against the database.", LOG_MINING_READ_ONLY.name());
            final Set<String> racNodes = Strings.setOf(config.getString(RAC_NODES), String::new);
            if (!racNodes.isEmpty()) {
                LOGGER.warn("The property '{}' is set, but is ignored due to using read-only mode.", RAC_NODES.name());
            }
        }
        return 0;
    }

    public static int validateLogMiningFlushTableName(Configuration config, Field field, ValidationOutput problems) {
        if (config.getBoolean(LOG_MINING_READ_ONLY)) {
            // This option is not required when using read-only mode
            return 0;
        }
        else if (ConnectorAdapter.XSTREAM.equals(ConnectorAdapter.parse(config.getString(CONNECTOR_ADAPTER)))) {
            // This option is not required when using XStream
            return 0;
        }
        return Field.isRequired(config, field, problems);
    }

    public static int validateUsernameExcludeList(Configuration config, Field field, ValidationOutput problems) {
        final String includeList = config.getString(LOG_MINING_USERNAME_INCLUDE_LIST);
        final String excludeList = config.getString(LOG_MINING_USERNAME_EXCLUDE_LIST);

        if (includeList != null && excludeList != null) {
            problems.accept(TABLE_EXCLUDE_LIST, excludeList,
                    String.format("\"%s\" is already specified", LOG_MINING_USERNAME_INCLUDE_LIST.name()));
            return 1;
        }
        return 0;
    }

    public static int validateRequiredWhenUsingOpenLogReplicator(Configuration config, Field field, ValidationOutput problems) {
        if (ConnectorAdapter.OLR.equals(ConnectorAdapter.parse(config.getString(CONNECTOR_ADAPTER)))) {
            return Field.isRequired(config, field, problems);
        }
        return 0;
    }

    public static int validateLogMiningIncludeRedoSql(Configuration config, Field field, ValidationOutput problems) {
        if (config.getBoolean(field)) {
            if (ConnectorAdapter.LOG_MINER.equals(ConnectorAdapter.parse(config.getString(CONNECTOR_ADAPTER)))) {
                if (config.getBoolean(LOB_ENABLED)) {
                    problems.accept(field, config.getBoolean(field), String.format(
                            "The configuration property '%s' cannot be enabled when '%s' is set to true.",
                            field.name(), LOB_ENABLED.name()));
                    return 1;
                }
            }
            else {
                problems.accept(field, config.getBoolean(field), String.format(
                        "The configuration property '%s' requires '%s' set to '%s' and without '%s' enabled.",
                        field.name(), CONNECTOR_ADAPTER.name(), ConnectorAdapter.LOG_MINER.getValue(), LOB_ENABLED.name()));
                return 1;
            }
        }
        return 0;
    }

    public static int validateLogMiningStrategy(Configuration config, Field field, ValidationOutput problems) {
        if (ConnectorAdapter.LOG_MINER.equals(ConnectorAdapter.parse(config.getString(CONNECTOR_ADAPTER)))) {
            if (config.getBoolean(LOB_ENABLED)) {
                // When LOB is enabled, the combination is not valid with the hybrid strategy.
                // This is because we currently are not capable of decoding all LOB-based operations in
                // the LogMiner event stream to support CLOB, NCLOB, BLOB, XML, and JSON just yet.
                // This is an ongoing, work-in-progress strategy.
                final String strategy = config.getString(LOG_MINING_STRATEGY);
                if (LogMiningStrategy.HYBRID.equals(LogMiningStrategy.parse(strategy))) {
                    problems.accept(LOG_MINING_STRATEGY, strategy,
                            String.format("The hybrid mining strategy is not compatible when enabling '%s'. " +
                                    "Please use a different '%s' or do not enable '%s'.",
                                    LOB_ENABLED.name(), LOG_MINING_STRATEGY.name(), LOB_ENABLED.name()));
                    return 1;
                }
            }
        }
        return 0;
    }

    public static int validateEhCacheGlobalConfigField(Configuration config, Field field, ValidationOutput problems) {
        if (ConnectorAdapter.LOG_MINER.equals(ConnectorAdapter.parse(config.getString(CONNECTOR_ADAPTER)))) {
            if (LogMiningBufferType.parse(config.getString(LOG_MINING_BUFFER_TYPE)).isEhcache()) {
                // The string cannot include any `<cache ` or `<default-serializers` tags.
                final String globalConfig = config.getString(LOG_MINING_BUFFER_EHCACHE_GLOBAL_CONFIG, "").toLowerCase();
                if (!Strings.isNullOrEmpty(globalConfig)) {
                    if (globalConfig.contains("<cache") || globalConfig.contains("<default-serializers")) {
                        problems.accept(LOG_MINING_BUFFER_EHCACHE_GLOBAL_CONFIG, globalConfig,
                                "The ehcache global configuration should not contain a <cache/> or <default-serializers/> section");
                        return 1;
                    }
                }
            }
        }
        return 0;
    }

    public static int validateEhcacheConfigFieldRequired(Configuration config, Field field, ValidationOutput problems) {
        if (ConnectorAdapter.LOG_MINER.equals(ConnectorAdapter.parse(config.getString(CONNECTOR_ADAPTER)))) {
            if (LogMiningBufferType.parse(config.getString(LOG_MINING_BUFFER_TYPE)).isEhcache()) {
                return Field.isRequired(config, field, problems);
            }
        }
        return 0;
    }

    public static int validateObjectIdCacheSize(Configuration config, Field field, ValidationOutput problems) {
        int result = Field.isRequired(config, field, problems);
        if (result != 0) {
            return result;
        }
        return Field.isPositiveInteger(config, field, problems);
    }
}
