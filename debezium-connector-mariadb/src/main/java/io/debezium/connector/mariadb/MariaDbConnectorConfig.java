/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import java.util.Optional;
import java.util.function.Predicate;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.ConfigDefinition;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.config.Field.ValidationOutput;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.gtid.GtidSetFactory;
import io.debezium.connector.mariadb.charset.MariaDbCharsetRegistryServiceProvider;
import io.debezium.connector.mariadb.gtid.MariaDbGtidSetFactory;
import io.debezium.connector.mariadb.history.MariaDbHistoryRecordComparator;
import io.debezium.function.Predicates;
import io.debezium.relational.history.HistoryRecordComparator;

/**
 * Configuration properties for MariaDB.
 *
 * @author Chris Cranford
 */
public class MariaDbConnectorConfig extends BinlogConnectorConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(MariaDbConnectorConfig.class);

    /**
     * For MariaDB to mimic MySQL behavior using {@link Integer#MIN_VALUE}, the default fetch
     * size must explicitly be set to {@code 1}. This is because MariaDB drivers 3.x+ do not
     * support the old non-compliant JDBC-spec style that MySQL uses.
     */
    private static final int DEFAULT_NON_STREAMING_FETCH_SIZE = 1;

    public static final Field SOURCE_INFO_STRUCT_MAKER = CommonConnectorConfig.SOURCE_INFO_STRUCT_MAKER
            .withDefault(MariaDbSourceInfoStructMaker.class.getName());

    /**
     * The set of predefined snapshot locking mode options.
     */
    public enum SnapshotLockingMode implements EnumeratedValue {
        /**
         * This mode will block all writes for the entire duration of the snapshot.<p></p>
         *
         * Replaces deprecated configuration option snapshot.locking.minimal with a value of false.
         */
        EXTENDED("extended"),
        /**
         * The connector holds the global read lock for just the initial portion of the snapshot while the connector reads the database
         * schemas and other metadata. The remaining work in a snapshot involves selecting all rows from each table, and this can be done
         * in a consistent fashion using the REPEATABLE READ transaction even when the global read lock is no longer held and while other
         * MySQL clients are updating the database.<p></p>
         *
         * Replaces deprecated configuration option snapshot.locking.minimal with a value of true.
         */
        MINIMAL("minimal"),
        /**
         * This mode will avoid using ANY table locks during the snapshot process.  This mode can only be used with SnapShotMode
         * set to schema_only or schema_only_recovery.
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

        public boolean usesMinimalLocking() {
            return value.equals(MINIMAL.value);
        }

        public boolean usesLocking() {
            return !value.equals(NONE.value);
        }

        public boolean flushResetsIsolationLevel() {
            return true;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
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
         * @param value the configuration property value; may not be null
         * @param defaultValue the default value; may be null
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

    public static final Field SNAPSHOT_LOCKING_MODE = Field.create(SNAPSHOT_LOCKING_MODE_PROPERTY_NAME)
            .withDisplayName("Snapshot locking mode")
            .withEnum(SnapshotLockingMode.class, SnapshotLockingMode.MINIMAL)
            .withImportance(Importance.LOW)
            .withDescription("Controls how long the connector holds onto the global read lock while it is performing "
                    + "a snapshot. The default is 'minimal', meaning the connector holds the global read lock (and thus "
                    + "prevents updates) for just the initial portion of the snapshot while the database schemas and other "
                    + "metadata are being read. The remaining work in a snapshot involves selecting all rows from each table, "
                    + "and this can be done using the snapshot process' REPEATABLE READ transaction isolation even when the "
                    + "lock is no longer held and other operations are updating the database. However, in some cases it may "
                    + "be desirable to block all writes for the entire duration of the snapshot; in such cases set this "
                    + "to 'extended'. Using a value of 'none' will prevent the connector from acquiring any table locks "
                    + "during the snapshot process. This mode can only be used in combination with snapshot.mode values of "
                    + "'schema_only' or 'schema_only_recovery' and is only safe to use if no schema changes are happening "
                    + "while the snapshot is taken.")
            .withValidation(MariaDbConnectorConfig::validateSnapshotLockingMode);

    /**
     * MariaDB GTID format uses "{@code domain-server-sequence}". This configuration should specify a
     * comma-separated list of regular expressions that match the "{@code domain-server}" tuples when
     * locating the binlog position in a MariaDB server. Only the GTID ranges that have sources that
     * match one of these patterns will be used.
     */
    public static final Field GTID_SOURCE_INCLUDES = BinlogConnectorConfig.GTID_SOURCE_INCLUDES
            .withDescription("The source domain IDs used to include GTID ranges when determining the starting "
                    + "position in the MariaDB server's binlog.");

    /**
     * MariaDB GTID format uses "{@code domain-server-sequence}". This configuration should specify a
     * comma-separataed list of regular expressions that match the "{@code domain-server}" tuples when
     * locating the binlog position in a MariaDB server. GTIDs that do not match any of these patterns
     * will be used.
     */
    public static final Field GTID_SOURCE_EXCLUDES = BinlogConnectorConfig.GTID_SOURCE_EXCLUDES
            .withDescription("The source domain IDs used to exclude GTID ranges when determining the starting "
                    + "position in the MariaDB server's binlog.");

    private static final ConfigDefinition CONFIG_DEFINITION = BinlogConnectorConfig.CONFIG_DEFINITION.edit()
            .name("MariaDB")
            .excluding(
                    BinlogConnectorConfig.GTID_SOURCE_INCLUDES,
                    BinlogConnectorConfig.GTID_SOURCE_EXCLUDES)
            .connector(SNAPSHOT_LOCKING_MODE)
            .events(
                    GTID_SOURCE_INCLUDES,
                    GTID_SOURCE_EXCLUDES,
                    SOURCE_INFO_STRUCT_MAKER)
            .create();

    protected static ConfigDef configDef() {
        return CONFIG_DEFINITION.configDef();
    }

    /**
     * The set of {@link Field}s defined as part of this connector configuration.
     */
    public static Field.Set ALL_FIELDS = Field.setOf(CONFIG_DEFINITION.all());

    private final GtidSetFactory gtidSetFactory;
    private final Predicate<String> gtidSourceFilter;
    private final SnapshotLockingMode snapshotLockingMode;
    private final SnapshotLockingStrategy snapshotLockingStrategy;

    public MariaDbConnectorConfig(Configuration config) {
        super(MariaDbConnector.class, config, DEFAULT_NON_STREAMING_FETCH_SIZE);
        this.gtidSetFactory = new MariaDbGtidSetFactory();

        final String gtidIncludes = config.getString(GTID_SOURCE_INCLUDES);
        final String gtidExcludes = config.getString(GTID_SOURCE_EXCLUDES);
        this.gtidSourceFilter = gtidIncludes != null ? Predicates.includes(gtidIncludes)
                : (gtidExcludes != null ? Predicates.excludes(gtidExcludes) : null);

        this.snapshotLockingMode = SnapshotLockingMode.parse(config.getString(SNAPSHOT_LOCKING_MODE));
        this.snapshotLockingStrategy = new MariaDbSnapshotLockingStrategy(snapshotLockingMode);

        getServiceRegistry().registerServiceProvider(new MariaDbCharsetRegistryServiceProvider());
    }

    @Override
    protected SourceInfoStructMaker<? extends AbstractSourceInfo> getSourceInfoStructMaker(Version version) {
        return getSourceInfoStructMaker(SOURCE_INFO_STRUCT_MAKER, Module.name(), Module.version(), this);
    }

    @Override
    public String getContextName() {
        return Module.contextName();
    }

    @Override
    public String getConnectorName() {
        return Module.name();
    }

    @Override
    public Predicate<String> getGtidSourceFilter() {
        return gtidSourceFilter;
    }

    @Override
    public GtidSetFactory getGtidSetFactory() {
        return gtidSetFactory;
    }

    @Override
    protected HistoryRecordComparator getHistoryRecordComparator() {
        return new MariaDbHistoryRecordComparator(gtidSourceFilter, getGtidSetFactory());
    }

    @Override
    protected SnapshotLockingStrategy getSnapshotLockingStrategy() {
        return snapshotLockingStrategy;
    }

    @Override
    public Optional<SnapshotLockingMode> getSnapshotLockingMode() {
        return Optional.of(snapshotLockingMode);
    }

    /**
     * Custom {@link io.debezium.connector.binlog.BinlogConnectorConfig.SnapshotLockingStrategy} for MariaDB.
     */
    public static class MariaDbSnapshotLockingStrategy implements SnapshotLockingStrategy {

        public final SnapshotLockingMode snapshotLockingMode;

        public MariaDbSnapshotLockingStrategy(SnapshotLockingMode snapshotLockingMode) {
            this.snapshotLockingMode = snapshotLockingMode;
        }

        @Override
        public boolean isLockingEnabled() {
            return snapshotLockingMode.usesLocking();
        }

        @Override
        public boolean isMinimalLockingEnabled() {
            return snapshotLockingMode.usesMinimalLocking();
        }

        @Override
        public boolean isIsolationLevelResetOnFlush() {
            return snapshotLockingMode.flushResetsIsolationLevel();
        }
    }

    /**
     * Validate the new snapshot.locking.mode configuration.
     *
     * @param config the connector configuration
     * @param field the field being validated
     * @param problems the validation output
     * @return the number of problems detected
     */
    private static int validateSnapshotLockingMode(Configuration config, Field field, ValidationOutput problems) {
        if (config.hasKey(SNAPSHOT_LOCKING_MODE.name())) {
            SnapshotLockingMode lockingModeValue = SnapshotLockingMode.parse(config.getString(SNAPSHOT_LOCKING_MODE));
            if (lockingModeValue == null) {
                problems.accept(SNAPSHOT_LOCKING_MODE, lockingModeValue, "Must be a valid snapshot.locking.mode value");
                return 1;
            }
        }
        return 0;
    }
}
