/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.util.Optional;
import java.util.function.Predicate;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
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
import io.debezium.connector.mysql.charset.MySqlCharsetRegistryServiceProvider;
import io.debezium.connector.mysql.gtid.MySqlGtidSetFactory;
import io.debezium.connector.mysql.history.MySqlHistoryRecordComparator;
import io.debezium.function.Predicates;
import io.debezium.relational.history.HistoryRecordComparator;

/**
 * The configuration properties.
 */
public class MySqlConnectorConfig extends BinlogConnectorConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySqlConnectorConfig.class);

    /**
     * The set of predefined Snapshot Locking Mode options.
     */
    public enum SnapshotLockingMode implements EnumeratedValue {

        /**
         * This mode will block all writes for the entire duration of the snapshot.
         *
         * Replaces deprecated configuration option snapshot.locking.minimal with a value of false.
         */
        EXTENDED("extended"),

        /**
         * The connector holds the global read lock for just the initial portion of the snapshot while the connector reads the database
         * schemas and other metadata. The remaining work in a snapshot involves selecting all rows from each table, and this can be done
         * in a consistent fashion using the REPEATABLE READ transaction even when the global read lock is no longer held and while other
         * MySQL clients are updating the database.
         *
         * Replaces deprecated configuration option snapshot.locking.minimal with a value of true.
         */
        MINIMAL("minimal"),

        /**
         * The connector holds a (Percona-specific) backup lock for just the initial portion of the snapshot while the connector
         * reads the database schemas and other metadata. This lock will only block DDL and DML on non-transactional tables
         * (MyISAM etc.). The remaining work in a snapshot involves selecting all rows from each table, and this can be done in a
         * consistent fashion using the REPEATABLE READ transaction even when the global read lock is no longer held and while other
         * MySQL clients are updating the database.
         */
        MINIMAL_PERCONA("minimal_percona"),

        /**
         * Similar to MINIMAL_PERCONA mode but prevents any table-level locks. The connector will fail if it cannot acquire a global lock.
         * This mode ensures table locks are never used, which can be important in environments where table locks are not allowed
         * or can cause issues.
         */
        MINIMAL_PERCONA_NO_TABLE_LOCKS("minimal_percona_no_table_locks"),

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
            return value.equals(MINIMAL.value) || value.equals(MINIMAL_PERCONA.value) || value.equals(MINIMAL_PERCONA_NO_TABLE_LOCKS.value);
        }

        public boolean usesLocking() {
            return !value.equals(NONE.value);
        }

        public boolean flushResetsIsolationLevel() {
            return !value.equals(MINIMAL_PERCONA.value) && !value.equals(MINIMAL_PERCONA_NO_TABLE_LOCKS.value);
        }

        public boolean preventsTableLocks() {
            return value.equals(MINIMAL_PERCONA_NO_TABLE_LOCKS.value);
        }

        public boolean useSingleTransaction() {
            return value.equals(MINIMAL.value) || value.equals(MINIMAL_PERCONA.value) || value.equals(MINIMAL_PERCONA_NO_TABLE_LOCKS.value);
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

    public enum MySqlSecureConnectionMode implements SecureConnectionMode, EnumeratedValue {
        /**
         * Establish an unencrypted connection.
         */
        DISABLED("disabled"),
        /**
         * Establish a secure (encrypted) connection if the server supports secure connections.
         * Fall back to an unencrypted connection otherwise.
         */
        PREFERRED("preferred"),
        /**
         * Establish a secure connection if the server supports secure connections.
         * The connection attempt fails if a secure connection cannot be established.
         */
        REQUIRED("required"),
        /**
         * Like REQUIRED, but additionally verify the server TLS certificate against the configured Certificate Authority
         * (CA) certificates. The connection attempt fails if no valid matching CA certificates are found.
         */
        VERIFY_CA("verify_ca"),
        /**
         * Like VERIFY_CA, but additionally verify that the server certificate matches the host to which the connection is
         * attempted.
         */
        VERIFY_IDENTITY("verify_identity");

        private final String value;

        MySqlSecureConnectionMode(String value) {
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
        public static BinlogConnectorConfig.SecureConnectionMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (MySqlSecureConnectionMode option : MySqlSecureConnectionMode.values()) {
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
        public static BinlogConnectorConfig.SecureConnectionMode parse(String value, String defaultValue) {
            BinlogConnectorConfig.SecureConnectionMode mode = parse(value);
            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }
            return mode;
        }
    }

    /**
     * {@link Integer#MIN_VALUE Minimum value} used for fetch size hint.
     * See <a href="https://issues.jboss.org/browse/DBZ-94">DBZ-94</a> for details.
     */
    protected static final int DEFAULT_SNAPSHOT_FETCH_SIZE = Integer.MIN_VALUE;

    public static final Field JDBC_DRIVER = Field.create(DATABASE_CONFIG_PREFIX + "jdbc.driver")
            .withDisplayName("JDBC Driver Class Name")
            .withType(Type.CLASS)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 41))
            .withWidth(Width.MEDIUM)
            .withDefault(com.mysql.cj.jdbc.Driver.class.getName())
            .withImportance(Importance.LOW)
            .withValidation(Field::isClassName)
            .withDescription("JDBC Driver class name used to connect to the MySQL database server.");

    public static final Field JDBC_PROTOCOL = Field.create(DATABASE_CONFIG_PREFIX + "protocol")
            .withDisplayName("JDBC Protocol")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 42))
            .withWidth(Width.MEDIUM)
            .withDefault("jdbc:mysql")
            .withImportance(Importance.LOW)
            .withDescription("JDBC protocol to use with the driver.");

    /**
     * A comma-separated list of regular expressions that match source UUIDs in the GTID set used to find the binlog
     * position in the MySQL server. Only the GTID ranges that have sources matching one of these include patterns will
     * be used.
     * Must not be used with {@link #GTID_SOURCE_EXCLUDES}.
     */
    public static final Field GTID_SOURCE_INCLUDES = BinlogConnectorConfig.GTID_SOURCE_INCLUDES
            .withDescription("The source UUIDs used to include GTID ranges when determine the starting "
                    + "position in the MySQL server's binlog.");

    /**
     * A comma-separated list of regular expressions that match source UUIDs in the GTID set used to find the binlog
     * position in the MySQL server. Only the GTID ranges that have sources matching none of these exclude patterns will
     * be used.
     * Must not be used with {@link #GTID_SOURCE_INCLUDES}.
     */
    public static final Field GTID_SOURCE_EXCLUDES = BinlogConnectorConfig.GTID_SOURCE_EXCLUDES
            .withDescription("The source UUIDs used to exclude GTID ranges when determine the starting "
                    + "position in the MySQL server's binlog.");

    public static final Field SNAPSHOT_LOCKING_MODE = Field.create(SNAPSHOT_LOCKING_MODE_PROPERTY_NAME)
            .withDisplayName("Snapshot locking mode")
            .withEnum(SnapshotLockingMode.class, SnapshotLockingMode.MINIMAL)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_SNAPSHOT, 1))
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("Controls how long the connector holds onto the global read lock while it is performing a snapshot. The default is 'minimal', "
                    + "which means the connector holds the global read lock (and thus prevents any updates) for just the initial portion of the snapshot "
                    + "while the database schemas and other metadata are being read. The remaining work in a snapshot involves selecting all rows from "
                    + "each table, and this can be done using the snapshot process' REPEATABLE READ transaction even when the lock is no longer held and "
                    + "other operations are updating the database. However, in some cases it may be desirable to block all writes for the entire duration "
                    + "of the snapshot; in such cases set this property to 'extended'. Using a value of 'none' will prevent the connector from acquiring any "
                    + "table locks during the snapshot process. This mode can only be used in combination with snapshot.mode values of 'schema_only' or "
                    + "'schema_only_recovery' and is only safe to use if no schema changes are happening while the snapshot is taken.")
            .withValidation(MySqlConnectorConfig::validateSnapshotLockingMode);

    public static final Field SSL_MODE = Field.create("database.ssl.mode")
            .withDisplayName("SSL mode")
            .withEnum(MySqlSecureConnectionMode.class, MySqlSecureConnectionMode.PREFERRED)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_SSL, 0))
            .withDescription("Whether to use an encrypted connection to the database. Options include: "
                    + "'disabled' to use an unencrypted connection; "
                    + "'preferred' (the default) to establish a secure (encrypted) connection if the server supports "
                    + "secure connections, but fall back to an unencrypted connection otherwise; "
                    + "'required' to use a secure (encrypted) connection, and fail if one cannot be established; "
                    + "'verify_ca' like 'required' but additionally verify the server TLS certificate against the "
                    + "configured Certificate Authority (CA) certificates, or fail if no valid matching CA certificates are found; or "
                    + "'verify_identity' like 'verify_ca' but additionally verify that the server certificate matches "
                    + "the host to which the connection is attempted.");

    public static final Field SOURCE_INFO_STRUCT_MAKER = CommonConnectorConfig.SOURCE_INFO_STRUCT_MAKER
            .withDefault(MySqlSourceInfoStructMaker.class.getName());

    private static final ConfigDefinition CONFIG_DEFINITION = BinlogConnectorConfig.CONFIG_DEFINITION.edit()
            .name("MySQL")
            .excluding(
                    BinlogConnectorConfig.GTID_SOURCE_INCLUDES,
                    BinlogConnectorConfig.GTID_SOURCE_EXCLUDES)
            .type(
                    JDBC_DRIVER,
                    JDBC_PROTOCOL,
                    SSL_MODE)
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
     * The set of {@link Field}s defined as part of this configuration.
     */
    public static Field.Set ALL_FIELDS = Field.setOf(CONFIG_DEFINITION.all());

    private final GtidSetFactory gtidSetFactory;
    private final Predicate<String> gtidSourceFilter;
    private final SnapshotLockingMode snapshotLockingMode;
    private final SnapshotLockingStrategy snapshotLockingStrategy;
    private final SecureConnectionMode secureConnectionMode;

    public MySqlConnectorConfig(Configuration config) {
        super(MySqlConnector.class, config, DEFAULT_SNAPSHOT_FETCH_SIZE);
        this.gtidSetFactory = new MySqlGtidSetFactory();

        this.snapshotLockingMode = SnapshotLockingMode.parse(config.getString(SNAPSHOT_LOCKING_MODE), SNAPSHOT_LOCKING_MODE.defaultValueAsString());
        this.snapshotLockingStrategy = new MySqlSnapshotLockingStrategy(snapshotLockingMode);

        this.secureConnectionMode = MySqlSecureConnectionMode.parse(config.getString(SSL_MODE));

        // Set up the GTID filter ...
        final String gtidSetIncludes = config.getString(GTID_SOURCE_INCLUDES);
        final String gtidSetExcludes = config.getString(GTID_SOURCE_EXCLUDES);
        this.gtidSourceFilter = gtidSetIncludes != null ? Predicates.includesUuids(gtidSetIncludes)
                : (gtidSetExcludes != null ? Predicates.excludesUuids(gtidSetExcludes) : null);

        getServiceRegistry().registerServiceProvider(new MySqlCharsetRegistryServiceProvider());
    }

    public Optional<SnapshotLockingMode> getSnapshotLockingMode() {
        return Optional.of(this.snapshotLockingMode);
    }

    @Override
    protected SnapshotLockingStrategy getSnapshotLockingStrategy() {
        return snapshotLockingStrategy;
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
    public HistoryRecordComparator getHistoryRecordComparator() {
        return new MySqlHistoryRecordComparator(gtidSourceFilter, getGtidSetFactory());
    }

    @Override
    public SecureConnectionMode getSslMode() {
        return secureConnectionMode;
    }

    @Override
    public boolean isSslModeEnabled() {
        return secureConnectionMode != MySqlSecureConnectionMode.DISABLED;
    }

    /**
     * Custom {@link io.debezium.connector.binlog.BinlogConnectorConfig.SnapshotLockingStrategy} for MySQL.
     */
    public static class MySqlSnapshotLockingStrategy implements SnapshotLockingStrategy {

        private final SnapshotLockingMode snapshotLockingMode;

        public MySqlSnapshotLockingStrategy(SnapshotLockingMode snapshotLockingMode) {
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

        @Override
        public boolean preventsTableLocks() {
            return snapshotLockingMode.preventsTableLocks();
        }

        @Override
        public boolean isSingleTransaction() {
            return snapshotLockingMode.useSingleTransaction();
        }
    }

    /**
     * Validate the new snapshot.locking.mode configuration, which replaces snapshot.minimal.locking.
     *
     * If minimal.locking is explicitly defined and locking.mode is NOT explicitly defined:
     *   - coerce minimal.locking into the new snap.locking.mode property.
     *
     * If minimal.locking is NOT explicitly defined and locking.mode IS explicitly defined:
     *   - use new locking.mode property.
     *
     * If BOTH minimal.locking and locking.mode ARE defined:
     *   - Throw a validation error.
     */
    private static int validateSnapshotLockingMode(Configuration config, Field field, ValidationOutput problems) {
        // Determine which configurations are explicitly defined
        if (config.hasKey(SNAPSHOT_LOCKING_MODE.name())) {
            final SnapshotLockingMode lockingModeValue = SnapshotLockingMode.parse(
                    config.getString(MySqlConnectorConfig.SNAPSHOT_LOCKING_MODE));
            // Sanity check, validate the configured value is a valid option.
            if (lockingModeValue == null) {
                problems.accept(SNAPSHOT_LOCKING_MODE, lockingModeValue, "Must be a valid snapshot.locking.mode value");
                return 1;
            }
        }

        // Everything checks out ok.
        return 0;
    }
}
