/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.document.Document;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.HistorizedRelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.relational.history.KafkaDatabaseHistory;
import oracle.streams.XStreamUtility;

/**
 * Connector configuration for Oracle.
 *
 * @author Gunnar Morling
 */
public class OracleConnectorConfig extends HistorizedRelationalDatabaseConnectorConfig {

    // TODO pull up to RelationalConnectorConfig
    public static final String DATABASE_CONFIG_PREFIX = "database.";

    public static final Field LOGICAL_NAME = Field.create("database.server.name")
            .withDisplayName("Namespace")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.HIGH)
            .withValidation(Field::isRequired)
            // TODO
            //.withValidation(Field::isRequired, MySqlConnectorConfig::validateServerNameIsDifferentFromHistoryTopicName)
            .withDescription("Unique name that identifies the database server and all recorded offsets, and"
                    + "that is used as a prefix for all schemas and topics.");

    public static final Field DATABASE_NAME = Field.create(DATABASE_CONFIG_PREFIX + JdbcConfiguration.DATABASE)
            .withDisplayName("Database name")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.HIGH)
            .withValidation(Field::isRequired)
            .withDescription("The name of the database the connector should be monitoring. When working with a "
                    + "multi-tenant set-up, must be set to the CDB name.");

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
            .withValidation(Field::isRequired)
            .withDescription("Name of the XStream Out server to connect to.");

    public static final Field SNAPSHOT_MODE = Field.create("snapshot.mode")
            .withDisplayName("Snapshot mode")
            .withEnum(SnapshotMode.class, SnapshotMode.INITIAL)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("The criteria for running a snapshot upon startup of the connector. "
                    + "Options include: "
                    + "'initial' (the default) to specify the connector should run a snapshot only when no offsets are available for the logical server name; "
                    + "'initial_schema_only' to specify the connector should run a snapshot of the schema when no offsets are available for the logical server name. ");

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
        .withDescription("For default oracle 12+, use default pos_version value v2, for oracle 11, use pos_version value v1.");

    /**
     * The set of {@link Field}s defined as part of this configuration.
     */
    public static Field.Set ALL_FIELDS = Field.setOf(
            LOGICAL_NAME,
            DATABASE_NAME,
            PDB_NAME,
            XSTREAM_SERVER_NAME,
            SNAPSHOT_MODE,
            HistorizedRelationalDatabaseConnectorConfig.DATABASE_HISTORY,
            RelationalDatabaseConnectorConfig.TABLE_WHITELIST,
            RelationalDatabaseConnectorConfig.TABLE_BLACKLIST,
            RelationalDatabaseConnectorConfig.TABLE_IGNORE_BUILTIN,
            CommonConnectorConfig.POLL_INTERVAL_MS,
            CommonConnectorConfig.MAX_BATCH_SIZE,
            CommonConnectorConfig.MAX_QUEUE_SIZE,
            CommonConnectorConfig.SNAPSHOT_DELAY_MS,
            Heartbeat.HEARTBEAT_INTERVAL,
            Heartbeat.HEARTBEAT_TOPICS_PREFIX,
            TABLENAME_CASE_INSENSITIVE,
            ORACLE_VERSION
    );

    private final String databaseName;
    private final String pdbName;
    private final String xoutServerName;
    private final SnapshotMode snapshotMode;

    private final boolean tablenameCaseInsensitive;
    private final OracleVersion oracleVersion;

    public OracleConnectorConfig(Configuration config) {
        super(config, config.getString(LOGICAL_NAME), new SystemTablesPredicate());

        this.databaseName = config.getString(DATABASE_NAME);
        this.pdbName = config.getString(PDB_NAME);
        this.xoutServerName = config.getString(XSTREAM_SERVER_NAME);
        this.snapshotMode = SnapshotMode.parse(config.getString(SNAPSHOT_MODE));
        this.tablenameCaseInsensitive = config.getBoolean(TABLENAME_CASE_INSENSITIVE);
        this.oracleVersion = OracleVersion.parse(config.getString(ORACLE_VERSION));
    }

    public static ConfigDef configDef() {
        ConfigDef config = new ConfigDef();

        Field.group(config, "Oracle", LOGICAL_NAME, DATABASE_NAME, PDB_NAME, XSTREAM_SERVER_NAME, SNAPSHOT_MODE);
        Field.group(config, "History Storage", KafkaDatabaseHistory.BOOTSTRAP_SERVERS,
                KafkaDatabaseHistory.TOPIC, KafkaDatabaseHistory.RECOVERY_POLL_ATTEMPTS,
                KafkaDatabaseHistory.RECOVERY_POLL_INTERVAL_MS, HistorizedRelationalDatabaseConnectorConfig.DATABASE_HISTORY);
        Field.group(config, "Events", RelationalDatabaseConnectorConfig.TABLE_WHITELIST,
                RelationalDatabaseConnectorConfig.TABLE_BLACKLIST,
                RelationalDatabaseConnectorConfig.TABLE_IGNORE_BUILTIN,
                Heartbeat.HEARTBEAT_INTERVAL, Heartbeat.HEARTBEAT_TOPICS_PREFIX
        );
        Field.group(config, "Connector", CommonConnectorConfig.POLL_INTERVAL_MS, CommonConnectorConfig.MAX_BATCH_SIZE,
                CommonConnectorConfig.MAX_QUEUE_SIZE, CommonConnectorConfig.SNAPSHOT_DELAY_MS);

        return config;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getPdbName() {
        return pdbName;
    }

    public String getXoutServerName() {
        return xoutServerName;
    }

    public SnapshotMode getSnapshotMode() {
        return snapshotMode;
    }

    public boolean  getTablenameCaseInsensitive() {
        return tablenameCaseInsensitive;
    }

    public OracleVersion getOracleVersion() {
        return oracleVersion;
    }

    @Override
    protected HistoryRecordComparator getHistoryRecordComparator() {
        return new HistoryRecordComparator() {
            @Override
            protected boolean isPositionAtOrBefore(Document recorded, Document desired) {
                return (recorded.getLong(SourceInfo.SCN_KEY).compareTo(desired.getLong(SourceInfo.SCN_KEY)) < 1);
            }
        };
    }

    public static enum OracleVersion implements EnumeratedValue {

        V11("11"),
        V12Plus("12+");
        private final String version;

        private OracleVersion(String version) {
            this.version = version;
        }

        @Override
        public String getValue() {
            return version;
        }

        public int getPosVersion() {
            switch(version) {
                case "11": 
                    return XStreamUtility.POS_VERSION_V1;
                case "12+": 
                    return XStreamUtility.POS_VERSION_V2;
                default: 
                    return XStreamUtility.POS_VERSION_V2;
            }
        }

        public static OracleVersion parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();

            for (OracleVersion option : OracleVersion.values()) {
                if (option.getValue().equalsIgnoreCase(value)) return option;
            }

            return null;
        }

        public static OracleVersion parse(String value, String defaultValue) {
            OracleVersion option = parse(value);

            if (option == null && defaultValue != null) {
                option = parse(defaultValue);
            }

            return option;
        }
    }

    /**
     * The set of predefined SnapshotMode options or aliases.
     */
    public static enum SnapshotMode implements EnumeratedValue {

        /**
         * Perform a snapshot of data and schema upon initial startup of a connector.
         */
        INITIAL("initial", true),

        /**
         * Perform a snapshot of the schema but no data upon initial startup of a connector.
         */
        INITIAL_SCHEMA_ONLY("initial_schema_only", false);

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
                if (option.getValue().equalsIgnoreCase(value)) return option;
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
}
