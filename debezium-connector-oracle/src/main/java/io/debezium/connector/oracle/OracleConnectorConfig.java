/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.util.function.Predicate;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.connect.errors.ConnectException;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.document.Document;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.relational.history.DatabaseHistory;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.relational.history.KafkaDatabaseHistory;

public class OracleConnectorConfig extends RelationalDatabaseConnectorConfig {

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
                    + "that is used as a prefix for all schemas and topics. "
                    + "Each distinct MySQL installation should have a separate namespace and monitored by "
                    + "at most one Debezium connector.");

    public static final Field DATABASE_NAME = Field.create(DATABASE_CONFIG_PREFIX + JdbcConfiguration.DATABASE)
            .withDisplayName("Database name")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.HIGH)
            .withValidation(Field::isRequired)
            .withDescription("The name of the database the connector should be monitoring. When working with a "
                    + "multi-tenant set-up, must be set to the CDB name.");

    /**
     * The database history class is hidden in the {@link #configDef()} since that is designed to work with a user interface,
     * and in these situations using Kafka is the only way to go.
     */
    public static final Field DATABASE_HISTORY = Field.create("database.history")
            .withDisplayName("Database history class")
            .withType(Type.CLASS)
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withInvisibleRecommender()
            .withDescription("The name of the DatabaseHistory class that should be used to store and recover database schema changes. "
                    + "The configuration properties for the history are prefixed with the '"
                    + DatabaseHistory.CONFIGURATION_FIELD_PREFIX_STRING + "' string.")
            .withDefault(KafkaDatabaseHistory.class.getName());

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

    /**
     * The set of {@link Field}s defined as part of this configuration.
     */
    public static Field.Set ALL_FIELDS = Field.setOf(
            LOGICAL_NAME,
            DATABASE_NAME,
            PDB_NAME,
            XSTREAM_SERVER_NAME,
            RelationalDatabaseConnectorConfig.TABLE_WHITELIST,
            RelationalDatabaseConnectorConfig.TABLE_BLACKLIST,
            RelationalDatabaseConnectorConfig.TABLE_IGNORE_BUILTIN,
            CommonConnectorConfig.POLL_INTERVAL_MS,
            CommonConnectorConfig.MAX_BATCH_SIZE,
            CommonConnectorConfig.MAX_QUEUE_SIZE
    );

    private final String databaseName;
    private final String pdbName;
    private final String xoutServerName;

    public OracleConnectorConfig(Configuration config) {
        super(config, config.getString(LOGICAL_NAME), new SystemTablesPredicate());

        this.databaseName = config.getString(DATABASE_NAME);
        this.pdbName = config.getString(PDB_NAME);
        this.xoutServerName = config.getString(XSTREAM_SERVER_NAME);
    }

    public static ConfigDef configDef() {
        ConfigDef config = new ConfigDef();

        Field.group(config, "Oracle", LOGICAL_NAME, DATABASE_NAME, PDB_NAME, XSTREAM_SERVER_NAME);
        Field.group(config, "Events", RelationalDatabaseConnectorConfig.TABLE_WHITELIST,
                RelationalDatabaseConnectorConfig.TABLE_BLACKLIST,
                RelationalDatabaseConnectorConfig.TABLE_IGNORE_BUILTIN
        );
        Field.group(config, "Connector", CommonConnectorConfig.POLL_INTERVAL_MS, CommonConnectorConfig.MAX_BATCH_SIZE, CommonConnectorConfig.MAX_QUEUE_SIZE);

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

    /**
     * Returns a configured (but not yet started) instance of the database history.
     */
    public DatabaseHistory getDatabaseHistory() {
        Configuration config = getConfig();

        DatabaseHistory databaseHistory = config.getInstance(OracleConnectorConfig.DATABASE_HISTORY, DatabaseHistory.class);
        if (databaseHistory == null) {
            throw new ConnectException("Unable to instantiate the database history class " +
                    config.getString(OracleConnectorConfig.DATABASE_HISTORY));
        }

        // Do not remove the prefix from the subset of config properties ...
        Configuration dbHistoryConfig = config.subset(DatabaseHistory.CONFIGURATION_FIELD_PREFIX_STRING, false)
                                              .edit()
                                              .withDefault(DatabaseHistory.NAME, getLogicalName() + "-dbhistory")
                                              .build();

        HistoryRecordComparator historyComparator = new HistoryRecordComparator() {
            @Override
            protected boolean isPositionAtOrBefore(Document recorded, Document desired) {
                return (recorded.getLong("scn")).compareTo(desired.getLong("scn")) < 1;
            }
        };
        databaseHistory.configure(dbHistoryConfig, historyComparator); // validates

        return databaseHistory;
    }

    private static class SystemTablesPredicate implements Predicate<TableId> {

        @Override
        public boolean test(TableId t) {
            return t.schema().toLowerCase().equals("system") ||
                    t.schema().toLowerCase().equals("sys") ||
                    t.schema().toLowerCase().equals("mdsys") ||
                    t.schema().toLowerCase().equals("ctxsys") ||
                    t.schema().toLowerCase().equals("outln") ||
                    t.schema().toLowerCase().equals("xdb");
        }
    }
}
