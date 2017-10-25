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
import io.debezium.config.Field;
import io.debezium.jdbc.JdbcConfiguration;

public class OracleConnectorConfig extends CommonConnectorConfig {

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
            .withDescription("Name of the XStream Out server to connect to.");

    /**
     * The set of {@link Field}s defined as part of this configuration.
     */
    public static Field.Set ALL_FIELDS = Field.setOf(
            LOGICAL_NAME,
            DATABASE_NAME,
            PDB_NAME,
            XSTREAM_SERVER_NAME,
            CommonConnectorConfig.POLL_INTERVAL_MS,
            CommonConnectorConfig.MAX_BATCH_SIZE,
            CommonConnectorConfig.MAX_QUEUE_SIZE
    );

    private final String databaseName;
    private final String pdbName;
    private final String xoutServerName;

    public OracleConnectorConfig(Configuration config) {
        super(config, LOGICAL_NAME);

        this.databaseName = config.getString(DATABASE_NAME);
        this.pdbName = config.getString(PDB_NAME);
        this.xoutServerName = config.getString(XSTREAM_SERVER_NAME);
    }

    public static ConfigDef configDef() {
        ConfigDef config = new ConfigDef();

        Field.group(config, "Oracle", LOGICAL_NAME, DATABASE_NAME, PDB_NAME, XSTREAM_SERVER_NAME);
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
}
