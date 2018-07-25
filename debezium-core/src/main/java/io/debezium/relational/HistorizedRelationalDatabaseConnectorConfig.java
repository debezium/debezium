/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.connect.errors.ConnectException;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.relational.history.DatabaseHistory;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.relational.history.KafkaDatabaseHistory;

/**
 * Configuration options shared across the relational CDC connectors which use a persistent database schema history.
 *
 * @author Gunnar Morling
 */
public abstract class HistorizedRelationalDatabaseConnectorConfig extends RelationalDatabaseConnectorConfig {

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

    protected HistorizedRelationalDatabaseConnectorConfig(Configuration config, String logicalName, TableFilter systemTablesFilter) {
        super(config, logicalName, systemTablesFilter);
    }

    /**
     * Returns a configured (but not yet started) instance of the database history.
     */
    public DatabaseHistory getDatabaseHistory() {
        Configuration config = getConfig();

        DatabaseHistory databaseHistory = config.getInstance(HistorizedRelationalDatabaseConnectorConfig.DATABASE_HISTORY, DatabaseHistory.class);
        if (databaseHistory == null) {
            throw new ConnectException("Unable to instantiate the database history class " +
                    config.getString(HistorizedRelationalDatabaseConnectorConfig.DATABASE_HISTORY));
        }

        // Do not remove the prefix from the subset of config properties ...
        Configuration dbHistoryConfig = config.subset(DatabaseHistory.CONFIGURATION_FIELD_PREFIX_STRING, false)
                                              .edit()
                                              .withDefault(DatabaseHistory.NAME, getLogicalName() + "-dbhistory")
                                              .build();

        HistoryRecordComparator historyComparator = getHistoryRecordComparator();
        databaseHistory.configure(dbHistoryConfig, historyComparator); // validates

        return databaseHistory;
    }

    /**
     * Returns a comparator to be used when recovering records from the schema history, making sure no history entries
     * newer than the offset we resume from are recovered (which could happen when restarting a connector after history
     * records have been persisted but no new offset has been committed yet).
     */
    protected abstract HistoryRecordComparator getHistoryRecordComparator();
}
