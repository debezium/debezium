/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import io.debezium.config.ConfigDefinition;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.document.Document;
import io.debezium.relational.ColumnFilterMode;
import io.debezium.relational.HistorizedRelationalDatabaseConnectorConfig;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.relational.history.HistoryRecordComparator;

/**
 * Configuration properties for the SQLite connector.
 *
 * @author Zihan Dai
 */
public class SqliteConnectorConfig extends HistorizedRelationalDatabaseConnectorConfig {

    /**
     * Path to the SQLite database file.
     */
    public static final Field DATABASE_PATH = Field.create("database.path")
            .withDisplayName("Database file path")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .withDescription("Path to the SQLite database file to capture changes from.")
            .required();

    /**
     * Interval at which the WAL file is polled for new changes.
     */
    public static final Field POLL_INTERVAL_MS = Field.create("poll.interval.ms")
            .withDisplayName("Poll interval (ms)")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDefault(1000L)
            .withDescription("Interval in milliseconds at which the WAL file is polled for new committed changes.");

    private static final ConfigDefinition CONFIG_DEFINITION = HistorizedRelationalDatabaseConnectorConfig.CONFIG_DEFINITION.edit()
            .name("SQLite")
            .type(DATABASE_PATH, POLL_INTERVAL_MS)
            .create();

    /**
     * The set of {@link Field}s defined as part of this configuration.
     */
    public static Field.Set ALL_FIELDS = Field.setOf(CONFIG_DEFINITION.all());

    public static ConfigDef configDef() {
        return CONFIG_DEFINITION.configDef();
    }

    private final String databasePath;
    private final long pollIntervalMs;

    public SqliteConnectorConfig(Configuration config) {
        super(
                SqliteConnector.class,
                config,
                new SqliteSystemTablesPredicate(),
                false,
                DEFAULT_SNAPSHOT_FETCH_SIZE,
                ColumnFilterMode.SCHEMA,
                false);

        this.databasePath = config.getString(DATABASE_PATH);
        this.pollIntervalMs = config.getLong(POLL_INTERVAL_MS);
    }

    public String getDatabasePath() {
        return databasePath;
    }

    public long getPollIntervalMs() {
        return pollIntervalMs;
    }

    @Override
    protected SourceInfoStructMaker<? extends AbstractSourceInfo> getSourceInfoStructMaker(Version version) {
        return getSourceInfoStructMaker(SqliteConnectorConfig.SOURCE_INFO_STRUCT_MAKER, Module.name(), Module.version(), this);
    }

    @Override
    public HistoryRecordComparator getHistoryRecordComparator() {
        return new HistoryRecordComparator() {
            @Override
            protected boolean isPositionAtOrBefore(Document recorded, Document desired) {
                // TODO: Implement WAL offset comparison once streaming is implemented.
                // For now, compare based on the WAL frame index stored in the offset.
                return SqliteHistoryRecordComparator.isPositionAtOrBefore(recorded, desired);
            }
        };
    }

    @Override
    public String getConnectorName() {
        return Module.name();
    }

    /**
     * Predicate that identifies SQLite internal/system tables.
     */
    private static class SqliteSystemTablesPredicate implements TableFilter {
        @Override
        public boolean isIncluded(io.debezium.relational.TableId id) {
            // Exclude SQLite internal tables (sqlite_master, sqlite_sequence, etc.)
            return !id.table().startsWith("sqlite_");
        }
    }
}
