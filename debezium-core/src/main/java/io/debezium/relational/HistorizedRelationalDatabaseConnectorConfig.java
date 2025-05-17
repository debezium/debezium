/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.util.function.Predicate;
import java.util.regex.Pattern;

import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;

import io.debezium.config.ConfigDefinition;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.function.Predicates;
import io.debezium.relational.Selectors.TableIdToStringMapper;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.relational.history.SchemaHistoryMetrics;

/**
 * Configuration options shared across the relational CDC connectors which use a persistent database schema history.
 *
 * @author Gunnar Morling
 */
public abstract class HistorizedRelationalDatabaseConnectorConfig extends RelationalDatabaseConnectorConfig {

    protected static final int DEFAULT_SNAPSHOT_FETCH_SIZE = 2_000;

    private static final String DEFAULT_SCHEMA_HISTORY = "io.debezium.storage.kafka.history.KafkaSchemaHistory";

    private final boolean useCatalogBeforeSchema;
    private final Class<? extends SourceConnector> connectorClass;
    private final boolean multiPartitionMode;
    private final Predicate<String> ddlFilter;
    protected boolean skipUnparseableDDL;
    protected boolean storeOnlyCapturedTablesDdl;
    protected boolean storeOnlyCapturedDatabasesDdl;

    /**
     * The database schema history class is hidden in the {@link #configDef()} since that is designed to work with a user interface,
     * and in these situations using Kafka is the only way to go.
     */
    public static final Field SCHEMA_HISTORY = Field.create("schema.history.internal")
            .withDisplayName("Database schema history class")
            .withType(Type.CLASS)
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withInvisibleRecommender()
            .withDescription("The name of the SchemaHistory class that should be used to store and recover database schema changes. "
                    + "The configuration properties for the history are prefixed with the '"
                    + SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + "' string.")
            .withDefault(DEFAULT_SCHEMA_HISTORY);

    public static final Field SKIP_UNPARSEABLE_DDL_STATEMENTS = SchemaHistory.SKIP_UNPARSEABLE_DDL_STATEMENTS;

    public static final Field STORE_ONLY_CAPTURED_TABLES_DDL = SchemaHistory.STORE_ONLY_CAPTURED_TABLES_DDL;

    public static final Field STORE_ONLY_CAPTURED_DATABASES_DDL = SchemaHistory.STORE_ONLY_CAPTURED_DATABASES_DDL;

    protected static final ConfigDefinition CONFIG_DEFINITION = RelationalDatabaseConnectorConfig.CONFIG_DEFINITION.edit()
            .history(
                    SCHEMA_HISTORY,
                    SKIP_UNPARSEABLE_DDL_STATEMENTS,
                    STORE_ONLY_CAPTURED_TABLES_DDL,
                    STORE_ONLY_CAPTURED_DATABASES_DDL)
            .create();

    protected HistorizedRelationalDatabaseConnectorConfig(Class<? extends SourceConnector> connectorClass,
                                                          Configuration config,
                                                          TableFilter systemTablesFilter,
                                                          boolean useCatalogBeforeSchema,
                                                          int defaultSnapshotFetchSize,
                                                          ColumnFilterMode columnFilterMode,
                                                          boolean multiPartitionMode) {
        this(connectorClass, config, systemTablesFilter, TableId::toString, useCatalogBeforeSchema,
                defaultSnapshotFetchSize, columnFilterMode, multiPartitionMode);
    }

    protected HistorizedRelationalDatabaseConnectorConfig(Class<? extends SourceConnector> connectorClass,
                                                          Configuration config,
                                                          TableFilter systemTablesFilter,
                                                          TableIdToStringMapper tableIdMapper,
                                                          boolean useCatalogBeforeSchema,
                                                          ColumnFilterMode columnFilterMode,
                                                          boolean multiPartitionMode) {
        this(connectorClass, config, systemTablesFilter, tableIdMapper, useCatalogBeforeSchema,
                DEFAULT_SNAPSHOT_FETCH_SIZE, columnFilterMode, multiPartitionMode);
    }

    protected HistorizedRelationalDatabaseConnectorConfig(Class<? extends SourceConnector> connectorClass,
                                                          Configuration config,
                                                          TableFilter systemTablesFilter,
                                                          TableIdToStringMapper tableIdMapper,
                                                          boolean useCatalogBeforeSchema,
                                                          int defaultSnapshotFetchSize,
                                                          ColumnFilterMode columnFilterMode,
                                                          boolean multiPartitionMode) {
        super(config, systemTablesFilter, tableIdMapper, defaultSnapshotFetchSize, columnFilterMode, useCatalogBeforeSchema);
        this.useCatalogBeforeSchema = useCatalogBeforeSchema;
        this.connectorClass = connectorClass;
        this.multiPartitionMode = multiPartitionMode;
        this.ddlFilter = createDdlFilter(config);
        this.skipUnparseableDDL = config.getBoolean(SKIP_UNPARSEABLE_DDL_STATEMENTS);
        this.storeOnlyCapturedTablesDdl = config.getBoolean(STORE_ONLY_CAPTURED_TABLES_DDL);
        this.storeOnlyCapturedDatabasesDdl = config.getBoolean(STORE_ONLY_CAPTURED_DATABASES_DDL);
    }

    /**
     * Returns a configured (but not yet started) instance of the database schema history.
     */
    public SchemaHistory getSchemaHistory() {
        Configuration config = getConfig();

        SchemaHistory schemaHistory = config.getInstance(SCHEMA_HISTORY, SchemaHistory.class);
        if (schemaHistory == null) {
            throw new ConnectException("Unable to instantiate the database schema history class " +
                    config.getString(SCHEMA_HISTORY));
        }

        // Do not remove the prefix from the subset of config properties ...
        Configuration schemaHistoryConfig = config.subset(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING, false)
                .edit()
                .with(config.subset(Field.INTERNAL_PREFIX + SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING, false))
                .withDefault(SchemaHistory.NAME, getLogicalName() + "-schemahistory")
                .withDefault(SchemaHistory.INTERNAL_CONNECTOR_CLASS, connectorClass.getName())
                .withDefault(SchemaHistory.INTERNAL_CONNECTOR_ID, logicalName)
                .withDefault(SchemaHistory.CONNECTOR_NAME, connectorName())
                .build();

        HistoryRecordComparator historyComparator = getHistoryRecordComparator();
        schemaHistory.configure(schemaHistoryConfig, historyComparator,
                new SchemaHistoryMetrics(this, multiPartitionMode()), useCatalogBeforeSchema()); // validates

        return schemaHistory;
    }

    public boolean useCatalogBeforeSchema() {
        return useCatalogBeforeSchema;
    }

    public boolean multiPartitionMode() {
        return multiPartitionMode;
    }

    private Predicate<String> createDdlFilter(Configuration config) {
        // Set up the DDL filter
        final String ddlFilter = config.getString(SchemaHistory.DDL_FILTER);
        return (ddlFilter != null) ? Predicates.includes(ddlFilter, Pattern.CASE_INSENSITIVE | Pattern.DOTALL) : (x -> false);
    }

    public Predicate<String> ddlFilter() {
        return ddlFilter;
    }

    public boolean skipUnparseableDdlStatements() {
        return skipUnparseableDDL;
    }

    public boolean storeOnlyCapturedTables() {
        return storeOnlyCapturedTablesDdl;
    }

    public boolean storeOnlyCapturedDatabases() {
        return storeOnlyCapturedDatabasesDdl;
    }

    /**
     * Returns a comparator to be used when recovering records from the schema history, making sure no history entries
     * newer than the offset we resume from are recovered (which could happen when restarting a connector after history
     * records have been persisted but no new offset has been committed yet).
     */
    public abstract HistoryRecordComparator getHistoryRecordComparator();

}
