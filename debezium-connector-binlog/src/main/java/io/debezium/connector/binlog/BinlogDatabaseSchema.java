/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.DefaultValueConverter;
import io.debezium.relational.HistorizedRelationalDatabaseSchema;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.SystemVariables;
import io.debezium.relational.SystemVariables.Scope;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.relational.ValueConverterProvider;
import io.debezium.relational.ddl.DdlChanges;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.relational.ddl.DdlParserListener;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.text.MultipleParsingExceptions;
import io.debezium.text.ParsingException;
import io.debezium.util.Collect;
import io.debezium.util.Strings;

/**
 * Abstract implementation that records schema history for binlog-based databases.<p></p>
 *
 * The schema information includes the {@link io.debezium.relational.Tables} and the Kafka Connect
 * {@link #schemaFor(TableId)} for each table, where the {@link org.apache.kafka.connect.data.Schema} excludes
 * any columns that have been specified in the {@link BinlogConnectorConfig#COLUMN_EXCLUDE_LIST} configuration.
 *
 * @author Chris Cranford
 */
public abstract class BinlogDatabaseSchema<P extends Partition, O extends OffsetContext, V extends ValueConverterProvider, D extends DefaultValueConverter>
        extends HistorizedRelationalDatabaseSchema {

    private final static Logger LOGGER = LoggerFactory.getLogger(BinlogDatabaseSchema.class);

    private final Set<String> ignoredQueryStatements = Collect.unmodifiableSet("BEGIN", "END", "FLUSH PRIVILEGES");
    private final DdlParser ddlParser;
    private final RelationalTableFilters filters;
    private final DdlChanges ddlChanges;
    private final Map<Long, TableId> tableIdsByTableNumber = new ConcurrentHashMap<>();
    private final Map<Long, TableId> excludeTableIdsByTableNumber = new ConcurrentHashMap<>();
    private final BinlogConnectorConfig connectorConfig;

    /**
     * Creates a binlog-connector based relational schema based on the supplied configuration. The DDL
     * statements passed to the schema are parsed and a logical model is maintained of the database.
     *
     * @param connectorConfig the connector configuration, should not be null
     * @param valueConverter the connector-specific value converter, should not be null
     * @param defaultValueConverter the connector-specific default value converter, should not be null
     * @param topicNamingStrategy the topic naming strategy to be used, should not be null
     * @param schemaNameAdjuster the schema name adjuster, should not be null
     * @param tableIdCaseInsensitive whether table identifiers are case-insensitive
     */
    public BinlogDatabaseSchema(BinlogConnectorConfig connectorConfig,
                                V valueConverter,
                                D defaultValueConverter,
                                TopicNamingStrategy<TableId> topicNamingStrategy,
                                SchemaNameAdjuster schemaNameAdjuster,
                                boolean tableIdCaseInsensitive) {
        super(connectorConfig,
                topicNamingStrategy,
                connectorConfig.getTableFilters().dataCollectionFilter(),
                connectorConfig.getColumnFilter(),
                new TableSchemaBuilder(
                        valueConverter,
                        defaultValueConverter,
                        schemaNameAdjuster,
                        connectorConfig.customConverterRegistry(),
                        connectorConfig.getSourceInfoStructMaker().schema(),
                        connectorConfig.getFieldNamer(),
                        false,
                        connectorConfig.getEventConvertingFailureHandlingMode()),
                tableIdCaseInsensitive,
                connectorConfig.getKeyMapper());
        this.ddlParser = createDdlParser(connectorConfig, valueConverter);
        this.ddlChanges = this.ddlParser.getDdlChanges();
        this.connectorConfig = connectorConfig;
        this.filters = connectorConfig.getTableFilters();
    }

    @Override
    protected DdlParser getDdlParser() {
        return ddlParser;
    }

    @Override
    public boolean skipSchemaChangeEvent(SchemaChangeEvent event) {
        if (storeOnlyCapturedDatabases() && !Strings.isNullOrEmpty(event.getDatabase())
                && !connectorConfig.getTableFilters().databaseFilter().test(event.getDatabase())) {
            LOGGER.debug("Skipping schema event as it belongs to a non-captured database: '{}'", event);
            return true;
        }
        return false;
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent schemaChange) {
        switch (schemaChange.getType()) {
            case CREATE:
            case ALTER:
                schemaChange.getTableChanges().forEach(x -> buildAndRegisterSchema(x.getTable()));
                break;
            case DROP:
                schemaChange.getTableChanges().forEach(x -> removeSchema(x.getId()));
                break;
            default:
        }

        // Record the DDL statement so that we can later recover them.
        // This is done _after_ writing the schema change records so that failure recovery (which is based on
        // the schema history) won't lose schema change records.
        //
        // We either store:
        // - all DDLs if configured
        // - or global SEt variables
        // - or DDLs for captured objects
        if (!storeOnlyCapturedTables() || isGlobalSetVariableStatement(schemaChange.getDdl(), schemaChange.getDatabase())
                || schemaChange.getTables().stream().map(Table::id).anyMatch(filters.dataCollectionFilter()::isIncluded)) {
            LOGGER.debug("Recorded DDL statements for database '{}': {}", schemaChange.getDatabase(), schemaChange.getDdl());
            record(schemaChange, schemaChange.getTableChanges());
        }
    }

    /**
     * Get all table names for all databases that are captured.
     *
     * @return array of table names
     */
    public String[] capturedTablesAsStringArray() {
        return tableIds().stream().map(TableId::toString).toArray(String[]::new);
    }

    /**
     * Sets system variables with use by the DDL parser.
     *
     * @param scope the variable scope
     * @param variables system variables; may not be null but can be empty
     */
    public void setSystemVariables(Scope scope, Map<String, String> variables) {
        variables.forEach((name, value) -> ddlParser.systemVariables().setVariable(scope, name, value));
    }

    /**
     * Get all system variables known to the DDL parser.
     *
     * @return the system variables; never null
     */
    public SystemVariables systemVariables() {
        return ddlParser.systemVariables();
    }

    /**
     * Checks whether the provided DDL is a global set variable statement.
     *
     * @param ddl the DDL to check; may be null or empty
     * @param databaseName the database name; may be null or empty
     * @return true if the statement is a global set variable statement; false otherwise
     */
    public boolean isGlobalSetVariableStatement(String ddl, String databaseName) {
        return (databaseName == null || databaseName.isEmpty()) && ddl != null && ddl.toUpperCase().startsWith("SET ");
    }

    /**
     * Assign the given table number to the table with the specified {@link TableId}.
     *
     * @param tableNumber the table number found in binlog events
     * @param id the identifier for the corresponding table
     * @return true if the assignment was successful, false if the table is excluded via configuration
     */
    public boolean assignTableNumber(long tableNumber, TableId id) {
        final TableSchema tableSchema = schemaFor(id);
        if (tableSchema == null) {
            excludeTableIdsByTableNumber.put(tableNumber, id);
            return false;
        }
        tableIdsByTableNumber.put(tableNumber, id);
        return true;
    }

    /**
     * Return the table id associated with connector-specific table number.
     *
     * @param tableNumber the table number from binlog
     * @return the table id or null if not known
     */
    public TableId getTableId(long tableNumber) {
        return tableIdsByTableNumber.get(tableNumber);
    }

    /**
     * Return the excluded table id associated with connector-specific table number.
     *
     * @param tableNumber the table number from binlog
     * @return the table id or null if not known
     */
    public TableId getExcludeTableId(long tableNumber) {
        return excludeTableIdsByTableNumber.get(tableNumber);
    }

    /**
     * Clear all table mappings.<p></p>
     *
     * This should be done when the logs are rotated, since in that a different table, numbering scheme will be used
     * by all subsequent {@code TABLE_MAP} binlog events.
     */
    public void clearTableMappings() {
        LOGGER.debug("Clearing table number mappings");
        tableIdsByTableNumber.clear();
        excludeTableIdsByTableNumber.clear();
    }

    /**
     * Parse DDL from the connector's snapshot phase.
     *
     * @param partition the partition; should not be null
     * @param ddlStatements the DDL statement to be parsed
     * @param databaseName the database name
     * @param offset the connector offsets; should not be null
     * @param sourceTime time the DDL happened, should not be null
     * @return list of parsed schema changes
     */
    public List<SchemaChangeEvent> parseSnapshotDdl(P partition, String ddlStatements, String databaseName, O offset, Instant sourceTime) {
        LOGGER.debug("Processing snapshot DDL '{}' for database '{}'", ddlStatements, databaseName);
        return parseDdl(partition, ddlStatements, databaseName, offset, sourceTime, true);
    }

    /**
     * Parse DDL from the connector's streaming phase.
     *
     * @param partition the partition; should not be null
     * @param ddlStatements the DDL statement to be parsed
     * @param databaseName the database name
     * @param offset the connector offsets; should not be null
     * @param sourceTime time the DDL happened, should not be null
     * @return list of parsed schema changes
     */
    public List<SchemaChangeEvent> parseStreamingDdl(P partition, String ddlStatements, String databaseName, O offset, Instant sourceTime) {
        LOGGER.debug("Processing streaming DDL '{}' for database '{}'", ddlStatements, databaseName);
        return parseDdl(partition, ddlStatements, databaseName, offset, sourceTime, false);
    }

    /**
     * Creates the connector-specific DDL parser instance.
     *
     * @param connectorConfig the binlog connector configuration, never null
     * @param valueConverter the value converter, never null
     * @return the connector's DDL parser instance, never null
     */
    protected abstract DdlParser createDdlParser(BinlogConnectorConfig connectorConfig, V valueConverter);

    /**
     * Update offsets based on a table-specific event.
     *
     * @param offset the offsets to be mutated; should not be null
     * @param databaseName the database name
     * @param tableIds set of table identifiers
     * @param changeTime the time the event happened
     */
    protected abstract void handleTableEvent(O offset, String databaseName, Set<TableId> tableIds, Instant changeTime);

    /**
     * Update offsets based on a database-specific event.
     *
     * @param offset the offsets to be mutated; should not be null
     * @param databaseName the database name
     * @param changeTime the time the event happened
     */
    protected abstract void handleDatabaseEvent(O offset, String databaseName, Instant changeTime);

    /**
     * Discards any currently-cached schemas and rebuild using filters
     */
    protected void refreshSchemas() {
        clearSchemas();
        tableIds().forEach(id -> buildAndRegisterSchema(tableFor(id)));
    }

    private List<SchemaChangeEvent> parseDdl(P partition, String ddlStatements, String databaseName, O offset,
                                             Instant sourceTime, boolean snapshot) {
        final List<SchemaChangeEvent> schemaChangeEvents = new ArrayList<>(3);
        if (ignoredQueryStatements.contains(ddlStatements)) {
            return schemaChangeEvents;
        }

        try {
            this.ddlChanges.reset();
            this.ddlParser.setCurrentSchema(databaseName);
            this.ddlParser.parse(ddlStatements, tables());
        }
        catch (ParsingException | MultipleParsingExceptions e) {
            if (skipUnparseableDdlStatements()) {
                LOGGER.warn("Ignoring unparseable DDL statement '{}'", ddlStatements, e);
            }
            else {
                throw e;
            }
        }

        // No need to send schema events or store DDL if no table has changed
        if (!storeOnlyCapturedTables() || isGlobalSetVariableStatement(ddlStatements, databaseName) || ddlChanges.anyMatch(filters)) {
            // We are supposed to _also_ record the schema changes as SourceRecords, but these need to be filtered
            // by database. Unfortunately, the databaseName on the event might not be the same database as that
            // being modified by the DDL statements (since the DDL statements can have fully-qualified names).
            // Therefore, we have to look at each statement to figure out which database it applies and then
            // record the DDL statements (still in the same order) to those databases.
            if (!ddlChanges.isEmpty()) {
                // We understood at least some of the DDL statements and can figure out to which database they apply.
                // They also apply to more databases than 'databaseName', so we need to apply the DDL statements in
                // the same order they were read for each _affected_ database, grouped together if multiple apply
                // to the same _affected_ database...
                ddlChanges.getEventsByDatabase((String dbName, List<DdlParserListener.Event> events) -> {
                    final String sanitizedDbName = Strings.defaultIfEmpty(dbName, "");
                    if (acceptableDatabase(dbName)) {
                        final Set<TableId> tableIds = new HashSet<>();
                        events.forEach(event -> {
                            final TableId tableId = getTableId(event);
                            if (tableId != null) {
                                tableIds.add(tableId);
                            }
                        });
                        events.forEach(event -> {
                            final TableId tableId = getTableId(event);
                            handleTableEvent(offset, dbName, tableIds, sourceTime);
                            // For SET with multiple parameters
                            if (event instanceof DdlParserListener.TableCreatedEvent) {
                                emitChangeEvent(partition, offset, schemaChangeEvents, sanitizedDbName, event, tableId,
                                        SchemaChangeEvent.SchemaChangeEventType.CREATE, snapshot);
                            }
                            else if (event instanceof DdlParserListener.TableAlteredEvent
                                    || event instanceof DdlParserListener.TableIndexCreatedEvent
                                    || event instanceof DdlParserListener.TableIndexDroppedEvent) {
                                emitChangeEvent(partition, offset, schemaChangeEvents, sanitizedDbName, event, tableId,
                                        SchemaChangeEvent.SchemaChangeEventType.ALTER, snapshot);
                            }
                            else if (event instanceof DdlParserListener.TableDroppedEvent) {
                                emitChangeEvent(partition, offset, schemaChangeEvents, sanitizedDbName, event, tableId,
                                        SchemaChangeEvent.SchemaChangeEventType.DROP, snapshot);
                            }
                            else if (event instanceof DdlParserListener.TableTruncatedEvent) {
                                emitChangeEvent(partition, offset, schemaChangeEvents, sanitizedDbName, event, tableId,
                                        SchemaChangeEvent.SchemaChangeEventType.TRUNCATE, snapshot);
                            }
                            else if (event instanceof DdlParserListener.SetVariableEvent) {
                                // SET statement with multiple variable emits event for each variable. We want to emit only
                                // one change event
                                final DdlParserListener.SetVariableEvent varEvent = (DdlParserListener.SetVariableEvent) event;
                                if (varEvent.order() == 0) {
                                    emitChangeEvent(partition, offset, schemaChangeEvents, sanitizedDbName, event,
                                            tableId, SchemaChangeEvent.SchemaChangeEventType.DATABASE, snapshot);
                                }
                            }
                            else {
                                emitChangeEvent(partition, offset, schemaChangeEvents, sanitizedDbName, event, tableId,
                                        SchemaChangeEvent.SchemaChangeEventType.DATABASE, snapshot);
                            }
                        });
                    }
                });
            }
            else {
                handleDatabaseEvent(offset, databaseName, sourceTime);
                schemaChangeEvents
                        .add(SchemaChangeEvent.ofDatabase(partition, offset, databaseName, ddlStatements, snapshot));
            }
        }
        else {
            LOGGER.debug("Changes for DDL '{}' were filtered and not recorded in database schema history", ddlStatements);
        }
        return schemaChangeEvents;
    }

    private void emitChangeEvent(P partition, O offset, List<SchemaChangeEvent> schemaChangeEvents,
                                 String sanitizedDbName, DdlParserListener.Event event, TableId tableId,
                                 SchemaChangeEvent.SchemaChangeEventType type, boolean snapshot) {
        SchemaChangeEvent schemaChangeEvent;
        if (type.equals(SchemaChangeEvent.SchemaChangeEventType.ALTER) && event instanceof DdlParserListener.TableAlteredEvent
                && ((DdlParserListener.TableAlteredEvent) event).previousTableId() != null) {
            schemaChangeEvent = SchemaChangeEvent.ofRename(
                    partition,
                    offset,
                    sanitizedDbName,
                    null,
                    event.statement(),
                    tableId != null ? tables().forTable(tableId) : null,
                    ((DdlParserListener.TableAlteredEvent) event).previousTableId());
        }
        else {
            Table table = getTable(tableId, type);
            schemaChangeEvent = SchemaChangeEvent.of(
                    type,
                    partition,
                    offset,
                    sanitizedDbName,
                    null,
                    event.statement(),
                    table,
                    snapshot);
        }
        schemaChangeEvents.add(schemaChangeEvent);
    }

    private Table getTable(TableId tableId, SchemaChangeEvent.SchemaChangeEventType type) {
        if (tableId == null) {
            return null;
        }
        if (SchemaChangeEvent.SchemaChangeEventType.DROP == type) {
            // DROP events don't have information about tableChanges, so we are creating a Table object
            // with just the tableId to be used during blocking snapshot to filter out drop events not
            // related to table to be snapshotted.
            return Table.editor().tableId(tableId).create();
        }
        return tables().forTable(tableId);
    }

    private boolean acceptableDatabase(String databaseName) {
        return !storeOnlyCapturedTables()
                || filters.databaseFilter().test(databaseName)
                || databaseName == null
                || databaseName.isEmpty();
    }

    private TableId getTableId(DdlParserListener.Event event) {
        if (event instanceof DdlParserListener.TableEvent) {
            return ((DdlParserListener.TableEvent) event).tableId();
        }
        else if (event instanceof DdlParserListener.TableIndexEvent) {
            return ((DdlParserListener.TableIndexEvent) event).tableId();
        }
        return null;
    }
}
