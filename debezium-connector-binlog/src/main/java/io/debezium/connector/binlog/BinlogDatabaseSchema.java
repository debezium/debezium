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
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.binlog.jdbc.BinlogSystemVariables;
import io.debezium.connector.binlog.jdbc.LowerCaseTableNames;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.DefaultValueConverter;
import io.debezium.relational.HistorizedRelationalDatabaseSchema;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.SystemVariables;
import io.debezium.relational.SystemVariables.Scope;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.relational.Tables;
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
public abstract class BinlogDatabaseSchema<P extends BinlogPartition, O extends BinlogOffsetContext<?>, V extends ValueConverterProvider, D extends DefaultValueConverter>
        extends HistorizedRelationalDatabaseSchema {

    private final static Logger LOGGER = LoggerFactory.getLogger(BinlogDatabaseSchema.class);
    private static final Pattern TRUNCATE_STATEMENT_PATTERN = Pattern.compile("(SET STATEMENT .*)?TRUNCATE TABLE .*", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    private final Set<String> ignoredQueryStatements = Collect.unmodifiableSet("BEGIN", "END", "FLUSH PRIVILEGES");
    private final DdlParser ddlParser;
    private final RelationalTableFilters filters;
    private final Map<Long, TableId> tableIdsByTableNumber = new ConcurrentHashMap<>();
    private final Map<Long, TableId> excludeTableIdsByTableNumber = new ConcurrentHashMap<>();
    private final BinlogConnectorConfig connectorConfig;
    private final V valueConverter;
    private final LowerCaseTableNames lowerCaseTableNames;
    /**
     * The names of known databases by their lowercase name, as created on a server with {@code lower_case_table_names=2}.
     */
    private final Map<String, String> databaseNames = new ConcurrentHashMap<>();

    /**
     * Creates a binlog-connector based relational schema based on the supplied configuration. The DDL
     * statements passed to the schema are parsed and a logical model is maintained of the database.
     *
     * @param connectorConfig the connector configuration, should not be null
     * @param valueConverter the connector-specific value converter, should not be null
     * @param defaultValueConverter the connector-specific default value converter, should not be null
     * @param topicNamingStrategy the topic naming strategy to be used, should not be null
     * @param schemaNameAdjuster the schema name adjuster, should not be null
     * @param lowerCaseTableNames how the server stores and compares database and table names, should not be null
     * @param converterRegistry
     */
    public BinlogDatabaseSchema(BinlogConnectorConfig connectorConfig,
                                V valueConverter,
                                D defaultValueConverter,
                                TopicNamingStrategy<TableId> topicNamingStrategy,
                                SchemaNameAdjuster schemaNameAdjuster,
                                LowerCaseTableNames lowerCaseTableNames, CustomConverterRegistry converterRegistry,
                                CdcSourceTaskContext<? extends CommonConnectorConfig> taskContext) {
        super(connectorConfig,
                topicNamingStrategy,
                connectorConfig.getTableFilters().dataCollectionFilter(),
                connectorConfig.getColumnFilter(),
                new TableSchemaBuilder(
                        valueConverter,
                        defaultValueConverter,
                        schemaNameAdjuster,
                        converterRegistry,
                        connectorConfig.getSourceInfoStructMaker().schema(),
                        connectorConfig.getFieldNamer(),
                        false,
                        connectorConfig.getEventConvertingFailureHandlingMode()),
                lowerCaseTableNames.isCaseInsensitive(),
                connectorConfig.getKeyMapper(), taskContext);
        this.ddlParser = createDdlParser(connectorConfig, valueConverter);
        this.connectorConfig = connectorConfig;
        this.filters = connectorConfig.getTableFilters();
        this.valueConverter = valueConverter;
        this.lowerCaseTableNames = lowerCaseTableNames;
    }

    /**
     * Parses a table definition, as returned by {@code SHOW CREATE TABLE}, into a {@link Table} without
     * mutating the live schema state. A short-lived parser instance is used so that the shared parser
     * used for the streaming phase is not affected by this call (debezium/dbz#1550).
     *
     * @param tableId the fully-qualified identifier of the table the definition belongs to; should not be null
     * @param createTableDdl the {@code CREATE TABLE} statement text; should not be null
     * @param systemVariables the system variables for the connection that returned the table definition; should not be null
     * @return the parsed table definition, or {@code null} if the statement did not produce the requested table
     */
    Table parseTableDefinition(TableId tableId, String createTableDdl, Map<String, String> systemVariables) {
        final DdlParser parser = createDdlParser(connectorConfig, valueConverter);
        final Tables parsedTables = new Tables(lowerCaseTableNames.isCaseInsensitive());
        systemVariables.forEach((name, value) -> parser.systemVariables().setVariable(BinlogSystemVariables.BinlogScope.SESSION, name, value));
        parser.setCurrentDatabase(tableId.catalog());
        parser.parse(createTableDdl, parsedTables);
        final Table table = parsedTables.forTable(tableId);
        if (table == null) {
            return null;
        }
        final Table knownTable = tables().forTable(table.id());
        return table.edit().tableId(knownTable != null ? knownTable.id() : toServerTableId(table.id())).create();
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
        return tableIds().stream()
                .map(this::tableFor)
                .filter(Objects::nonNull)
                .map(table -> table.id().toString())
                .toArray(String[]::new);
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
        // With case-insensitive table names, TABLE_MAP events can use a different case than the table schema.
        // Events use the identifier of the table schema, so that all of them refer to the table by the same name.
        tableIdsByTableNumber.put(tableNumber, tableSchema.id());
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
    protected void handleTableEvent(O offset, String databaseName, Set<TableId> tableIds, Instant changeTime) {
        offset.tableEvent(databaseName, tableIds, changeTime);
    }

    /**
     * Update offsets based on a database-specific event.
     *
     * @param offset the offsets to be mutated; should not be null
     * @param databaseName the database name
     * @param changeTime the time the event happened
     */
    protected void handleDatabaseEvent(O offset, String databaseName, Instant changeTime) {
        offset.databaseEvent(databaseName, changeTime);
    }

    /**
     * Discards any currently-cached schemas and rebuild using filters
     */
    protected void refreshSchemas() {
        clearSchemas();
        tableIds().forEach(id -> buildAndRegisterSchema(tableFor(id)));
    }

    @Override
    public void recover(Offsets<?, ?> offsets) throws InterruptedException {
        super.recover(offsets);
        // The schema history can contain names recorded before they followed lower_case_table_names
        if (lowerCaseTableNames.isCaseInsensitive() && applyServerNamesToTables()) {
            refreshSchemas();
        }
    }

    /**
     * Returns the identifier under which the server reports the given table, based on {@code lower_case_table_names}.
     * With {@code 1}, database and table names are lowercase. With {@code 2}, the table name is kept and the database
     * name is resolved to the name of the known database, because MySQL records the session database in lowercase.
     *
     * @param tableId the table identifier; may be null
     * @return the table identifier as reported by the server, or null if the given identifier is null
     */
    protected TableId toServerTableId(TableId tableId) {
        if (tableId == null || !lowerCaseTableNames.isCaseInsensitive()) {
            return tableId;
        }
        final String tableName = lowerCaseTableNames == LowerCaseTableNames.STORED_IN_LOWERCASE
                ? tableId.table().toLowerCase(Locale.ROOT)
                : tableId.table();
        return new TableId(toServerDatabaseName(tableId.catalog()), tableId.schema(), tableName);
    }

    /**
     * Returns the name under which the server reports the given database, based on {@code lower_case_table_names}.
     *
     * @param databaseName the database name; may be null
     * @return the database name as reported by the server, or null if the given name is null
     */
    protected String toServerDatabaseName(String databaseName) {
        if (databaseName == null || !lowerCaseTableNames.isCaseInsensitive()) {
            return databaseName;
        }
        final String lowercaseName = databaseName.toLowerCase(Locale.ROOT);
        return lowerCaseTableNames == LowerCaseTableNames.STORED_IN_LOWERCASE
                ? lowercaseName
                : databaseNames.getOrDefault(lowercaseName, databaseName);
    }

    /**
     * Renames the table affected by a DDL statement to the name reported by the server, if needed.
     */
    private TableId applyServerName(TableId tableId) {
        if (tableId == null || !lowerCaseTableNames.isCaseInsensitive()) {
            return tableId;
        }
        final Table table = tables().forTable(tableId);
        if (table == null) {
            // The table was dropped or renamed by the statement
            return toServerTableId(tableId);
        }
        final TableId serverTableId = toServerTableId(table.id());
        if (!serverTableId.equals(table.id())) {
            tables().overwriteTable(table.edit().tableId(serverTableId).create());
        }
        return serverTableId;
    }

    /**
     * Renames recovered tables to the names reported by the server, if needed.
     *
     * @return true if any table was renamed
     */
    private boolean applyServerNamesToTables() {
        final List<Table> knownTables = tables().tableIds().stream()
                .map(tables()::forTable)
                .filter(Objects::nonNull)
                .toList();
        if (lowerCaseTableNames == LowerCaseTableNames.CASE_INSENSITIVE) {
            knownTables.forEach(table -> rememberRecoveredDatabaseName(table.id().catalog()));
        }
        boolean renamed = false;
        for (Table table : knownTables) {
            final TableId serverTableId = toServerTableId(table.id());
            if (!serverTableId.equals(table.id())) {
                tables().overwriteTable(table.edit().tableId(serverTableId).create());
                renamed = true;
            }
        }
        return renamed;
    }

    /**
     * Records the name of a database created or dropped by a DDL statement. With {@code lower_case_table_names=2}, the
     * server keeps the name as it was created.
     */
    private void rememberDatabaseName(DdlParserListener.Event event) {
        if (lowerCaseTableNames != LowerCaseTableNames.CASE_INSENSITIVE) {
            return;
        }
        if (event instanceof DdlParserListener.DatabaseCreatedEvent created && !Strings.isNullOrEmpty(created.databaseName())) {
            databaseNames.put(created.databaseName().toLowerCase(Locale.ROOT), created.databaseName());
        }
        else if (event instanceof DdlParserListener.DatabaseDroppedEvent dropped && !Strings.isNullOrEmpty(dropped.databaseName())) {
            databaseNames.remove(dropped.databaseName().toLowerCase(Locale.ROOT));
        }
    }

    /**
     * Records the name of the database of a recovered table, unless another name of the database is already known.
     * A lowercase name is replaced, because it can come from the session database, which MySQL records in lowercase.
     */
    private void rememberRecoveredDatabaseName(String databaseName) {
        if (Strings.isNullOrEmpty(databaseName)) {
            return;
        }
        final String lowercaseName = databaseName.toLowerCase(Locale.ROOT);
        databaseNames.merge(lowercaseName, databaseName, (known, recovered) -> known.equals(lowercaseName) ? recovered : known);
    }

    private List<SchemaChangeEvent> parseDdl(P partition, String ddlStatements, String databaseName, O offset,
                                             Instant sourceTime, boolean snapshot) {
        final List<SchemaChangeEvent> schemaChangeEvents = new ArrayList<>(3);
        if (ignoredQueryStatements.contains(ddlStatements)) {
            return schemaChangeEvents;
        }

        DdlChanges ddlChanges = new DdlChanges();
        try {
            this.ddlParser.setCurrentSchema(databaseName);
            ddlChanges = this.ddlParser.parse(ddlStatements, tables());
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
        // Also skip if DDL matches the filter (e.g., CREATE FUNCTION, PROCEDURE, VIEW, TRIGGER)
        // BUT do NOT filter TRUNCATE statements as they need special handling based on skipped.operations config
        if (!TRUNCATE_STATEMENT_PATTERN.matcher(ddlStatements).matches() && ddlFilter().test(ddlStatements)) {
            LOGGER.debug("Changes for DDL '{}' were filtered and not recorded in database schema history", ddlStatements);
            return schemaChangeEvents;
        }

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
                ddlChanges.getEventsByDatabase((String parsedDbName, List<DdlParserListener.Event> events) -> {
                    events.forEach(this::rememberDatabaseName);
                    final String dbName = toServerDatabaseName(parsedDbName);
                    final String sanitizedDbName = Strings.defaultIfEmpty(dbName, "");
                    if (acceptableDatabase(dbName)) {
                        final Set<TableId> tableIds = new HashSet<>();
                        events.forEach(event -> {
                            final TableId tableId = applyServerName(getTableId(event));
                            if (tableId != null) {
                                tableIds.add(tableId);
                            }
                        });
                        events.forEach(event -> {
                            final TableId tableId = applyServerName(getTableId(event));
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
                    toServerTableId(((DdlParserListener.TableAlteredEvent) event).previousTableId()));
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
