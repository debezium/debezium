/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.connector.mysql.MySqlSystemVariables.MySqlScope;
import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.relational.HistorizedRelationalDatabaseSchema;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.SystemVariables;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlChanges;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.relational.ddl.DdlParserListener.Event;
import io.debezium.relational.ddl.DdlParserListener.SetVariableEvent;
import io.debezium.relational.ddl.DdlParserListener.TableAlteredEvent;
import io.debezium.relational.ddl.DdlParserListener.TableCreatedEvent;
import io.debezium.relational.ddl.DdlParserListener.TableDroppedEvent;
import io.debezium.relational.ddl.DdlParserListener.TableEvent;
import io.debezium.relational.ddl.DdlParserListener.TableIndexCreatedEvent;
import io.debezium.relational.ddl.DdlParserListener.TableIndexDroppedEvent;
import io.debezium.relational.ddl.DdlParserListener.TableIndexEvent;
import io.debezium.relational.ddl.DdlParserListener.TableTruncatedEvent;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.SchemaChangeEvent.SchemaChangeEventType;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.text.MultipleParsingExceptions;
import io.debezium.text.ParsingException;
import io.debezium.util.Collect;
import io.debezium.util.Strings;

/**
 * Component that records the schema history for databases hosted by a MySQL database server. The schema information includes
 * the {@link Tables table definitions} and the Kafka Connect {@link #schemaFor(TableId) Schema}s for each table, where the
 * {@link Schema} excludes any columns that have been {@link MySqlConnectorConfig#COLUMN_EXCLUDE_LIST specified} in the
 * configuration.
 *
 * @author Randall Hauch
 */
@NotThreadSafe
public class MySqlDatabaseSchema extends HistorizedRelationalDatabaseSchema {

    private final static Logger LOGGER = LoggerFactory.getLogger(MySqlDatabaseSchema.class);

    private final Set<String> ignoredQueryStatements = Collect.unmodifiableSet("BEGIN", "END", "FLUSH PRIVILEGES");
    private final DdlParser ddlParser;
    private final RelationalTableFilters filters;
    private final DdlChanges ddlChanges;
    private final Map<Long, TableId> tableIdsByTableNumber = new ConcurrentHashMap<>();
    private final Map<Long, TableId> excludeTableIdsByTableNumber = new ConcurrentHashMap<>();
    private boolean storageInitializationExecuted = false;
    private final MySqlConnectorConfig connectorConfig;

    /**
     * Create a schema component given the supplied {@link MySqlConnectorConfig MySQL connector configuration}.
     * The DDL statements passed to the schema are parsed and a logical model of the database schema is created.
     *
     */
    public MySqlDatabaseSchema(MySqlConnectorConfig connectorConfig, MySqlValueConverters valueConverter, TopicNamingStrategy<TableId> topicNamingStrategy,
                               SchemaNameAdjuster schemaNameAdjuster, boolean tableIdCaseInsensitive) {
        super(connectorConfig, topicNamingStrategy, connectorConfig.getTableFilters().dataCollectionFilter(), connectorConfig.getColumnFilter(),
                new TableSchemaBuilder(
                        valueConverter,
                        new MySqlDefaultValueConverter(valueConverter),
                        schemaNameAdjuster,
                        connectorConfig.customConverterRegistry(),
                        connectorConfig.getSourceInfoStructMaker().schema(),
                        connectorConfig.getFieldNamer(),
                        false),
                tableIdCaseInsensitive, connectorConfig.getKeyMapper());

        this.ddlParser = new MySqlAntlrDdlParser(
                true,
                false,
                connectorConfig.isSchemaCommentsHistoryEnabled(),
                valueConverter,
                getTableFilter());
        this.ddlChanges = this.ddlParser.getDdlChanges();
        this.connectorConfig = connectorConfig;
        filters = connectorConfig.getTableFilters();
    }

    /**
     * Get all table names for all databases that are monitored whose events are captured by Debezium
     *
     * @return the array with the table names
     */
    public String[] capturedTablesAsStringArray() {
        final Collection<TableId> tables = tableIds();
        String[] ret = new String[tables.size()];
        int i = 0;
        for (TableId table : tables) {
            ret[i++] = table.toString();
        }
        return ret;
    }

    /**
     * Set the system variables on the DDL parser.
     *
     * @param variables the system variables; may not be null but may be empty
     */
    public void setSystemVariables(Map<String, String> variables) {
        variables.forEach((varName, value) -> {
            ddlParser.systemVariables().setVariable(MySqlScope.SESSION, varName, value);
        });
    }

    /**
     * Get the system variables as known by the DDL parser.
     *
     * @return the system variables; never null
     */
    public SystemVariables systemVariables() {
        return ddlParser.systemVariables();
    }

    protected void appendDropTableStatement(StringBuilder sb, TableId tableId) {
        sb.append("DROP TABLE ").append(tableId).append(" IF EXISTS;").append(System.lineSeparator());
    }

    protected void appendCreateTableStatement(StringBuilder sb, Table table) {
        sb.append("CREATE TABLE ").append(table.id()).append(';').append(System.lineSeparator());
    }

    /**
     * Discard any currently-cached schemas and rebuild them using the filters.
     */
    protected void refreshSchemas() {
        clearSchemas();
        // Create TableSchema instances for any existing table ...
        this.tableIds().forEach(id -> {
            Table table = this.tableFor(id);
            buildAndRegisterSchema(table);
        });
    }

    public boolean isGlobalSetVariableStatement(String ddl, String databaseName) {
        return (databaseName == null || databaseName.isEmpty()) && ddl != null && ddl.toUpperCase().startsWith("SET ");
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

        // Record the DDL statement so that we can later recover them if needed. We do this _after_ writing the
        // schema change records so that failure recovery (which is based on of the history) won't lose
        // schema change records.
        // We are storing either
        // - all DDLs if configured
        // - or global SET variables
        // - or DDLs for monitored objects
        if (!storeOnlyCapturedTables() || isGlobalSetVariableStatement(schemaChange.getDdl(), schemaChange.getDatabase())
                || schemaChange.getTables().stream().map(Table::id).anyMatch(filters.dataCollectionFilter()::isIncluded)) {
            LOGGER.debug("Recorded DDL statements for database '{}': {}", schemaChange.getDatabase(), schemaChange.getDdl());
            record(schemaChange, schemaChange.getTableChanges());
        }
    }

    public List<SchemaChangeEvent> parseSnapshotDdl(MySqlPartition partition, String ddlStatements, String databaseName,
                                                    MySqlOffsetContext offset, Instant sourceTime) {
        LOGGER.debug("Processing snapshot DDL '{}' for database '{}'", ddlStatements, databaseName);
        return parseDdl(partition, ddlStatements, databaseName, offset, sourceTime, true);
    }

    public List<SchemaChangeEvent> parseStreamingDdl(MySqlPartition partition, String ddlStatements, String databaseName,
                                                     MySqlOffsetContext offset, Instant sourceTime) {
        LOGGER.debug("Processing streaming DDL '{}' for database '{}'", ddlStatements, databaseName);
        return parseDdl(partition, ddlStatements, databaseName, offset, sourceTime, false);
    }

    private List<SchemaChangeEvent> parseDdl(MySqlPartition partition, String ddlStatements, String databaseName,
                                             MySqlOffsetContext offset, Instant sourceTime, boolean snapshot) {
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
                ddlChanges.getEventsByDatabase((String dbName, List<Event> events) -> {
                    final String sanitizedDbName = (dbName == null) ? "" : dbName;
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
                            offset.tableEvent(dbName, tableIds, sourceTime);
                            // For SET with multiple parameters
                            if (event instanceof TableCreatedEvent) {
                                emitChangeEvent(partition, offset, schemaChangeEvents, sanitizedDbName, event, tableId,
                                        SchemaChangeEventType.CREATE, snapshot);
                            }
                            else if (event instanceof TableAlteredEvent || event instanceof TableIndexCreatedEvent || event instanceof TableIndexDroppedEvent) {
                                emitChangeEvent(partition, offset, schemaChangeEvents, sanitizedDbName, event, tableId,
                                        SchemaChangeEventType.ALTER, snapshot);
                            }
                            else if (event instanceof TableDroppedEvent) {
                                emitChangeEvent(partition, offset, schemaChangeEvents, sanitizedDbName, event, tableId,
                                        SchemaChangeEventType.DROP, snapshot);
                            }
                            else if (event instanceof TableTruncatedEvent) {
                                emitChangeEvent(partition, offset, schemaChangeEvents, sanitizedDbName, event, tableId,
                                        SchemaChangeEventType.TRUNCATE, snapshot);
                            }
                            else if (event instanceof SetVariableEvent) {
                                // SET statement with multiple variable emits event for each variable. We want to emit only
                                // one change event
                                final SetVariableEvent varEvent = (SetVariableEvent) event;
                                if (varEvent.order() == 0) {
                                    emitChangeEvent(partition, offset, schemaChangeEvents, sanitizedDbName, event,
                                            tableId, SchemaChangeEventType.DATABASE, snapshot);
                                }
                            }
                            else {
                                emitChangeEvent(partition, offset, schemaChangeEvents, sanitizedDbName, event, tableId,
                                        SchemaChangeEventType.DATABASE, snapshot);
                            }
                        });
                    }
                });
            }
            else {
                offset.databaseEvent(databaseName, sourceTime);
                schemaChangeEvents
                        .add(SchemaChangeEvent.ofDatabase(partition, offset, databaseName, ddlStatements, snapshot));
            }
        }
        else {
            LOGGER.debug("Changes for DDL '{}' were filtered and not recorded in database schema history", ddlStatements);
        }
        return schemaChangeEvents;
    }

    private void emitChangeEvent(MySqlPartition partition, MySqlOffsetContext offset, List<SchemaChangeEvent> schemaChangeEvents,
                                 final String sanitizedDbName, Event event, TableId tableId, SchemaChangeEventType type,
                                 boolean snapshot) {
        SchemaChangeEvent schemaChangeEvent;
        if (type.equals(SchemaChangeEventType.ALTER) && event instanceof TableAlteredEvent
                && ((TableAlteredEvent) event).previousTableId() != null) {
            schemaChangeEvent = SchemaChangeEvent.ofRename(
                    partition,
                    offset,
                    sanitizedDbName,
                    null,
                    event.statement(),
                    tableId != null ? tables().forTable(tableId) : null,
                    ((TableAlteredEvent) event).previousTableId());
        }
        else {
            schemaChangeEvent = SchemaChangeEvent.of(
                    type,
                    partition,
                    offset,
                    sanitizedDbName,
                    null,
                    event.statement(),
                    tableId != null ? tables().forTable(tableId) : null,
                    snapshot);
        }
        schemaChangeEvents.add(schemaChangeEvent);
    }

    private boolean acceptableDatabase(final String databaseName) {
        return !storeOnlyCapturedTables()
                || filters.databaseFilter().test(databaseName)
                || databaseName == null
                || databaseName.isEmpty();
    }

    private TableId getTableId(Event event) {
        if (event instanceof TableEvent) {
            return ((TableEvent) event).tableId();
        }
        else if (event instanceof TableIndexEvent) {
            return ((TableIndexEvent) event).tableId();
        }
        return null;
    }

    @Override
    protected DdlParser getDdlParser() {
        return ddlParser;
    }

    /**
     * Return true if the database schema history entity exists
     */
    public boolean historyExists() {
        return schemaHistory.exists();
    }

    /**
     * Assign the given table number to the table with the specified {@link TableId table ID}.
     *
     * @param tableNumber the table number found in binlog events
     * @param id the identifier for the corresponding table
     * @return {@code true} if the assignment was successful, or {@code false} if the table is currently excluded in the
     *         connector's configuration
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
     * Return the table id associated with MySQL-specific table number.
     *
     * @param tableNumber the table number from binlog
     * @return the table id or null if not known
     */
    public TableId getTableId(long tableNumber) {
        return tableIdsByTableNumber.get(tableNumber);
    }

    /**
     * Return the excluded table id associated with MySQL-specific table number.
     *
     * @param tableNumber the table number from binlog
     * @return the table id or null if not known
     */
    public TableId getExcludeTableId(long tableNumber) {
        return excludeTableIdsByTableNumber.get(tableNumber);
    }

    /**
     * Clear all of the table mappings. This should be done when the logs are rotated, since in that a different table
     * numbering scheme will be used by all subsequent TABLE_MAP binlog events.
     */
    public void clearTableMappings() {
        LOGGER.debug("Clearing table number mappings");
        tableIdsByTableNumber.clear();
        excludeTableIdsByTableNumber.clear();
    }

    @Override
    public void initializeStorage() {
        super.initializeStorage();
        storageInitializationExecuted = true;
    }

    public boolean isStorageInitializationExecuted() {
        return storageInitializationExecuted;
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
}
