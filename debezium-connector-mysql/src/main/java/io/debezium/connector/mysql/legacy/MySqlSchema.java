/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.legacy;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.annotation.NotThreadSafe;
import io.debezium.config.CommonConnectorConfig.EventProcessingFailureHandlingMode;
import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnector;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlConnectorConfig.BigIntUnsignedHandlingMode;
import io.debezium.connector.mysql.MySqlSystemVariables.MySqlScope;
import io.debezium.connector.mysql.MySqlValueConverters;
import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.document.Document;
import io.debezium.jdbc.JdbcValueConverters.BigIntUnsignedMode;
import io.debezium.jdbc.JdbcValueConverters.DecimalMode;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.SystemVariables;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.relational.Tables;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.relational.ddl.DdlChanges;
import io.debezium.relational.ddl.DdlChanges.DatabaseStatementStringConsumer;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.relational.history.DatabaseHistory;
import io.debezium.relational.history.DatabaseHistoryMetrics;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.relational.history.KafkaDatabaseHistory;
import io.debezium.schema.TopicSelector;
import io.debezium.text.MultipleParsingExceptions;
import io.debezium.text.ParsingException;
import io.debezium.util.Collect;
import io.debezium.util.SchemaNameAdjuster;

/**
 * Component that records the schema history for databases hosted by a MySQL database server. The schema information includes
 * the {@link Tables table definitions} and the Kafka Connect {@link #schemaFor(TableId) Schema}s for each table, where the
 * {@link Schema} excludes any columns that have been {@link MySqlConnectorConfig#COLUMN_EXCLUDE_LIST specified} in the
 * configuration.
 * <p>
 * The history is changed by {@link #applyDdl(SourceInfo, String, String, DatabaseStatementStringConsumer) applying DDL
 * statements}, and every change is {@link DatabaseHistory persisted} as defined in the supplied {@link MySqlConnectorConfig MySQL
 * connector configuration}. This component can be reconstructed (e.g., on connector restart) and the history
 * {@link #loadHistory(SourceInfo) loaded} from persisted storage.
 * <p>
 * Note that when {@link #applyDdl(SourceInfo, String, String, DatabaseStatementStringConsumer) applying DDL statements}, the
 * caller is able to supply a {@link DatabaseStatementStringConsumer consumer function} that will be called with the DDL
 * statements and the database to which they apply, grouped by database names. However, these will only be called based when the
 * databases are included by the database filters defined in the {@link MySqlConnectorConfig MySQL connector configuration}.
 *
 * @author Randall Hauch
 */
@NotThreadSafe
public class MySqlSchema extends RelationalDatabaseSchema {

    private final static Logger logger = LoggerFactory.getLogger(MySqlSchema.class);

    private final Set<String> ignoredQueryStatements = Collect.unmodifiableSet("BEGIN", "END", "FLUSH PRIVILEGES");
    private final DdlParser ddlParser;
    private final Filters filters;
    private final DatabaseHistory dbHistory;
    private final DdlChanges ddlChanges;
    private final HistoryRecordComparator historyComparator;
    private final boolean skipUnparseableDDL;
    private final boolean storeOnlyMonitoredTablesDdl;
    private boolean recoveredTables;

    /**
     * Create a schema component given the supplied {@link MySqlConnectorConfig MySQL connector configuration}.
     *
     * @param configuration the connector configuration, which is presumed to be valid
     * @param gtidFilter the predicate function that should be applied to GTID sets in database history, and which
     *          returns {@code true} if a GTID source is to be included, or {@code false} if a GTID source is to be excluded;
     *          may be null if not needed
     * @param tableIdCaseInsensitive true if table lookup ignores letter case
     */
    public MySqlSchema(MySqlConnectorConfig configuration,
                       Predicate<String> gtidFilter,
                       boolean tableIdCaseInsensitive,
                       TopicSelector<TableId> topicSelector,
                       Filters tableFilters) {
        super(
                configuration,
                topicSelector,
                TableFilter.fromPredicate(tableFilters.tableFilter()),
                tableFilters.columnFilter(),
                new TableSchemaBuilder(
                        getValueConverters(configuration), SchemaNameAdjuster.create(),
                        configuration.customConverterRegistry(),
                        configuration.getSourceInfoStructMaker().schema(),
                        configuration.getSanitizeFieldNames()),
                tableIdCaseInsensitive,
                configuration.getKeyMapper());

        Configuration config = configuration.getConfig();

        this.filters = tableFilters;

        // Do not remove the prefix from the subset of config properties ...
        String connectorName = config.getString("name", configuration.getLogicalName());
        Configuration dbHistoryConfig = config.subset(DatabaseHistory.CONFIGURATION_FIELD_PREFIX_STRING, false)
                .edit()
                .withDefault(DatabaseHistory.NAME, connectorName + "-dbhistory")
                .with(KafkaDatabaseHistory.INTERNAL_CONNECTOR_CLASS, MySqlConnector.class.getName())
                .with(KafkaDatabaseHistory.INTERNAL_CONNECTOR_ID, configuration.getLogicalName())
                .build();
        this.skipUnparseableDDL = dbHistoryConfig.getBoolean(DatabaseHistory.SKIP_UNPARSEABLE_DDL_STATEMENTS);
        this.storeOnlyMonitoredTablesDdl = dbHistoryConfig.getBoolean(DatabaseHistory.STORE_ONLY_MONITORED_TABLES_DDL);

        this.ddlParser = new MySqlAntlrDdlParser(getValueConverters(configuration), getTableFilter());
        this.ddlChanges = this.ddlParser.getDdlChanges();

        // Create and configure the database history ...
        this.dbHistory = config.getInstance(MySqlConnectorConfig.DATABASE_HISTORY, DatabaseHistory.class);
        if (this.dbHistory == null) {
            throw new ConnectException("Unable to instantiate the database history class " +
                    config.getString(MySqlConnectorConfig.DATABASE_HISTORY));
        }

        // Set up a history record comparator that uses the GTID filter ...
        this.historyComparator = new HistoryRecordComparator() {
            @Override
            protected boolean isPositionAtOrBefore(Document recorded, Document desired) {
                return SourceInfo.isPositionAtOrBefore(recorded, desired, gtidFilter);
            }
        };
        this.dbHistory.configure(dbHistoryConfig, historyComparator, new DatabaseHistoryMetrics(configuration), true); // validates
    }

    private static MySqlValueConverters getValueConverters(MySqlConnectorConfig configuration) {
        // Use MySQL-specific converters and schemas for values ...

        TemporalPrecisionMode timePrecisionMode = configuration.getTemporalPrecisionMode();

        DecimalMode decimalMode = configuration.getDecimalMode();

        String bigIntUnsignedHandlingModeStr = configuration.getConfig().getString(MySqlConnectorConfig.BIGINT_UNSIGNED_HANDLING_MODE);
        BigIntUnsignedHandlingMode bigIntUnsignedHandlingMode = BigIntUnsignedHandlingMode.parse(bigIntUnsignedHandlingModeStr);
        BigIntUnsignedMode bigIntUnsignedMode = bigIntUnsignedHandlingMode.asBigIntUnsignedMode();
        final boolean timeAdjusterEnabled = configuration.getConfig().getBoolean(MySqlConnectorConfig.ENABLE_TIME_ADJUSTER);
        // TODO With MySQL connector rewrite the error handling should report also binlog coordinates
        return new MySqlValueConverters(decimalMode, timePrecisionMode, bigIntUnsignedMode,
                configuration.binaryHandlingMode(), timeAdjusterEnabled ? MySqlValueConverters::adjustTemporal : x -> x,
                (message, exception) -> {
                    if (configuration
                            .getEventProcessingFailureHandlingMode() == EventProcessingFailureHandlingMode.FAIL) {
                        throw new DebeziumException(message, exception);
                    }
                    else if (configuration
                            .getEventProcessingFailureHandlingMode() == EventProcessingFailureHandlingMode.WARN) {
                        logger.warn(message, exception);
                    }
                });
    }

    public HistoryRecordComparator historyComparator() {
        return this.historyComparator;
    }

    /**
     * Start by acquiring resources needed to persist the database history
     */
    public synchronized void start() {
        this.dbHistory.start();
    }

    /**
     * Stop recording history and release any resources acquired since {@link #start()}.
     */
    public synchronized void shutdown() {
        this.dbHistory.stop();
    }

    /**
     * Get the {@link Filters database and table filters} defined by the configuration.
     *
     * @return the filters; never null
     */
    public Filters filters() {
        return filters;
    }

    /**
     * Get all table names for all databases that are monitored whose events are captured by Debezium
     *
     * @return the array with the table names
     */
    public String[] monitoredTablesAsStringArray() {
        final Collection<TableId> tables = tableIds();
        String[] ret = new String[tables.size()];
        int i = 0;
        for (TableId table : tables) {
            ret[i++] = table.toString();
        }
        return ret;
    }

    /**
     * Decide whether events should be captured for a given table
     *
     * @param id the fully-qualified table identifier; may be null
     * @return true if events from the table are captured
     */
    public boolean isTableMonitored(TableId id) {
        return filters.tableFilter().test(id);
    }

    /**
     * Get the information about where the DDL statement history is recorded.
     *
     * @return the history description; never null
     */
    public String historyLocation() {
        return dbHistory.toString();
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
     * Load the database schema information using the previously-recorded history, and stop reading the history when the
     * the history reaches the supplied starting point.
     *
     * @param startingPoint the source information with the current {@link SourceInfo#partition()} and {@link SourceInfo#offset()
     *            offset} at which the database schemas are to reflect; may not be null
     */
    public void loadHistory(SourceInfo startingPoint) {
        tables().clear();
        dbHistory.recover(startingPoint.partition(), startingPoint.offset(), tables(), ddlParser);
        recoveredTables = !tableIds().isEmpty();
        refreshSchemas();
    }

    /**
     * Return true if the database history entity exists
     */
    public boolean historyExists() {
        return dbHistory.exists();
    }

    /**
     * Initialize permanent storage for database history
     */
    public void intializeHistoryStorage() {
        if (!dbHistory.storageExists()) {
            dbHistory.initializeStorage();
        }
    }

    /**
     * Discard any currently-cached schemas and rebuild them using the filters.
     */
    public void refreshSchemas() {
        clearSchemas();
        // Create TableSchema instances for any existing table ...
        this.tableIds().forEach(id -> {
            Table table = this.tableFor(id);
            buildAndRegisterSchema(table);
        });
    }

    /**
     * Apply the supplied DDL statements to this database schema and record the history. If a {@code statementConsumer} is
     * supplied, then call it for each sub-sequence of the DDL statements that all apply to the same database.
     * <p>
     * Typically DDL statements are applied using a connection to a single database, and unless the statements use fully-qualified
     * names, the DDL statements apply to this database.
     *
     * @param source the current {@link SourceInfo#partition()} and {@link SourceInfo#offset() offset} at which these changes are
     *            found; may not be null
     * @param databaseName the name of the default database under which these statements are applied; may not be null
     * @param ddlStatements the {@code ;}-separated DDL statements; may be null or empty
     * @param statementConsumer the consumer that should be called with each sub-sequence of DDL statements that apply to
     *            a single database; may be null if no action is to be performed with the changes
     * @return {@code true} if changes were made to the database schema, or {@code false} if the DDL statements had no
     *         effect on the database schema
     */
    public boolean applyDdl(SourceInfo source, String databaseName, String ddlStatements,
                            DatabaseStatementStringConsumer statementConsumer) {
        Set<TableId> changes;
        if (ignoredQueryStatements.contains(ddlStatements)) {
            return false;
        }
        try {
            this.ddlChanges.reset();
            this.ddlParser.setCurrentSchema(databaseName);
            this.ddlParser.parse(ddlStatements, tables());
        }
        catch (ParsingException | MultipleParsingExceptions e) {
            if (skipUnparseableDDL) {
                logger.warn("Ignoring unparseable DDL statement '{}': {}", ddlStatements, e);
            }
            else {
                throw e;
            }
        }
        changes = tables().drainChanges();
        // No need to send schema events or store DDL if no table has changed
        if (!storeOnlyMonitoredTablesDdl || ddlChanges.anyMatch(filters.databaseFilter(), filters.tableFilter())) {
            if (statementConsumer != null) {

                // We are supposed to _also_ record the schema changes as SourceRecords, but these need to be filtered
                // by database. Unfortunately, the databaseName on the event might not be the same database as that
                // being modified by the DDL statements (since the DDL statements can have fully-qualified names).
                // Therefore, we have to look at each statement to figure out which database it applies and then
                // record the DDL statements (still in the same order) to those databases.

                if (!ddlChanges.isEmpty() && ddlChanges.applyToMoreDatabasesThan(databaseName)) {

                    // We understood at least some of the DDL statements and can figure out to which database they apply.
                    // They also apply to more databases than 'databaseName', so we need to apply the DDL statements in
                    // the same order they were read for each _affected_ database, grouped together if multiple apply
                    // to the same _affected_ database...
                    ddlChanges.groupStatementStringsByDatabase((dbName, tables, ddl) -> {
                        if (filters.databaseFilter().test(dbName) || dbName == null || "".equals(dbName)) {
                            if (dbName == null) {
                                dbName = "";
                            }
                            statementConsumer.consume(dbName, tables, ddl);
                        }
                    });
                }
                else if (filters.databaseFilter().test(databaseName) || databaseName == null || "".equals(databaseName)) {
                    if (databaseName == null) {
                        databaseName = "";
                    }
                    statementConsumer.consume(databaseName, changes, ddlStatements);
                }
            }

            // Record the DDL statement so that we can later recover them if needed. We do this _after_ writing the
            // schema change records so that failure recovery (which is based on of the history) won't lose
            // schema change records.
            // We are storing either
            // - all DDLs if configured
            // - or global SET variables
            // - or DDLs for monitored objects
            if (!storeOnlyMonitoredTablesDdl || isGlobalSetVariableStatement(ddlStatements, databaseName) || changes.stream().anyMatch(filters().tableFilter()::test)) {
                dbHistory.record(source.partition(), source.offset(), databaseName, ddlStatements);
            }
        }
        else {
            logger.debug("Changes for DDL '{}' were filtered and not recorded in database history", ddlStatements);
        }

        // Figure out what changed ...
        changes.forEach(tableId -> {
            Table table = tableFor(tableId);
            if (table == null) { // removed
                removeSchema(tableId);
            }
            else {
                buildAndRegisterSchema(table);
            }
        });
        return true;
    }

    public boolean isGlobalSetVariableStatement(String ddl, String databaseName) {
        return databaseName == null && ddl != null && ddl.toUpperCase().startsWith("SET ");
    }

    /**
     * @return true if only monitored tables should be stored in database history, false if all tables should be stored
     */
    public boolean isStoreOnlyMonitoredTablesDdl() {
        return storeOnlyMonitoredTablesDdl;
    }

    @Override
    public boolean tableInformationComplete() {
        return recoveredTables;
    }
}
