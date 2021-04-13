/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;

import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.document.Array;
import io.debezium.function.Predicates;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.relational.history.TableChanges.TableChange;
import io.debezium.relational.history.TableChanges.TableChangeType;
import io.debezium.text.ParsingException;

/**
 * @author Randall Hauch
 *
 */
public abstract class AbstractDatabaseHistory implements DatabaseHistory {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    // Temporary preference for DDL over logical schema due to DBZ-32
    public static final Field INTERNAL_PREFER_DDL = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "prefer.ddl")
            .withDisplayName("Prefer DDL for schema recovery")
            .withType(Type.BOOLEAN)
            .withDefault(false)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("Prefer DDL for schema reovery in case logica schema is present")
            .withInvisibleRecommender()
            .withNoValidation();

    protected Configuration config;
    private HistoryRecordComparator comparator = HistoryRecordComparator.INSTANCE;
    private boolean skipUnparseableDDL;
    private boolean storeOnlyMonitoredTablesDdl;
    private Function<String, Optional<Pattern>> ddlFilter = (x -> Optional.empty());
    private DatabaseHistoryListener listener = DatabaseHistoryListener.NOOP;
    private boolean useCatalogBeforeSchema;
    private boolean preferDdl = false;
    private TableChanges.TableChangesSerializer<Array> tableChangesSerializer = new JsonTableChangeSerializer();

    protected AbstractDatabaseHistory() {
    }

    @Override
    public void configure(Configuration config, HistoryRecordComparator comparator, DatabaseHistoryListener listener, boolean useCatalogBeforeSchema) {
        this.config = config;
        this.comparator = comparator != null ? comparator : HistoryRecordComparator.INSTANCE;
        this.skipUnparseableDDL = config.getBoolean(DatabaseHistory.SKIP_UNPARSEABLE_DDL_STATEMENTS);
        this.storeOnlyMonitoredTablesDdl = config.getBoolean(DatabaseHistory.STORE_ONLY_MONITORED_TABLES_DDL);

        final String ddlFilter = config.getString(DatabaseHistory.DDL_FILTER);
        this.ddlFilter = (ddlFilter != null) ? Predicates.matchedBy(ddlFilter) : this.ddlFilter;
        this.listener = listener;
        this.useCatalogBeforeSchema = useCatalogBeforeSchema;
        this.preferDdl = config.getBoolean(INTERNAL_PREFER_DDL);
    }

    @Override
    public void start() {
        listener.started();
    }

    @Override
    public final void record(Map<String, ?> source, Map<String, ?> position, String databaseName, String ddl)
            throws DatabaseHistoryException {

        record(source, position, databaseName, null, ddl, null);
    }

    @Override
    public final void record(Map<String, ?> source, Map<String, ?> position, String databaseName, String schemaName, String ddl, TableChanges changes)
            throws DatabaseHistoryException {
        final HistoryRecord record = new HistoryRecord(source, position, databaseName, schemaName, ddl, changes);
        storeRecord(record);
        listener.onChangeApplied(record);
    }

    @Override
    public final void recover(Map<String, ?> source, Map<String, ?> position, Tables schema, DdlParser ddlParser) {
        logger.debug("Recovering DDL history for source partition {} and offset {}", source, position);
        listener.recoveryStarted();
        HistoryRecord stopPoint = new HistoryRecord(source, position, null, null, null, null);
        recoverRecords(recovered -> {
            listener.onChangeFromHistory(recovered);
            if (comparator.isAtOrBefore(recovered, stopPoint)) {
                Array tableChanges = recovered.tableChanges();
                String ddl = recovered.ddl();

                if (!preferDdl && tableChanges != null && !tableChanges.isEmpty()) {
                    TableChanges changes = tableChangesSerializer.deserialize(tableChanges, useCatalogBeforeSchema);
                    for (TableChange entry : changes) {
                        if (entry.getType() == TableChangeType.CREATE || entry.getType() == TableChangeType.ALTER) {
                            schema.overwriteTable(entry.getTable());
                        }
                        // DROP
                        else {
                            schema.removeTable(entry.getId());
                        }
                    }
                    listener.onChangeApplied(recovered);
                }
                else if (ddl != null && ddlParser != null) {
                    if (recovered.databaseName() != null) {
                        ddlParser.setCurrentDatabase(recovered.databaseName()); // may be null
                    }
                    if (recovered.schemaName() != null) {
                        ddlParser.setCurrentSchema(recovered.schemaName()); // may be null
                    }
                    Optional<Pattern> filteredBy = ddlFilter.apply(ddl);
                    if (filteredBy.isPresent()) {
                        logger.info("a DDL '{}' was filtered out of processing by regular expression '{}", ddl, filteredBy.get());
                        return;
                    }
                    try {
                        logger.debug("Applying: {}", ddl);
                        ddlParser.parse(ddl, schema);
                        listener.onChangeApplied(recovered);
                    }
                    catch (final ParsingException e) {
                        if (skipUnparseableDDL) {
                            logger.warn("Ignoring unparseable statements '{}' stored in database history: {}", ddl, e);
                        }
                        else {
                            throw e;
                        }
                    }
                }
            }
            else {
                logger.debug("Skipping: {}", recovered.ddl());
            }
        });
        listener.recoveryStopped();
    }

    protected abstract void storeRecord(HistoryRecord record) throws DatabaseHistoryException;

    protected abstract void recoverRecords(Consumer<HistoryRecord> records);

    @Override
    public void stop() {
        listener.stopped();
    }

    @Override
    public void initializeStorage() {
    }

    @Override
    public boolean storeOnlyMonitoredTables() {
        return storeOnlyMonitoredTablesDdl;
    }

    @Override
    public boolean skipUnparseableDdlStatements() {
        return skipUnparseableDDL;
    }
}
