/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.document.Array;
import io.debezium.document.Document;
import io.debezium.function.Predicates;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.relational.history.TableChanges.TableChange;
import io.debezium.relational.history.TableChanges.TableChangeType;
import io.debezium.relational.history.TableChanges.TableChangesSerializer;
import io.debezium.text.MultipleParsingExceptions;
import io.debezium.text.ParsingException;
import io.debezium.util.Clock;
import io.debezium.util.Loggings;

/**
 * @author Randall Hauch
 *
 */
public abstract class AbstractSchemaHistory implements SchemaHistory {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    public static Field.Set ALL_FIELDS = Field.setOf(NAME, INTERNAL_CONNECTOR_CLASS, INTERNAL_CONNECTOR_ID);

    protected Configuration config;
    private HistoryRecordComparator comparator = HistoryRecordComparator.INSTANCE;
    private boolean skipUnparseableDDL;
    private Predicate<String> ddlFilter = x -> false;
    private SchemaHistoryListener listener = SchemaHistoryListener.NOOP;
    private boolean useCatalogBeforeSchema;
    private boolean preferDdl = false;
    private final TableChangesSerializer<Array> tableChangesSerializer = new JsonTableChangeSerializer();

    protected AbstractSchemaHistory() {
    }

    @Override
    public void configure(Configuration config, HistoryRecordComparator comparator, SchemaHistoryListener listener, boolean useCatalogBeforeSchema) {
        this.config = config;
        this.comparator = comparator != null ? comparator : HistoryRecordComparator.INSTANCE;
        this.skipUnparseableDDL = config.getBoolean(SKIP_UNPARSEABLE_DDL_STATEMENTS);

        final String ddlFilter = config.getString(DDL_FILTER);
        this.ddlFilter = (ddlFilter != null) ? Predicates.includes(ddlFilter, Pattern.CASE_INSENSITIVE | Pattern.DOTALL) : (x -> false);
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
            throws SchemaHistoryException {

        record(source, position, databaseName, null, ddl, null, Clock.SYSTEM.currentTimeAsInstant());
    }

    @Override
    public final void record(Map<String, ?> source, Map<String, ?> position, String databaseName, String schemaName,
                             String ddl, TableChanges changes, Instant timestamp)
            throws SchemaHistoryException {
        final HistoryRecord record = new HistoryRecord(source, position, databaseName, schemaName, ddl, changes, timestamp);
        storeRecord(record);
        listener.onChangeApplied(record);
    }

    @Override
    public void recover(Map<Map<String, ?>, Map<String, ?>> offsets, Tables schema, DdlParser ddlParser) {
        listener.recoveryStarted();
        Map<Document, HistoryRecord> stopPoints = new HashMap<>();
        offsets.forEach((Map<String, ?> source, Map<String, ?> position) -> {
            Document srcDocument = Document.create();
            if (source != null) {
                source.forEach(srcDocument::set);
            }
            stopPoints.put(srcDocument, new HistoryRecord(source, position, null, null, null, null, null));
        });

        recoverRecords(recovered -> {
            listener.onChangeFromHistory(recovered);
            Document srcDocument = recovered.document().getDocument(HistoryRecord.Fields.SOURCE);
            if (stopPoints.containsKey(srcDocument) && comparator.isAtOrBefore(recovered, stopPoints.get(srcDocument))) {
                Array tableChanges = recovered.tableChanges();
                String ddl = recovered.ddl();

                if (!preferDdl && tableChanges != null && !tableChanges.isEmpty()) {
                    TableChanges changes = tableChangesSerializer.deserialize(tableChanges, useCatalogBeforeSchema);
                    for (TableChange entry : changes) {
                        if (entry.getType() == TableChangeType.CREATE) {
                            schema.overwriteTable(entry.getTable());
                        }
                        else if (entry.getType() == TableChangeType.ALTER) {
                            if (entry.getPreviousId() != null) {
                                schema.removeTable(entry.getPreviousId());
                            }
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
                    if (ddlFilter.test(ddl)) {
                        logger.info("a DDL '{}' was filtered out of processing by regular expression '{}'",
                                Loggings.maybeRedactSensitiveData(ddl), config.getString(DDL_FILTER));
                        return;
                    }
                    try {
                        logger.debug("Applying: {}", ddl);
                        ddlParser.parse(ddl, schema);
                        listener.onChangeApplied(recovered);
                    }
                    catch (final ParsingException | MultipleParsingExceptions e) {
                        if (skipUnparseableDDL) {
                            logger.warn("Ignoring unparseable statements '{}' stored in database schema history", ddl, e);
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

    protected abstract void storeRecord(HistoryRecord record) throws SchemaHistoryException;

    protected abstract void recoverRecords(Consumer<HistoryRecord> records);

    @Override
    public void stop() {
        listener.stopped();
    }

    @Override
    public void initializeStorage() {
    }
}
