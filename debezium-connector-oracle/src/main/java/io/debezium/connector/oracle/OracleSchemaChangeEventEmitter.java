/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.antlr.OracleDdlParser;
import io.debezium.connector.oracle.logminer.LogMinerAdapter;
import io.debezium.pipeline.spi.SchemaChangeEventEmitter;
import io.debezium.relational.Attribute;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.relational.ddl.DdlChanges;
import io.debezium.relational.ddl.DdlParserListener;
import io.debezium.relational.ddl.DdlParserListener.TableAlteredEvent;
import io.debezium.relational.ddl.DdlParserListener.TableCreatedEvent;
import io.debezium.relational.ddl.DdlParserListener.TableDroppedEvent;
import io.debezium.relational.ddl.DdlParserListener.TableTruncatedEvent;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.text.MultipleParsingExceptions;
import io.debezium.text.ParsingException;

/**
 * {@link SchemaChangeEventEmitter} implementation based on Oracle.
 *
 * @author Gunnar Morling
 */
public class OracleSchemaChangeEventEmitter implements SchemaChangeEventEmitter {

    private static final Logger LOGGER = LoggerFactory.getLogger(OracleSchemaChangeEventEmitter.class);

    private final OraclePartition partition;
    private final OracleOffsetContext offsetContext;
    private final TableId tableId;
    private final OracleDatabaseSchema schema;
    private final Instant changeTime;
    private final String sourceDatabaseName;
    private final String objectOwner;
    private final String ddlText;
    private final TableFilter filters;
    private final AbstractOracleStreamingChangeEventSourceMetrics streamingMetrics;
    private final TruncateReceiver truncateReceiver;
    private final OracleConnectorConfig connectorConfig;
    private final Long objectId;
    private final Long dataObjectId;

    public OracleSchemaChangeEventEmitter(OracleConnectorConfig connectorConfig, OraclePartition partition,
                                          OracleOffsetContext offsetContext, TableId tableId, String sourceDatabaseName,
                                          String objectOwner, String ddlText, OracleDatabaseSchema schema,
                                          Instant changeTime, AbstractOracleStreamingChangeEventSourceMetrics streamingMetrics,
                                          TruncateReceiver truncateReceiver) {
        this(connectorConfig, partition, offsetContext, tableId, sourceDatabaseName, objectOwner, null, null,
                ddlText, schema, changeTime, streamingMetrics, truncateReceiver);
    }

    public OracleSchemaChangeEventEmitter(OracleConnectorConfig connectorConfig, OraclePartition partition,
                                          OracleOffsetContext offsetContext, TableId tableId, String sourceDatabaseName,
                                          String objectOwner, Long objectId, Long dataObjectId, String ddlText,
                                          OracleDatabaseSchema schema, Instant changeTime,
                                          AbstractOracleStreamingChangeEventSourceMetrics streamingMetrics,
                                          TruncateReceiver truncateReceiver) {
        this.partition = partition;
        this.offsetContext = offsetContext;
        this.tableId = tableId;
        this.sourceDatabaseName = sourceDatabaseName;
        this.objectOwner = objectOwner;
        this.ddlText = ddlText;
        this.schema = schema;
        this.changeTime = changeTime;
        this.streamingMetrics = streamingMetrics;
        this.filters = connectorConfig.getTableFilters().dataCollectionFilter();
        this.truncateReceiver = truncateReceiver;
        this.connectorConfig = connectorConfig;
        // These are only provided by LogMiner
        this.objectId = objectId;
        this.dataObjectId = dataObjectId;
    }

    @Override
    public void emitSchemaChangeEvent(Receiver receiver) throws InterruptedException {
        // Cache a copy of the table's schema prior to parsing the DDL.
        // This is needed in the event that the parsed DDL is a drop table
        // todo: verify whether this is actually necessary in the emitted SchemaChangeEvent
        final Table tableBefore = schema.tableFor(tableId);

        final OracleDdlParser parser = schema.getDdlParser();
        final DdlChanges ddlChanges = parser.getDdlChanges();
        try {
            ddlChanges.reset();
            parser.setCurrentDatabase(sourceDatabaseName);
            parser.setCurrentSchema(objectOwner);
            parser.parse(ddlText, schema.getTables());
        }
        catch (ParsingException | MultipleParsingExceptions e) {
            if (schema.skipUnparseableDdlStatements()) {
                LOGGER.warn("Ignoring unparsable DDL statement '{}':", ddlText, e);
                streamingMetrics.incrementWarningCount();
                streamingMetrics.incrementSchemaChangeParseErrorCount();
            }
            else {
                throw e;
            }
        }

        if (!ddlChanges.isEmpty() && (filters.isIncluded(tableId) || !schema.storeOnlyCapturedTables())) {
            List<SchemaChangeEvent> changeEvents = new ArrayList<>();
            ddlChanges.getEventsByDatabase((String dbName, List<DdlParserListener.Event> events) -> {
                events.forEach(event -> {
                    switch (event.type()) {
                        case CREATE_TABLE:
                            changeEvents.add(createTableEvent(partition, (TableCreatedEvent) event));
                            break;
                        case ALTER_TABLE:
                            changeEvents.add(alterTableEvent(partition, (TableAlteredEvent) event));
                            break;
                        case DROP_TABLE:
                            changeEvents.add(dropTableEvent(partition, tableBefore, (TableDroppedEvent) event));
                            break;
                        case TRUNCATE_TABLE:
                            changeEvents.add(truncateTableEvent(partition, (TableTruncatedEvent) event));
                            break;
                        default:
                            LOGGER.info("Skipped DDL event type {}: {}", event.type(), ddlText);
                            break;
                    }
                });
            });

            for (SchemaChangeEvent event : changeEvents) {
                if (!schema.skipSchemaChangeEvent(event)) {
                    if (SchemaChangeEvent.SchemaChangeEventType.TRUNCATE == event.getType()) {
                        truncateReceiver.processTruncateEvent();
                    }
                    else {
                        receiver.schemaChangeEvent(event);
                    }

                }
            }
        }
    }

    private SchemaChangeEvent createTableEvent(OraclePartition partition, TableCreatedEvent event) {
        applyTableObjectAttributes(tableId);
        offsetContext.tableEvent(tableId, changeTime);
        return SchemaChangeEvent.ofCreate(
                partition,
                offsetContext,
                tableId.catalog(),
                tableId.schema(),
                event.statement(),
                schema.tableFor(event.tableId()),
                false);
    }

    private SchemaChangeEvent alterTableEvent(OraclePartition partition, TableAlteredEvent event) {
        final Set<TableId> tableIds = new LinkedHashSet<>();
        tableIds.add(tableId);
        tableIds.add(event.tableId());

        applyTableObjectAttributes(event.tableId());
        offsetContext.tableEvent(tableIds, changeTime);
        if (tableId == null) {
            return SchemaChangeEvent.ofAlter(
                    partition,
                    offsetContext,
                    tableId.catalog(),
                    tableId.schema(),
                    event.statement(),
                    schema.tableFor(event.tableId()));
        }
        else {
            return SchemaChangeEvent.ofRename(
                    partition,
                    offsetContext,
                    tableId.catalog(),
                    tableId.schema(),
                    event.statement(),
                    schema.tableFor(event.tableId()),
                    tableId);
        }
    }

    private SchemaChangeEvent dropTableEvent(OraclePartition partition, Table tableSchemaBeforeDrop, TableDroppedEvent event) {
        // intentionally no need to apply table attributes
        offsetContext.tableEvent(tableId, changeTime);
        return SchemaChangeEvent.ofDrop(
                partition,
                offsetContext,
                tableId.catalog(),
                tableId.schema(),
                event.statement(),
                tableSchemaBeforeDrop);
    }

    private SchemaChangeEvent truncateTableEvent(OraclePartition partition, TableTruncatedEvent event) {
        applyTableObjectAttributes(event.tableId());
        offsetContext.tableEvent(tableId, changeTime);
        return SchemaChangeEvent.ofTruncate(
                partition,
                offsetContext,
                tableId.catalog(),
                tableId.schema(),
                event.statement(),
                schema.tableFor(event.tableId()));
    }

    void applyTableObjectAttributes(TableId tableId) {
        if (connectorConfig.getAdapter() instanceof LogMinerAdapter) {
            if (OracleConnectorConfig.LogMiningStrategy.HYBRID.equals(connectorConfig.getLogMiningStrategy())) {
                // todo: centralize this with other attribute set locations
                final Table table = schema.tableFor(tableId);
                if (table != null) {
                    // If the table has not yet been registered, there is nothing to apply
                    final TableEditor editor = table.edit();
                    editor.addAttribute(Attribute.editor().name("OBJECT_ID").value(objectId).create());
                    editor.addAttribute(Attribute.editor().name("DATA_OBJECT_ID").value(dataObjectId).create());
                    schema.getTables().overwriteTable(editor.create());
                }
                else {
                    LOGGER.debug("Cannot apply table attributes to table '{}', schema is not yet registered.", tableId);
                }
            }
        }
    }

}
