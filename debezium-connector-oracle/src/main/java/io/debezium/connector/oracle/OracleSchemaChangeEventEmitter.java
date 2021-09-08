/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.antlr.OracleDdlParser;
import io.debezium.pipeline.spi.SchemaChangeEventEmitter;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.relational.ddl.DdlChanges;
import io.debezium.relational.ddl.DdlParserListener;
import io.debezium.relational.ddl.DdlParserListener.TableAlteredEvent;
import io.debezium.relational.ddl.DdlParserListener.TableCreatedEvent;
import io.debezium.relational.ddl.DdlParserListener.TableDroppedEvent;
import io.debezium.relational.history.DatabaseHistory;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.SchemaChangeEvent.SchemaChangeEventType;
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
    private final DatabaseHistory databaseHistory;
    private final Instant changeTime;
    private final String sourceDatabaseName;
    private final String objectOwner;
    private final String ddlText;
    private final TableFilter filters;
    private final OracleStreamingChangeEventSourceMetrics streamingMetrics;

    public OracleSchemaChangeEventEmitter(OracleConnectorConfig connectorConfig, OraclePartition partition,
                                          OracleOffsetContext offsetContext, TableId tableId, String sourceDatabaseName,
                                          String objectOwner, String ddlText, OracleDatabaseSchema schema,
                                          Instant changeTime, OracleStreamingChangeEventSourceMetrics streamingMetrics) {
        this.partition = partition;
        this.offsetContext = offsetContext;
        this.tableId = tableId;
        this.sourceDatabaseName = sourceDatabaseName;
        this.objectOwner = objectOwner;
        this.ddlText = ddlText;
        this.schema = schema;
        this.databaseHistory = connectorConfig.getDatabaseHistory();
        this.changeTime = changeTime;
        this.streamingMetrics = streamingMetrics;
        this.filters = connectorConfig.getTableFilters().dataCollectionFilter();
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
            if (databaseHistory.skipUnparseableDdlStatements()) {
                LOGGER.warn("Ignoring unparsable DDL statement '{}': {}", ddlText, e);
                streamingMetrics.incrementWarningCount();
                streamingMetrics.incrementUnparsableDdlCount();
            }
            else {
                throw e;
            }
        }

        if (!ddlChanges.isEmpty() && filters.isIncluded(tableId)) {
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
                        default:
                            LOGGER.info("Skipped DDL event type {}: {}", event.type(), ddlText);
                            break;
                    }
                });
            });

            for (SchemaChangeEvent event : changeEvents) {
                receiver.schemaChangeEvent(event);
            }
        }
    }

    private SchemaChangeEvent createTableEvent(OraclePartition partition, TableCreatedEvent event) {
        offsetContext.tableEvent(tableId, changeTime);
        return new SchemaChangeEvent(
                partition.getSourcePartition(),
                offsetContext.getOffset(),
                offsetContext.getSourceInfo(),
                tableId.catalog(),
                tableId.schema(),
                event.statement(),
                schema.tableFor(event.tableId()),
                SchemaChangeEventType.CREATE,
                false);
    }

    private SchemaChangeEvent alterTableEvent(OraclePartition partition, TableAlteredEvent event) {
        final Set<TableId> tableIds = new HashSet<>();
        tableIds.add(tableId);
        tableIds.add(event.tableId());

        offsetContext.tableEvent(tableIds, changeTime);
        return new SchemaChangeEvent(
                partition.getSourcePartition(),
                offsetContext.getOffset(),
                offsetContext.getSourceInfo(),
                tableId.catalog(),
                tableId.schema(),
                event.statement(),
                schema.tableFor(event.tableId()),
                SchemaChangeEventType.ALTER,
                false);
    }

    private SchemaChangeEvent dropTableEvent(OraclePartition partition, Table tableSchemaBeforeDrop, TableDroppedEvent event) {
        offsetContext.tableEvent(tableId, changeTime);
        return new SchemaChangeEvent(
                partition.getSourcePartition(),
                offsetContext.getOffset(),
                offsetContext.getSourceInfo(),
                tableId.catalog(),
                tableId.schema(),
                event.statement(),
                tableSchemaBeforeDrop,
                SchemaChangeEventType.DROP,
                false);
    }
}
