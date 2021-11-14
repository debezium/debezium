/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.BlobChunkList;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleValueConverters;
import io.debezium.connector.oracle.logminer.LogMinerHelper;
import io.debezium.connector.oracle.logminer.events.DmlEvent;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.LobWriteEvent;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;
import io.debezium.connector.oracle.logminer.events.SelectLobLocatorEvent;
import io.debezium.function.BlockingConsumer;
import io.debezium.relational.Table;

/**
 * A consumer of transaction events at commit time that is capable of inspecting the event stream,
 * merging events that should be merged when LOB support is enabled, and then delegating the final
 * stream of events to a delegate consumer.
 *
 * When a table has a LOB field, Oracle LogMiner often supplies us with synthetic events that deal
 * with sub-tasks that occur in the database as a result of writing LOB data to the database.  We
 * would prefer to emit these synthetic events as a part of the overall logical event, whether that
 * is an insert or update.
 *
 * An example of a scenario would be the following logical user action:
 *      INSERT INTO my_table (id,lob_field) values (1, 'some clob data');
 *
 * Oracle LogMiner provides the connector with the following events:
 *      INSERT INTO my_table (id,lob_field) values (1, EMPTY_CLOB());
 *      UPDATE my_table SET lob_field = 'some clob data' where id = 1;
 *
 * When LOB support is enabled, this consumer implementation will detect that the update is an
 * event that should be merged with the previous insert event so that the emitted change events
 * consists a single logical change, an insert that have an after section like:
 *
 * <pre>
 *     "after": {
 *         "id": 1,
 *         "lob_field": "some clob data"
 *     }
 * </pre>
 *
 * When LOB support isn't enabled, events are simply passed through to the delegate and no event
 * inspection, merging, or buffering occurs.
 *
 * @author Chris Cranford
 */
public class TransactionCommitConsumer implements AutoCloseable, BlockingConsumer<LogMinerEvent> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionCommitConsumer.class);

    private final BlockingConsumer<LogMinerEvent> delegate;
    private final OracleConnectorConfig connectorConfig;
    private final OracleDatabaseSchema schema;
    private final List<String> lobWriteData;

    /**
     * Describes the current LOB event buffering state, whether we're working on a series of
     * {@code LOB_WRITE} events, {@code LOB_ERASE} events, or any other type of event that
     * does not require special handling.
     */
    enum LobState {
        WRITE,
        ERASE,
        OTHER
    };

    private LogMinerEvent lastEvent;
    private SelectLobLocatorEvent lastSelectLobLocatorEvent;
    private LobState lobState;

    public TransactionCommitConsumer(BlockingConsumer<LogMinerEvent> delegate, OracleConnectorConfig connectorConfig, OracleDatabaseSchema schema) {
        this.delegate = delegate;
        this.lobState = LobState.OTHER;
        this.lobWriteData = new ArrayList<>();
        this.connectorConfig = connectorConfig;
        this.schema = schema;
    }

    @Override
    public void close() throws InterruptedException {
        if (lastEvent != null) {
            if (!lobWriteData.isEmpty()) {
                mergeLobWriteData(lastEvent);
            }
            dispatchChangeEvent(lastEvent);
        }
    }

    @Override
    public void accept(LogMinerEvent event) throws InterruptedException {
        if (!connectorConfig.isLobEnabled()) {
            // LOB support is not enabled, perform immediate dispatch
            dispatchChangeEvent(event);
            return;
        }

        if (lastEvent == null) {
            // Always cache first event, follow-up events will dictate merge/dispatch status
            this.lastEvent = event;
            if (EventType.SELECT_LOB_LOCATOR == event.getEventType()) {
                this.lastSelectLobLocatorEvent = (SelectLobLocatorEvent) event;
            }
        }
        else {

            // Check whether the LOB data queue needs to be drained to the last event
            LobState currentLobState = resolveLobStateByCurrentEvent(event);
            if (currentLobState != this.lobState) {
                if (this.lobState == LobState.WRITE) {
                    mergeLobWriteData(lastEvent);
                }
                this.lobState = currentLobState;
            }

            if (!isMerged(event, lastEvent)) {
                LOGGER.trace("\tMerged skipped.");
                // Events were not merged, dispatch last one and cache new
                dispatchChangeEvent(lastEvent);
                this.lastEvent = event;
            }
            else {
                LOGGER.trace("\tMerged successfully.");
            }
        }
    }

    private void dispatchChangeEvent(LogMinerEvent event) throws InterruptedException {
        LOGGER.trace("\tEmitting event {} {}", event.getEventType(), event);
        delegate.accept(event);
    }

    private LobState resolveLobStateByCurrentEvent(LogMinerEvent event) {
        switch (event.getEventType()) {
            case LOB_WRITE:
                return LobState.WRITE;
            case LOB_ERASE:
                return LobState.ERASE;
            default:
                return LobState.OTHER;
        }
    }

    private boolean isMerged(LogMinerEvent event, LogMinerEvent prevEvent) {
        LOGGER.trace("\tVerifying merge eligibility for event {} with {}", event.getEventType(), prevEvent.getEventType());
        if (EventType.SELECT_LOB_LOCATOR == event.getEventType()) {
            SelectLobLocatorEvent locatorEvent = (SelectLobLocatorEvent) event;
            this.lastSelectLobLocatorEvent = locatorEvent;
            if (EventType.INSERT == prevEvent.getEventType()) {
                // Previous event is an INSERT
                // Only merge the SEL_LOB_LOCATOR event if the previous INSERT is for the same table/row
                // and if the INSERT's column value is either EMPTY_CLOB() or EMPTY_BLOB()
                if (isForSameTableOrScn(event, prevEvent)) {
                    LOGGER.trace("\tMerging SEL_LOB_LOCATOR with previous INSERT event");
                    return true;
                }
            }
            else if (EventType.UPDATE == prevEvent.getEventType()) {
                if (isForSameTableOrScn(event, prevEvent) && isSameTableRow(event, prevEvent)) {
                    LOGGER.trace("\tUpdating SEL_LOB_LOCATOR column '{}' to previous UPDATE event", locatorEvent.getColumnName());
                    return true;
                }
            }
            else if (EventType.SELECT_LOB_LOCATOR == prevEvent.getEventType()) {
                if (isForSameTableOrScn(event, prevEvent) && isSameTableRow(event, prevEvent)) {
                    LOGGER.trace("\tAdding column '{}' to previous SEL_LOB_LOCATOR event", locatorEvent.getColumnName());
                    return true;
                }
            }
        }
        else if (EventType.LOB_WRITE == event.getEventType()) {
            final LobWriteEvent writeEvent = (LobWriteEvent) event;
            if (lastSelectLobLocatorEvent.isBinary()) {
                if (!writeEvent.getData().startsWith("HEXTORAW('") && !writeEvent.getData().endsWith("')")) {
                    throw new DebeziumException("Unexpected LOB data chunk: " + writeEvent.getData());
                }
            }
            LOGGER.trace("\tAdded LOB_WRITE data to internal LOB queue.");
            lobWriteData.add(writeEvent.getData());
            return true;
        }
        else if (EventType.LOB_ERASE == event.getEventType()) {
            // nothing is done with the event, its just consumed and treated as merged.
            LOGGER.warn("\tLOB_ERASE for table '{}' column '{}' is not supported.",
                    lastSelectLobLocatorEvent.getTableId(), lastSelectLobLocatorEvent.getColumnName());
            LOGGER.trace("\tSkipped LOB_ERASE, treated as merged.");
            return true;
        }
        else if (EventType.LOB_TRIM == event.getEventType()) {
            // nothing is done with the event, its just consumed and treated as merged.
            LOGGER.trace("\tSkipped LOB_TRIM, treated as merged.");
            return true;
        }
        else if (EventType.INSERT == event.getEventType() || EventType.UPDATE == event.getEventType()) {
            // Previous event is an INSERT
            // The only valid combination here would be if the current event is an UPDATE since an INSERT
            // cannot be merged with a prior INSERT.
            if (EventType.INSERT == prevEvent.getEventType()) {
                if (EventType.UPDATE == event.getEventType()) {
                    if (isForSameTableOrScn(event, prevEvent) && isSameTableRow(event, prevEvent)) {
                        LOGGER.trace("\tMerging UPDATE event with previous INSERT event");
                        mergeNewColumns((DmlEvent) event, (DmlEvent) prevEvent);
                        return true;
                    }
                }
            }
            else if (EventType.UPDATE == prevEvent.getEventType()) {
                // Previous event is an UPDATE
                // This will happen if there are non LOB and inline LOB fields updated in the same SQL.
                // The inline LOB values should be merged with the previous UPDATE event
                if (EventType.UPDATE == event.getEventType()) {
                    if (isForSameTableOrScn(event, prevEvent) && isSameTableRow(event, prevEvent)) {
                        LOGGER.trace("\tMerging UPDATE event with previous UPDATE event");
                        mergeNewColumns((DmlEvent) event, (DmlEvent) prevEvent);
                        return true;
                    }
                }
            }
            else if (EventType.SELECT_LOB_LOCATOR == prevEvent.getEventType()) {
                // Previous event is a SEL_LOB_LOCATOR
                // SQL contains both non-inline LOB and inline-LOB field changes
                if (EventType.UPDATE == event.getEventType()) {
                    if (isForSameTableOrScn(event, prevEvent) && isSameTableRow(event, prevEvent)) {
                        for (int i = 0; i < ((DmlEvent) event).getDmlEntry().getNewValues().length; ++i) {
                            Object value = ((DmlEvent) event).getDmlEntry().getNewValues()[i];
                            Object prevValue = ((DmlEvent) prevEvent).getDmlEntry().getNewValues()[i];
                            if (prevValue == null && value != null) {
                                LOGGER.trace("\tAdding column index {} to previous SEL_LOB_LOCATOR event", i);
                                ((DmlEvent) prevEvent).getDmlEntry().getNewValues()[i] = value;
                            }
                        }
                        return true;
                    }
                }
            }
        }

        LOGGER.trace("\tEvent {} is for a different row, merge skipped.", event.getEventType());
        return false;
    }

    private void mergeLobWriteData(LogMinerEvent event) {
        final Object data;
        if (this.lastSelectLobLocatorEvent.isBinary()) {
            // For BLOB we pass the list of chunks as-is to the value converter
            data = new BlobChunkList(lobWriteData);
        }
        else {
            // For CLOB we go ahead and pre-process the list into a single string
            data = String.join("", lobWriteData);
        }

        final DmlEvent dmlEvent = (DmlEvent) event;
        final String columnName = lastSelectLobLocatorEvent.getColumnName();
        final int columnIndex = getSelectLobLocatorColumnIndex();

        LOGGER.trace("\tSet LOB data for column '{}' on table {} in event {}", columnName, event.getTableId(), event.getEventType());
        dmlEvent.getDmlEntry().getNewValues()[columnIndex] = data;
        lobWriteData.clear();
    }

    private int getSelectLobLocatorColumnIndex() {
        final String columnName = lastSelectLobLocatorEvent.getColumnName();
        return LogMinerHelper.getColumnIndexByName(columnName, schema.tableFor(lastSelectLobLocatorEvent.getTableId()));
    }

    private boolean isForSameTableOrScn(LogMinerEvent event, LogMinerEvent prevEvent) {
        if (prevEvent != null) {
            if (event.getTableId().equals(prevEvent.getTableId())) {
                return true;
            }
            return event.getScn().equals(prevEvent.getScn()) && event.getRsId().equals(prevEvent.getRsId());
        }
        return false;
    }

    private boolean isSameTableRow(LogMinerEvent event, LogMinerEvent prevEvent) {
        final Table table = schema.tableFor(event.getTableId());
        if (table == null) {
            LOGGER.trace("Unable to locate table '{}' schema, unable to detect if same row.", event.getTableId());
            return false;
        }
        for (String columnName : table.primaryKeyColumnNames()) {
            int position = LogMinerHelper.getColumnIndexByName(columnName, table);
            Object prevValue = ((DmlEvent) prevEvent).getDmlEntry().getNewValues()[position];
            if (prevValue == null) {
                throw new DebeziumException("Could not find column " + columnName + " in previous event");
            }
            Object value = ((DmlEvent) event).getDmlEntry().getNewValues()[position];
            if (value == null) {
                throw new DebeziumException("Could not find column " + columnName + " in event");
            }
            if (!Objects.equals(value, prevValue)) {
                return false;
            }
        }
        return true;
    }

    private void mergeNewColumns(DmlEvent event, DmlEvent prevEvent) {
        final boolean prevEventIsInsert = EventType.INSERT == prevEvent.getEventType();
        for (int i = 0; i < event.getDmlEntry().getNewValues().length; ++i) {
            Object value = event.getDmlEntry().getNewValues()[i];
            Object prevValue = prevEvent.getDmlEntry().getNewValues()[i];
            if (prevEventIsInsert && "EMPTY_CLOB()".equals(prevValue)) {
                LOGGER.trace("\tAssigning column index {} with updated CLOB value.", i);
                prevEvent.getDmlEntry().getNewValues()[i] = value;
            }
            else if (prevEventIsInsert && "EMPTY_BLOB()".equals(prevValue)) {
                LOGGER.trace("\tAssigning column index {} with updated BLOB value.", i);
                prevEvent.getDmlEntry().getNewValues()[i] = value;
            }
            else if (!prevEventIsInsert && OracleValueConverters.UNAVAILABLE_VALUE.equals(value)) {
                LOGGER.trace("\tSkipped column index {} with unavailable column value.", i);
            }
            else if (!prevEventIsInsert && value != null) {
                LOGGER.trace("\tUpdating column index {} in previous event", i);
                prevEvent.getDmlEntry().getNewValues()[i] = value;
            }
        }
    }
}
