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
import io.debezium.connector.oracle.logminer.LogMinerHelper;
import io.debezium.connector.oracle.logminer.events.DmlEvent;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.LobEraseEvent;
import io.debezium.connector.oracle.logminer.events.LobWriteEvent;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;
import io.debezium.connector.oracle.logminer.events.SelectLobLocatorEvent;
import io.debezium.connector.oracle.logminer.events.Transaction;
import io.debezium.relational.Table;

/**
 * Helper class that performs common transaction reconciliation.
 *
 * Transactions read from Oracle LogMiner are subject to containing events that need to be merged
 * together to reflect a single logical SQL operation, such as events that pertain to LOB fields.
 * This class facilities all the steps needed to merge events and reconcile a transaction.
 *
 * @author Chris Cranford
 */
public class TransactionReconciliation {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionReconciliation.class);

    private final OracleConnectorConfig connectorConfig;
    private final OracleDatabaseSchema schema;

    public TransactionReconciliation(OracleConnectorConfig connectorConfig, OracleDatabaseSchema schema) {
        this.connectorConfig = connectorConfig;
        this.schema = schema;
    }

    /**
     * Reconcile the specified transaction by merging multiple events that should be emitted as a single
     * logical event, such as changes made to LOB column types that involve multiple events.
     *
     * @param transaction transaction to be reconciled, never {@code null}
     */
    public void reconcile(Transaction transaction) {
        // Do not perform reconciliation if LOB support is not enabled.
        if (!connectorConfig.isLobEnabled()) {
            return;
        }

        final String transactionId = transaction.getTransactionId();
        LOGGER.trace("Reconciling transaction {}", transactionId);

        DmlEvent prevEvent = null;
        int prevEventSize = transaction.getEvents().size();
        for (int i = 0; i < transaction.getEvents().size();) {

            final LogMinerEvent event = transaction.getEvents().get(i);
            LOGGER.trace("Processing event {}", event);

            switch (event.getEventType()) {
                case SELECT_LOB_LOCATOR:
                    if (shouldMergeSelectLobLocatorEvent(transaction, i, (SelectLobLocatorEvent) event, prevEvent)) {
                        continue;
                    }
                    break;
                case INSERT:
                case UPDATE:
                    if (shouldMergeDmlEvent(transaction, i, (DmlEvent) event, prevEvent)) {
                        continue;
                    }
                    break;
            }

            ++i;
            prevEvent = (DmlEvent) event;
            LOGGER.trace("Previous event is now {}", prevEvent);
        }

        int eventSize = transaction.getEvents().size();
        if (eventSize != prevEventSize) {
            LOGGER.trace("Reconciled transaction {} from {} events to {}.", transactionId, prevEventSize, eventSize);
        }
        else {
            LOGGER.trace("Transaction {} event queue was unmodified.", transactionId);
        }
    }

    /**
     * Attempts to merge the provided SEL_LOB_LOCATOR event with the previous event in the transaction.
     *
     * @param transaction transaction being processed, never {@code null}
     * @param index event index being processed
     * @param event event being processed, never {@code null}
     * @param prevEvent previous event in the transaction, can be {@code null}
     * @return true if the event is merged, false if the event was not merged.
     */
    protected boolean shouldMergeSelectLobLocatorEvent(Transaction transaction, int index, SelectLobLocatorEvent event, DmlEvent prevEvent) {
        LOGGER.trace("\tDetected SelectLobLocatorEvent for column '{}'", event.getColumnName());

        final int columnIndex = LogMinerHelper.getColumnIndexByName(event.getColumnName(), schema.tableFor(event.getTableId()));

        // Read and combine all LOB_WRITE events that follow SEL_LOB_LOCATOR
        Object lobData = null;
        final List<String> lobWrites = readAndCombineLobWriteEvents(transaction, index, event.isBinary());
        if (!lobWrites.isEmpty()) {
            if (event.isBinary()) {
                // For BLOB we pass the list of string chunks as-is to the value converter
                lobData = new BlobChunkList(lobWrites);
            }
            else {
                // For CLOB we go ahead and pre-process the List into a single string.
                lobData = String.join("", lobWrites);
            }
        }

        // Read and consume all LOB_ERASE events that follow SEL_LOB_LOCATOR
        final int lobEraseEvents = readAndConsumeLobEraseEvents(transaction, index);
        if (lobEraseEvents > 0) {
            LOGGER.warn("LOB_ERASE for table '{}' column '{}' is not supported, use DML operations to manipulate LOB columns only.", event.getTableId(),
                    event.getColumnName());
            if (lobWrites.isEmpty()) {
                // There are no write and only erase events, discard entire SEL_LOB_LOCATOR
                // To simulate this, we treat this as a "merge" op so caller doesn't modify previous event
                transaction.getEvents().remove(index);
                return true;
            }
        }
        else if (lobEraseEvents == 0 && lobWrites.isEmpty()) {
            // There were no LOB operations present, discard entire SEL_LOB_LOCATOR
            // To simulate this, we treat this as a "merge" op so caller doesn't modify previous event
            transaction.getEvents().remove(index);
            return true;
        }

        // SelectLobLocatorEvent can be treated as a parent DML operation where an update occurs on any
        // LOB-based column. In this case, the event will be treated as an UPDATE event when emitted.

        if (prevEvent == null) {
            // There is no prior event, add column to this SelectLobLocatorEvent and don't merge.
            LOGGER.trace("\tAdding column '{}' to current event", event.getColumnName());
            event.getDmlEntry().getNewValues()[columnIndex] = lobData;
            return false;
        }

        if (EventType.INSERT == prevEvent.getEventType()) {
            // Previous event is an INSERT operation.
            // Only merge the SEL_LOB_LOCATOR event if the previous INSERT is for the same table/row
            // and if the INSERT's column value is EMPTY_CLOB() or EMPTY_BLOB()
            if (isForSameTableOrScn(event, prevEvent)) {
                LOGGER.trace("\tMerging SEL_LOB_LOCATOR with previous INSERT event");
                Object prevValue = prevEvent.getDmlEntry().getNewValues()[columnIndex];
                if (!"EMPTY_CLOB()".equals(prevValue) && !"EMPTY_BLOB()".equals(prevValue)) {
                    throw new DebeziumException("Expected to find column '" + event.getColumnName() + "' in table '"
                            + prevEvent.getTableId() + "' to be initialized as an empty LOB value.'");
                }

                prevEvent.getDmlEntry().getNewValues()[columnIndex] = lobData;

                // Remove the SEL_LOB_LOCATOR event from event list and indicate merged.
                transaction.getEvents().remove(index);
                return true;
            }
        }
        else if (EventType.UPDATE == prevEvent.getEventType()) {
            // Previous event is an UPDATE operation.
            // Only merge the SEL_LOB_LOCATOR event if the previous UPDATE is for the same table/row
            if (isForSameTableOrScn(event, prevEvent) && isSameTableRow(event, prevEvent)) {
                LOGGER.trace("\tUpdating SEL_LOB_LOCATOR column '{}' to previous UPDATE event", event.getColumnName());
                prevEvent.getDmlEntry().getNewValues()[columnIndex] = lobData;

                // Remove the SEL_LOB_LOCATOR event from event list and indicate merged.
                transaction.getEvents().remove(index);
                return true;
            }
        }
        else if (EventType.SELECT_LOB_LOCATOR == prevEvent.getEventType()) {
            // Previous event is a SEL_LOB_LOCATOR operation.
            // Only merge the two SEL_LOB_LOCATOR events if they're for the same table/row
            if (isForSameTableOrScn(event, prevEvent) && isSameTableRow(event, prevEvent)) {
                LOGGER.trace("\tAdding column '{}' to previous SEL_LOB_LOCATOR event", event.getColumnName());
                prevEvent.getDmlEntry().getNewValues()[columnIndex] = lobData;

                // Remove the SEL_LOB_LOCATOR event from event list and indicate merged.
                transaction.getEvents().remove(index);
                return true;
            }
        }
        else {
            throw new DebeziumException("Unexpected previous event operation: " + prevEvent.getEventType());
        }

        LOGGER.trace("\tSEL_LOB_LOCATOR event is for different row, merge skipped.");
        LOGGER.trace("\tAdding column '{}' to current event", event.getColumnName());
        event.getDmlEntry().getNewValues()[columnIndex] = lobData;
        return false;
    }

    /**
     * Attempts to merge the provided DML event with the previous event in the transaction.
     *
     * @param transaction transaction being processed, never {@code null}
     * @param index event index being processed
     * @param event event being processed, never {@code null}
     * @param prevEvent previous event in the transaction, can be {@code null}
     * @return true if the event is merged, false if the event was not merged
     */
    protected boolean shouldMergeDmlEvent(Transaction transaction, int index, DmlEvent event, DmlEvent prevEvent) {
        LOGGER.trace("\tDetected DmlEvent {}", event.getEventType());

        if (prevEvent == null) {
            // There is no prior event, therefore there is no reason to perform any merge.
            return false;
        }

        if (EventType.INSERT == prevEvent.getEventType()) {
            // Previous event is an INSERT operation.
            // The only valid combination here would be if the current event is an UPDATE since an INSERT cannot
            // be merged with a prior INSERT with how LogMiner materializes the rows.
            if (EventType.UPDATE == event.getEventType()) {
                if (isForSameTableOrScn(event, prevEvent) && isSameTableRow(event, prevEvent)) {
                    LOGGER.trace("\tMerging UPDATE event with previous INSERT event");
                    mergeNewColumns(event, prevEvent);

                    // Remove the UPDATE event from event list and indicate merged.
                    transaction.getEvents().remove(index);
                    return true;
                }
            }
        }
        else if (EventType.UPDATE == prevEvent.getEventType()) {
            // Previous event is an UPDATE operation.
            // This will happen if there are non-CLOB and inline-CLOB fields updated in the same SQL.
            // The inline-CLOB values should be merged with the previous UPDATE event.
            if (EventType.UPDATE == event.getEventType()) {
                if (isForSameTableOrScn(event, prevEvent) && isSameTableRow(event, prevEvent)) {
                    LOGGER.trace("\tMerging UPDATE event with previous UPDATE event");
                    mergeNewColumns(event, prevEvent);

                    // Remove the UPDATE event from event list and indicate merged.
                    transaction.getEvents().remove(index);
                    return true;
                }
            }
        }
        else if (EventType.SELECT_LOB_LOCATOR == prevEvent.getEventType()) {
            // Previous event is a SEL_LOB_LOCATOR operation.
            // SQL contained both non-inline CLOB and inline-CLOB field changes.
            if (EventType.UPDATE == event.getEventType()) {
                if (isForSameTableOrScn(event, prevEvent) && isSameTableRow(event, prevEvent)) {
                    LOGGER.trace("\tMerging UPDATE event with previous SEL_LOB_LOCATOR event");
                    for (int i = 0; i < event.getDmlEntry().getNewValues().length; ++i) {
                        Object value = event.getDmlEntry().getNewValues()[i];
                        Object prevValue = prevEvent.getDmlEntry().getNewValues()[i];
                        if (prevValue == null && value != null) {
                            LOGGER.trace("\tAdding column index {} to previous SEL_LOB_LOCATOR event", i);
                            prevEvent.getDmlEntry().getNewValues()[i] = value;
                        }
                    }

                    // Remove the UPDATE event from event list and indicate merged.
                    transaction.getEvents().remove(index);
                    return true;
                }
            }
        }

        LOGGER.trace("\tDmlEvent {} event is for different row, merge skipped.", event.getEventType());
        return false;
    }

    /**
     * Reads the transaction event queue and combines all LOB_WRITE events starting at the provided index.
     * for a SEL_LOB_LOCATOR event which is for binary data (BLOB) data types.
     *
     * @param transaction transaction being processed, never {@code null}
     * @param index index to the first LOB_WRITE operation
     * @return list of string-based values for each LOB_WRITE operation
     */
    protected List<String> readAndCombineLobWriteEvents(Transaction transaction, int index, boolean binaryData) {
        List<String> chunks = new ArrayList<>();
        for (int i = index + 1; i < transaction.getEvents().size(); ++i) {
            final LogMinerEvent event = transaction.getEvents().get(i);
            if (!(event instanceof LobWriteEvent)) {
                break;
            }

            final LobWriteEvent writeEvent = (LobWriteEvent) event;
            if (binaryData && !writeEvent.getData().startsWith("HEXTORAW('") && !writeEvent.getData().endsWith("')")) {
                throw new DebeziumException("Unexpected BLOB data chunk: " + writeEvent.getData());
            }

            chunks.add(writeEvent.getData());
        }

        if (!chunks.isEmpty()) {
            LOGGER.trace("\tCombined {} LobWriteEvent events", chunks.size());
            // Remove events from the transaction queue queue
            for (int i = 0; i < chunks.size(); ++i) {
                transaction.getEvents().remove(index + 1);
            }
        }

        return chunks;
    }

    /**
     * Read and remove all LobErase events detected in the transaction event queue.
     *
     * @param transaction transaction being processed, never {@code null}
     * @param index index to the first LOB_ERASE operation
     * @return number of LOB_ERASE events consumed and removed from the event queue
     */
    protected int readAndConsumeLobEraseEvents(Transaction transaction, int index) {
        int events = 0;
        for (int i = index + 1; i < transaction.getEvents().size(); ++i) {
            final LogMinerEvent event = transaction.getEvents().get(i);
            if (!(event instanceof LobEraseEvent)) {
                break;
            }
            events++;
        }

        if (events > 0) {
            LOGGER.trace("\tConsumed {} LobErase events", events);
            for (int i = 0; i < events; ++i) {
                transaction.getEvents().remove(index + 1);
            }
        }

        return events;
    }

    /**
     * Checks whether the two events are for the same table or participate in the same system change.
     *
     * @param event current event being processed, never {@code null}
     * @param prevEvent previous/parent event that has been processed, may be {@code null}
     * @return true if the two events are for the same table or system change number, false otherwise
     */
    protected boolean isForSameTableOrScn(LogMinerEvent event, LogMinerEvent prevEvent) {
        if (prevEvent != null) {
            if (event.getTableId().equals(prevEvent.getTableId())) {
                return true;
            }
            return event.getScn().equals(prevEvent.getScn()) && event.getRsId().equals(prevEvent.getRsId());
        }
        return false;
    }

    /**
     * Checks whether the two events are for the same table row.
     *
     * @param event current event being processed, never {@code null}
     * @param prevEvent previous/parent event that has been processed, never {@code null}
     * @return true if the two events are for the same table row, false otherwise
     */
    protected boolean isSameTableRow(DmlEvent event, DmlEvent prevEvent) {
        final Table table = schema.tableFor(event.getTableId());
        if (table == null) {
            LOGGER.trace("Unable to locate table '{}' schema, unable to detect if same row.", event.getTableId());
            return false;
        }
        for (String columnName : table.primaryKeyColumnNames()) {
            int position = LogMinerHelper.getColumnIndexByName(columnName, table);
            Object prevValue = prevEvent.getDmlEntry().getNewValues()[position];
            if (prevValue == null) {
                throw new DebeziumException("Could not find column " + columnName + " in previous event");
            }
            Object value = event.getDmlEntry().getNewValues()[position];
            if (value == null) {
                throw new DebeziumException("Could not find column " + columnName + " in event");
            }
            if (!Objects.equals(value, prevValue)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Merge column values from {@code event} with {@code prevEvent}.
     *
     * @param event current event being processed, never {@code null}
     * @param prevEvent previous/parent parent that has been processed, never {@code null}
     */
    protected void mergeNewColumns(DmlEvent event, DmlEvent prevEvent) {
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
            else if (!prevEventIsInsert && value != null) {
                LOGGER.trace("\tUpdating column index {} in previous event", i);
                prevEvent.getDmlEntry().getNewValues()[i] = value;
            }
        }
    }

}
