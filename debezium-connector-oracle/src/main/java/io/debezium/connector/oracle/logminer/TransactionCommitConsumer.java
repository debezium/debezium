/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleValueConverters;
import io.debezium.connector.oracle.logminer.events.DmlEvent;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.ExtendedStringBeginEvent;
import io.debezium.connector.oracle.logminer.events.ExtendedStringWriteEvent;
import io.debezium.connector.oracle.logminer.events.LobEraseEvent;
import io.debezium.connector.oracle.logminer.events.LobWriteEvent;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;
import io.debezium.connector.oracle.logminer.events.SelectLobLocatorEvent;
import io.debezium.connector.oracle.logminer.events.TruncateEvent;
import io.debezium.connector.oracle.logminer.events.XmlBeginEvent;
import io.debezium.connector.oracle.logminer.events.XmlEndEvent;
import io.debezium.connector.oracle.logminer.events.XmlWriteEvent;
import io.debezium.function.BlockingConsumer;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.util.Strings;

import oracle.sql.RAW;

/**
 * A consumer of transaction events at commit time that is capable of inspecting the event stream,
 * merging events that should be merged when LOB support is enabled, and then delegating the final
 * stream of events to a delegate consumer.
 *
 * When a table has a LOB or XML field, Oracle LogMiner often supplies us with synthetic events that deal
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
    private static final String NULL_COLUMN = "__debezium_null";
    private static final String BLOB_TYPE = "BLOB";
    private static final String CLOB_TYPE = "CLOB";

    private final Handler<LogMinerEvent> delegate;
    private final OracleConnectorConfig connectorConfig;
    private final OracleDatabaseSchema schema;
    private final Map<String, RowState> rows = new HashMap<>();
    private final ConstructionDetails currentLobDetails = new ConstructionDetails();
    private final ConstructionDetails currentExtendedStringDetails = new ConstructionDetails();
    private final ConstructionDetails currentXmlDetails = new ConstructionDetails();

    private long dispatchEventIndex = 0;
    private long enqueueEventIndex = 0;
    private int totalEvents = 0;

    public TransactionCommitConsumer(Handler<LogMinerEvent> delegate, OracleConnectorConfig connectorConfig, OracleDatabaseSchema schema) {
        this.delegate = delegate;
        this.connectorConfig = connectorConfig;
        this.schema = schema;
    }

    @Override
    public void close() throws InterruptedException {
        // Dispatch any of the existing events in the order they were received
        List<RowState> pending = new ArrayList<>(rows.values());
        pending.sort(Comparator.comparingLong(x -> x.transactionIndex));

        for (final RowState rowState : pending) {
            prepareAndDispatch(rowState.event);
        }

        // For situations where the consumer instance is reused, reset internal state
        rows.clear();
        currentLobDetails.reset();
        currentExtendedStringDetails.reset();
        currentXmlDetails.reset();

        totalEvents = 0;
        enqueueEventIndex = 0;
        dispatchEventIndex = 0;
    }

    @Override
    public void accept(LogMinerEvent event) throws InterruptedException {
        // track number of events passed
        totalEvents++;

        if (!connectorConfig.isLobEnabled()) {
            // LOB support is not enabled, perform immediate dispatch
            dispatchChangeEvent(event);
            return;
        }

        if (event instanceof DmlEvent) {
            acceptDmlEvent((DmlEvent) event);
        }
        else {
            acceptManipulationEvent(event);
        }
    }

    public int getTotalEvents() {
        return totalEvents;
    }

    private void acceptDmlEvent(DmlEvent event) throws InterruptedException {
        enqueueEventIndex++;

        final Table table = schema.tableFor(event.getTableId());
        if (table == null) {
            LOGGER.debug("Unable to locate table '{}' schema, ignoring event.", event.getTableId());
            return;
        }

        String rowId = rowIdFromEvent(table, event);
        RowState rowState = rows.get(rowId);
        DmlEvent accumulatorEvent = (null == rowState) ? null : rowState.event;

        // DBZ-6963
        // This short-circuits the commit consumer's accumulation logic by assessing whether the
        // table has any LOB columns (clob, blob, or xml). If the table does not, then there is
        // no need to perform any of these steps as it should never be eligible for merging.
        final List<Column> lobColumns = schema.getLobColumnsForTable(table.id());
        if (lobColumns.isEmpty()) {
            // There should never be a use case where the accumulator event is not null in this code
            // path because given that the table has no LOB columns, it won't ever be added to the
            // queue with the logic below. Therefore, there is no need to attempt to dispatch the
            // accumulator as it should be null.
            LOGGER.debug("\tEvent for table {} has no LOB columns, dispatching.", table.id());
            dispatchChangeEvent(event);
            return;
        }

        if (!tryMerge(accumulatorEvent, event)) {
            prepareAndDispatch(accumulatorEvent);
            if (rowId.equals(currentLobDetails.rowId)) {
                currentLobDetails.reset();
            }
            else if (rowId.equals(currentExtendedStringDetails.rowId)) {
                currentExtendedStringDetails.reset();
            }
            else if (rowId.equals(currentXmlDetails.rowId)) {
                currentXmlDetails.reset();
            }
            rows.put(rowId, new RowState(event, enqueueEventIndex));
            accumulatorEvent = event;
        }

        if (EventType.SELECT_LOB_LOCATOR == event.getEventType()) {
            final String columnName = ((SelectLobLocatorEvent) event).getColumnName();
            initConstructable(currentLobDetails, rowId, columnName, table, accumulatorEvent, LobUnderConstruction::fromInitialValue);
        }
        else if (EventType.EXTENDED_STRING_BEGIN == event.getEventType()) {
            final String columnName = ((ExtendedStringBeginEvent) event).getColumnName();
            initConstructable(currentExtendedStringDetails, rowId, columnName, table, accumulatorEvent, ExtendedStringUnderConstruction::fromInitialValue);
        }
        else if (EventType.XML_BEGIN == event.getEventType()) {
            final String columnName = ((XmlBeginEvent) event).getColumnName();
            initConstructable(currentXmlDetails, rowId, columnName, table, accumulatorEvent, XmlUnderConstruction::fromInitialValue);
        }
    }

    private void acceptManipulationEvent(LogMinerEvent event) {
        if (event instanceof LobWriteEvent || event instanceof LobEraseEvent) {
            acceptLobManipulationEvent(event);
        }
        else if (event instanceof ExtendedStringWriteEvent) {
            acceptExtendedStringManipulationEvent(event);
        }
        else if (event instanceof XmlWriteEvent || event instanceof XmlEndEvent) {
            acceptXmlManipulationEvent(event);
        }
    }

    private void acceptLobManipulationEvent(LogMinerEvent event) {
        if (!currentLobDetails.isInitialized()) {
            // should only happen when we start streaming in the middle of a LOB transaction (DBZ-4367)
            LOGGER.debug("Got LOB manipulation event without preceding LOB selector; ignoring {} {}.", event.getEventType(), event);
            return;
        }

        if (EventType.LOB_WRITE != event.getEventType()) {
            LOGGER.warn("\t{} for table '{}' column '{}' is not supported.", event.getEventType(), event.getTableId(), currentLobDetails.columnName);
            LOGGER.trace("All LOB manipulation events apart from LOB_WRITE are currently ignored; ignoring {} {}.", event.getEventType(), event);
            discardCurrentMergeState(currentLobDetails);
            return;
        }

        final LobUnderConstruction lob = (LobUnderConstruction) getConstructable(currentLobDetails);
        try {
            lob.add(new LobFragment(event));
        }
        catch (final DebeziumException exception) {
            LOGGER.warn("\tInvalid LOB manipulation event: {} ; ignoring {} {}", exception, event.getEventType(), event);
        }
    }

    private void acceptExtendedStringManipulationEvent(LogMinerEvent event) {
        if (!currentExtendedStringDetails.isInitialized()) {
            LOGGER.debug("Got ExtendedString manipulation event without preceding ExtendedString begin; ignoring {} {}.", event.getEventType(), event);
            return;
        }

        if (EventType.EXTENDED_STRING_WRITE != event.getEventType()) {
            LOGGER.warn("\t{} for table '{}' column '{}' is not supported.", event.getEventType(), event.getTableId(), currentExtendedStringDetails.columnName);
            LOGGER.trace("All ExtendedString manipulation events apart from 32K_WRITE are currently ignored; ignoring {} {}.", event.getEventType(), event);
            discardCurrentMergeState(currentExtendedStringDetails);
            return;
        }

        final ExtendedStringUnderConstruction lob = (ExtendedStringUnderConstruction) getConstructable(currentExtendedStringDetails);
        try {
            lob.add(new ExtendedStringFragment((ExtendedStringWriteEvent) event));
        }
        catch (final DebeziumException exception) {
            LOGGER.warn("\tInvalid ExtendedString manipulation event: {} ; ignoring {} {}", exception, event.getEventType(), event);
        }
    }

    private void acceptXmlManipulationEvent(LogMinerEvent event) {
        if (!currentXmlDetails.isInitialized()) {
            // should only happen when we start streaming in the middle of an XML transaction (DBZ-4367)
            LOGGER.trace("Got XML manipulation event without preceding XML begin; ignoring {} {}.", event.getEventType(), event);
            return;
        }

        if (EventType.XML_WRITE != event.getEventType() && EventType.XML_END != event.getEventType()) {
            LOGGER.warn("\t{} for table '{}' column '{}' is not supported.", event.getEventType(), event.getTableId(), currentXmlDetails.columnName);
            LOGGER.trace("All LOB manipulation events apart from XML_WRITE are currently ignored; ignoring {} {}.", event.getEventType(), event);
            discardCurrentMergeState(currentXmlDetails);
            return;
        }
        else if (EventType.XML_END == event.getEventType()) {
            // silently ignore it
            return;
        }

        final XmlUnderConstruction xml = (XmlUnderConstruction) getConstructable(currentXmlDetails);
        try {
            final XmlWriteEvent writeEvent = (XmlWriteEvent) event;
            if (!Objects.isNull(writeEvent.getXml())) {
                xml.add(new XmlFragment(writeEvent));
            }
        }
        catch (DebeziumException exception) {
            LOGGER.warn("\tInvalid XML manipulation event: {} ; ignoring {} {}", exception, event.getEventType(), event);
        }
    }

    private Object getConstructable(ConstructionDetails details) {
        return newValues(rows.get(details.rowId).event)[details.columnPosition];
    }

    private void initConstructable(ConstructionDetails details, String rowId, String columnName, Table table,
                                   DmlEvent accumulatorEvent, Function<Object, Object> constructor) {
        details.rowId = rowId;
        details.columnName = columnName;
        details.columnPosition = LogMinerHelper.getColumnIndexByName(columnName, table);

        Object[] values = newValues(accumulatorEvent);
        Object prevValue = values[details.columnPosition];
        values[details.columnPosition] = constructor.apply(prevValue);
    }

    private void prepareAndDispatch(DmlEvent event) throws InterruptedException {
        if (null == event) { // we just added the first event for this row
            return;
        }
        Object[] values = newValues(event);
        for (int i = 0; i < values.length; i++) {
            if (values[i] instanceof AbstractUnderConstruction) {
                values[i] = ((AbstractUnderConstruction<?>) values[i]).merge();
            }
        }
        // don't emit change events for ignored LOB manipulations (i.e. event is SEL_LOB_LOCATOR
        // and oldValues is equal to newValues)
        if (EventType.SELECT_LOB_LOCATOR == event.getEventType()) {
            boolean noop = true;
            Object[] oldValues = oldValues(event);
            for (int i = 0; i < values.length; i++) {
                if (!Objects.equals(oldValues[i], values[i])) {
                    noop = false;
                    break;
                }
            }
            if (noop) {
                LOGGER.trace("\tSkip emitting event {} {} because it's effectively a NOOP.", event.getEventType(), event);
                return;
            }
        }
        dispatchChangeEvent(event);
    }

    private boolean tryMerge(DmlEvent prev, DmlEvent next) {
        if (prev == null) { // first event for this row.
            return false;
        }

        // we can only merge into INSERT, UPDATE and SEL_LOB_LOCATOR
        // we can only merge from UPDATE and SEL_LOB_LOCATOR
        // merges _from_ SEL_LOB_LOCATOR are basically noops.
        // merges _from_ UPDATE mean we have to override the specified values.

        boolean merge = false;
        switch (prev.getEventType()) {
            case INSERT:
            case UPDATE:
            case XML_BEGIN:
            case SELECT_LOB_LOCATOR:
            case EXTENDED_STRING_BEGIN:
                switch (next.getEventType()) {
                    case XML_BEGIN:
                        merge = true;
                        break;
                    case SELECT_LOB_LOCATOR:
                        if (EventType.SELECT_LOB_LOCATOR == prev.getEventType()) {
                            if (isSelectLobLocatorForSameRow(prev, next)) {
                                merge = true;
                            }
                        }
                        else {
                            merge = true;
                        }
                        break;
                    case UPDATE:
                        if (EventType.UPDATE == prev.getEventType()) {
                            if (isUpdateForSameTableWithLobColumnChanges(prev, next)) {
                                mergeEvents(prev, next);
                                merge = true;
                            }
                        }
                        else {
                            // UPDATE always merges into other event types.
                            mergeEvents(prev, next);
                            merge = true;
                        }
                        break;
                    case EXTENDED_STRING_BEGIN:
                        // todo: are there any special logic here?
                        merge = true;
                        break;
                    case EXTENDED_STRING_WRITE:
                        if (EventType.EXTENDED_STRING_BEGIN == prev.getEventType()) {
                            mergeEvents(prev, next);
                            merge = true;
                        }
                        break;
                    default:
                        break;
                }
            default:
        }
        if (merge) {
            LOGGER.trace("\tMerging {} event into previous {} event.", next.getEventType(), prev.getEventType());
        }
        return merge;
    }

    private void mergeEvents(DmlEvent into, DmlEvent from) {
        Object[] intoVals = newValues(into);
        Object[] fromVals = newValues(from);
        for (int i = 0; i < intoVals.length; i++) {
            if (!OracleValueConverters.UNAVAILABLE_VALUE.equals(fromVals[i])) {
                LOGGER.trace("\t\tMerge column {}: replacing {} with {}.", i, intoVals[i], fromVals[i]);
                intoVals[i] = fromVals[i];
            }
        }
    }

    private boolean isUpdateForSameTableWithLobColumnChanges(DmlEvent into, DmlEvent event) {
        if (!into.getTableId().equals(event.getTableId())) {
            LOGGER.trace("\tUPDATE is for table '{}' and cannot be merged into an event for table '{}'.",
                    event.getTableId(), into.getTableId());
            return false;
        }

        final Table table = schema.tableFor(event.getTableId());
        if (Objects.isNull(table)) {
            throw new DebeziumException("Failed to find schema for update on table: " + event.getTableId());
        }

        final Object[] newValues = newValues(event);
        if (newValues.length > table.columns().size()) {
            throw new DebeziumException(String.format(
                    "Schema mismatch between event with %d columns and table having %d columns",
                    newValues.length, table.columns().size()));
        }

        // Check if we are merging two update events into one another.
        // If these two events have ROWID values that don't match 'AAAAAAAAAAAAAAAAAA' and are different,
        // then prevent the merge as they're two unique rows that were modified.
        if (hasRowId(into) && hasRowId(event)) {
            if (!into.getRowId().equals(event.getRowId())) {
                // Different ROWID values, merge isn't possible
                return false;
            }
        }

        // For each new value being SET by the UPDATE, we check whether the column is a BLOB or CLOB
        // If the column is an LOB and its new value isn't the placeholder, we force a merge.
        for (int i = 0; i < newValues.length; ++i) {
            final Column column = table.columns().get(i);
            if (isLobColumn(column) && !OracleValueConverters.UNAVAILABLE_VALUE.equals(newValues[i])) {
                LOGGER.trace("\tFor table {} which has an LOB column {}, merging.", event.getTableId(), column.name());
                return true;
            }
        }

        // The UPDATE isn't setting any LOB columns, so it's safe to assume a separate logical change and not merge.
        LOGGER.trace("\tFor table {} that has no LOB columns, merge skipped.", event.getTableId());
        return false;
    }

    private boolean isSelectLobLocatorForSameRow(DmlEvent into, DmlEvent event) {
        if (!into.getTableId().equals(event.getTableId())) {
            LOGGER.trace("\tSELECT_LOB_LOCATOR is for table '{}' and cannot be merged into event for table '{}'.",
                    event.getTableId(), into.getTableId());
            return false;
        }

        final Table table = schema.tableFor(event.getTableId());
        if (Objects.isNull(table)) {
            throw new DebeziumException("Failed to find schema for SElECT_LOB_LOCATOR on table: " + event.getTableId());
        }

        final Object[] newValues = newValues(into);
        final Object[] oldValues = oldValues(event);
        if (!table.primaryKeyColumnNames().isEmpty()) {
            // For primary key tables, compare only keys
            for (String columnName : table.primaryKeyColumnNames()) {
                int columnIndex = LogMinerHelper.getColumnIndexByName(columnName, table);
                if (columnIndex < newValues.length && columnIndex < oldValues.length) {
                    if (!newValues[columnIndex].equals(oldValues[columnIndex])) {
                        LOGGER.trace("\tSELECT_LOB_LOCATOR are for different primary keys, cannot merge.");
                        // different primary keys
                        return false;
                    }
                }
            }
        }
        else {
            // For keyless tables, compare non-lob columns
            for (Column column : table.columns()) {
                if (isLobColumn(column)) {
                    // Skip comparing LOB columns
                    continue;
                }
                int columnIndex = LogMinerHelper.getColumnIndexByName(column);
                if (columnIndex < newValues.length && columnIndex < oldValues.length) {
                    if (!newValues[columnIndex].equals(oldValues[columnIndex])) {
                        LOGGER.trace("\tSELECT_LOB_LOCATOR prev/new state differ for column '{}', cannot merge.", column.name());
                        return false;
                    }
                }
            }
        }

        // Same table and same non-lob column data
        return true;
    }

    private boolean isLobColumn(Column column) {
        return BLOB_TYPE.equalsIgnoreCase(column.typeName()) || CLOB_TYPE.equalsIgnoreCase(column.typeName());
    }

    private void dispatchChangeEvent(LogMinerEvent event) throws InterruptedException {
        final long eventIndex = dispatchEventIndex++;
        LOGGER.trace("\tEmitting event #{}: {} {}", eventIndex, event.getEventType(), event);
        delegate.accept(event, eventIndex, totalEvents);
    }

    private String rowIdFromEvent(Table table, DmlEvent event) {
        List<String> idParts = new ArrayList<>();
        idParts.add(event.getTableId().toString());

        Object[] values = (EventType.DELETE == event.getEventType()) ? oldValues(event) : newValues(event);

        if (event.getEventType() == EventType.DDL && event instanceof TruncateEvent) {
            // This is a special use case with TruncateEvent(s)
            // In this case the row-id should be just the table-name
            return String.join("|", idParts);
        }

        for (String columnName : table.primaryKeyColumnNames()) {
            int position = LogMinerHelper.getColumnIndexByName(columnName, table);
            if (position >= values.length) {
                throw new DebeziumException("Field values corrupt for " + event.getEventType() + " " + event);
            }
            Object value = values[position];
            idParts.add(value == null ? NULL_COLUMN : value.toString());
        }
        return String.join("|", idParts);
    }

    private Object[] newValues(DmlEvent event) {
        return event.getDmlEntry().getNewValues();
    }

    private Object[] oldValues(DmlEvent event) {
        return event.getDmlEntry().getOldValues();
    }

    private void discardCurrentMergeState(ConstructionDetails details) {
        final RowState state = rows.get(details.rowId);
        if (state != null) {
            LOGGER.trace("Discarding merge state for row id {}", details.rowId);
            rows.remove(details.rowId);
            details.reset();
        }
    }

    private boolean hasRowId(DmlEvent event) {
        return !Strings.isNullOrEmpty(event.getRowId()) && !event.getRowId().equalsIgnoreCase("AAAAAAAAAAAAAAAAAA");
    }

    static class ConstructionDetails {
        String rowId;
        String columnName;
        int columnPosition = -1;

        boolean isInitialized() {
            return rowId != null && columnName != null;
        }

        void reset() {
            rowId = null;
            columnName = null;
            columnPosition = -1;
        }
    }

    static class Fragment {
        String data;
    }

    static abstract class AbstractUnderConstruction<T extends Fragment> {
        protected List<T> fragments = new LinkedList<>();
        protected boolean isNull = true;

        void add(T fragment) {
            isNull = false;
            doAdd(fragment);
        }

        abstract Object merge();

        protected void doAdd(T fragment) {
            fragments.add(fragment);
        }

    }

    static class LobFragment extends Fragment {
        boolean binary;
        byte[] bytes;
        int offset;

        LobFragment(final LogMinerEvent event) {
            if (EventType.LOB_WRITE != event.getEventType()) {
                throw new IllegalArgumentException("can only construct LobFragments from LOB_WRITE events");
            }
            final LobWriteEvent writeEvent = (LobWriteEvent) event;
            initializeFromData(writeEvent.getData());
            this.offset = writeEvent.getOffset();

            // DBMS_LOB.WRITE rules:
            // length (from the writeEvent) may not be larger than buffer length, but it may be shorter. We don't expect
            // that to happen in the LogMiner events, but it doesn't hurt to check.
            final int eventLength = writeEvent.getLength();
            if (eventLength < length()) {
                truncate(eventLength);
            }
        }

        LobFragment(final String value) {
            initializeFromData(value);
            this.offset = 0;
        }

        private void initializeFromData(String data) {
            this.binary = data.startsWith(OracleValueConverters.HEXTORAW_FUNCTION_START)
                    && data.endsWith(OracleValueConverters.HEXTORAW_FUNCTION_END);
            if (this.binary) {
                try {
                    this.bytes = RAW.hexString2Bytes(data.substring(10, data.length() - 2));
                }
                catch (SQLException e) {
                    throw new DebeziumException("malformed hex string in LogMiner event BLOB value", e);
                }
            }
            else {
                this.data = data;
            }
        }

        int length() {
            return binary ? bytes.length : data.length();
        }

        int end() {
            return offset + length();
        }

        void truncate(int newLength) {
            if (newLength > length()) {
                throw new DebeziumException("cannot truncate LOB fragment from length " + length() + " to length " + newLength);
            }

            if (binary) {
                bytes = Arrays.copyOf(bytes, newLength);
            }
            else {
                data = data.substring(0, newLength);
            }
        }

        void frontTruncate(int newLength) {
            if (newLength > length()) {
                throw new DebeziumException("cannot front-truncate LOB fragment from length " + length() + " to length " + newLength);
            }

            if (binary) {
                bytes = Arrays.copyOfRange(bytes, bytes.length - newLength, bytes.length);
            }
            else {
                data = data.substring(data.length() - newLength);
            }
            offset += length() - newLength;
        }

        void absorb(LobFragment other) {
            if (other.offset < offset || other.end() > end()) {
                throw new DebeziumException(
                        "cannot absorb fragment (" + other.offset + ", " + other.end() + ") into fragment " +
                                "(" + offset + ", " + end() + ") because the absorbee does not fully overlap the absorber");
            }

            int prefixEnd = other.offset - offset;
            int suffixStart = other.end() - offset;

            if (binary) {
                System.arraycopy(other.bytes, 0, bytes, prefixEnd, other.bytes.length);
            }
            else {
                data = data.substring(0, prefixEnd) + other.data + data.substring(suffixStart);
            }
        }

        void append(LobFragment other) {
            if (other.offset < end()) {
                throw new DebeziumException("cannot append fragment: offset " + other.offset + " is before this " +
                        "fragment's end " + end());
            }

            if (binary) {
                bytes = Arrays.copyOf(bytes, other.end() - offset); // pads with zeroes
                System.arraycopy(other.bytes, 0, bytes, other.offset - offset, other.bytes.length);
            }
            else {
                int gap = other.offset - end();
                if (gap > 0) {
                    data = data + spaces(gap) + other.data;
                }
                else {
                    data = data + other.data;
                }
            }
        }

        static String spaces(int length) {
            char[] backing = new char[length];
            Arrays.fill(backing, ' ');
            return new String(backing);
        }
    }

    static class LobUnderConstruction extends AbstractUnderConstruction<LobFragment> {
        int start = 0;
        int end = 0;
        boolean binary = false;

        int middleInserts = 0;

        @Override
        protected void doAdd(LobFragment fragment) {
            if (fragments.isEmpty()) { // first fragment to be added
                fragments.add(fragment);
                start = fragment.offset;
                end = fragment.end();
                binary = fragment.binary;
                return;
            }

            if (fragment.binary != binary) {
                throw new DebeziumException("mixing binary and non-binary writes in a single LOB");
            }

            if (fragment.offset >= end) { // the expected case
                fragments.add(fragment);
                end = fragment.end();
                return;
            }

            // the uncommon case: writing somewhere in the middle
            middleInserts++;
            if (middleInserts % 10 == 0) {
                compact(); // try to keep the linear search time within reasonable bounds
            }

            // find the right spot to insert
            ListIterator<LobFragment> iter = fragments.listIterator();
            while (iter.hasNext()) {
                LobFragment frag = iter.next();
                if (fragment.offset < frag.end() && fragment.offset >= frag.offset) {
                    if (fragment.end() >= frag.end()) { // fragment partially overlaps frag
                        // truncate frag and insert after
                        frag.truncate(fragment.offset - frag.offset);
                        iter.add(fragment);
                    }
                    else { // fragment overlaps frag entirely
                        frag.absorb(fragment);
                    }
                    break;
                }
                if (frag.offset > fragment.offset) {
                    // insert before; no need to truncate preceding fragment
                    iter.previous();
                    iter.add(fragment);
                    break;
                }
            }

            // are there any following fragments that are (partially) overwritten by the fragment we're inserting?
            while (iter.hasNext()) {
                LobFragment frag = iter.next();
                if (frag.offset >= fragment.end()) { // we're done
                    break;
                }
                if (frag.end() <= fragment.end()) { // remove entirely
                    iter.remove();
                }
                else { // front-truncate
                    frag.frontTruncate(frag.end() - fragment.end());
                }
            }

            // adjust start and end bookkeeping as necessary
            if (fragment.offset < start) {
                start = fragment.offset;
            }
            if (fragment.end() > end) {
                end = fragment.end();
            }
        }

        void compact() {
            ListIterator<LobFragment> iter = fragments.listIterator();
            if (!iter.hasNext()) {
                return;
            }
            LobFragment prev = iter.next();
            while (iter.hasNext()) {
                LobFragment frag = iter.next();
                if (frag.offset - prev.end() < 128) {
                    prev.append(frag);
                    iter.remove();
                }
                else {
                    prev = frag;
                }
            }
        }

        /**
         * Merges all LOB fragments.
         *
         * Returns:
         *  - null if the isNull flag is set
         *  - "EMPTY_BLOB()" or "EMPTY_CLOB()" the lob is empty, but isNull is not set
         *  - a single String for (N)CLOB
         *  - a single byte[] from BLOB
         * Any holes will be filled with spaces (CLOB) or zero bytes (BLOB) as per the specification of DBMS_LOB.WRITE.
         */
        @Override
        Object merge() {
            if (isNull) {
                return null;
            }
            if (end == 0) {
                if (binary) {
                    return OracleValueConverters.EMPTY_BLOB_FUNCTION;
                }
                return OracleValueConverters.EMPTY_CLOB_FUNCTION;
            }

            if (binary) {
                byte[] buffer = new byte[end];
                ListIterator<LobFragment> iter = fragments.listIterator();
                while (iter.hasNext()) {
                    LobFragment frag = iter.next();
                    System.arraycopy(frag.bytes, 0, buffer, frag.offset, frag.bytes.length);
                }
                return buffer;
            }
            else {
                StringBuilder builder = new StringBuilder();
                int offset = 0;
                ListIterator<LobFragment> iter = fragments.listIterator();
                while (iter.hasNext()) {
                    LobFragment frag = iter.next();
                    if (offset < frag.offset) { // fill the holes between fragments
                        builder.append(LobFragment.spaces(frag.offset - offset));
                    }
                    if (frag.length() == 0) { // may happen in rare corner cases
                        continue;
                    }
                    builder.append(frag.data);
                    offset = frag.end();
                }
                return builder.toString();
            }
        }

        public String toString() {
            return "LobUnderConstruction{" +
                    "binary = " + binary +
                    ", start = " + start +
                    ", end = " + end +
                    ", #fragments = " + fragments.size() +
                    "}";
        }

        // Creates a LobUnderConstruction instance from the initial value stored in the
        // parent event's column.
        static LobUnderConstruction fromInitialValue(Object value) {
            if (null == value) {
                return new LobUnderConstruction();
            }
            if (value instanceof LobUnderConstruction) {
                return (LobUnderConstruction) value;
            }
            if (value instanceof String) {
                String strval = (String) value;
                LobUnderConstruction lob = new LobUnderConstruction();
                if (OracleValueConverters.EMPTY_BLOB_FUNCTION.equals(strval)) {
                    lob.binary = true;
                    lob.isNull = false; // lob must be emitted
                }
                else if (OracleValueConverters.EMPTY_CLOB_FUNCTION.equals(strval)) {
                    lob.binary = false;
                    lob.isNull = false; // lob must be emitted
                }
                else {
                    lob.add(new LobFragment(strval));
                }
                return lob;
            }

            LOGGER.trace("Don't know how to construct an initial LOB value from {}.", value);
            return new LobUnderConstruction();
        }
    }

    static class XmlFragment extends Fragment {
        XmlFragment(final XmlWriteEvent event) {
            if (EventType.XML_WRITE != event.getEventType()) {
                throw new IllegalArgumentException("can only construct XmlFragments from XML_WRITE events");
            }
            this.data = event.getXml();
        }

        XmlFragment(String data) {
            this.data = data;
        }
    }

    static class XmlUnderConstruction extends AbstractUnderConstruction<XmlFragment> {

        static XmlUnderConstruction fromInitialValue(Object value) {
            if (null == value) {
                return new XmlUnderConstruction();
            }
            if (value instanceof XmlUnderConstruction) {
                return (XmlUnderConstruction) value;
            }
            if (value instanceof String) {
                XmlUnderConstruction lob = new XmlUnderConstruction();
                lob.add(new XmlFragment((String) value));
                return lob;
            }

            LOGGER.trace("Don't know how to construct an initial XML value from {}.", value);
            return new XmlUnderConstruction();
        }

        @Override
        Object merge() {
            if (isNull) {
                return null;
            }
            final StringBuilder builder = new StringBuilder();
            fragments.forEach(fragment -> builder.append(fragment.data));
            return builder.toString();
        }

    }

    static class ExtendedStringFragment extends Fragment {
        private final String data;

        ExtendedStringFragment(ExtendedStringWriteEvent event) {
            if (EventType.EXTENDED_STRING_WRITE != event.getEventType()) {
                throw new IllegalArgumentException("Can only construct ExtendedStringFragment from 32K_WRITE events");
            }
            this.data = event.getData();
        }

        ExtendedStringFragment(String data) {
            this.data = data;
        }
    }

    static class ExtendedStringUnderConstruction extends AbstractUnderConstruction<ExtendedStringFragment> {
        static ExtendedStringUnderConstruction fromInitialValue(Object value) {
            if (null == value) {
                return new ExtendedStringUnderConstruction();
            }
            if (value instanceof ExtendedStringUnderConstruction) {
                return (ExtendedStringUnderConstruction) value;
            }
            if (value instanceof String) {
                final String strval = (String) value;
                ExtendedStringUnderConstruction lob = new ExtendedStringUnderConstruction();
                if (!OracleValueConverters.EMPTY_EXTENDED_STRING.equals(strval)) {
                    lob.add(new ExtendedStringFragment((String) value));
                }
                return lob;
            }

            LOGGER.trace("Don't know how to construct an initial extended string value from {}.", value);
            return new ExtendedStringUnderConstruction();
        }

        @Override
        Object merge() {
            if (isNull) {
                return null;
            }
            final StringBuilder builder = new StringBuilder();
            fragments.forEach(fragment -> builder.append(fragment.data));
            return builder.toString();
        }
    }

    private static class RowState {
        final DmlEvent event;
        final long transactionIndex;

        RowState(final DmlEvent event, final long transactionIndex) {
            this.event = event;
            this.transactionIndex = transactionIndex;
        }
    }

    @FunctionalInterface
    public interface Handler<T> {
        void accept(T event, long eventIndex, long eventsProcessed) throws InterruptedException;
    }
}
