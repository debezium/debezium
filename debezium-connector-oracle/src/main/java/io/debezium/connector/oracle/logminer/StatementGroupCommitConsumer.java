/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

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
import io.debezium.connector.oracle.logminer.events.RowIdCodec;
import io.debezium.connector.oracle.logminer.events.SelectLobLocatorEvent;
import io.debezium.connector.oracle.logminer.events.XmlBeginEvent;
import io.debezium.connector.oracle.logminer.events.XmlEndEvent;
import io.debezium.connector.oracle.logminer.events.XmlWriteEvent;
import io.debezium.relational.Column;
import io.debezium.relational.Table;

import oracle.sql.RAW;

/**
 * A consumer of transaction events at commit time that groups the events LogMiner supplies for one
 * statement's change to one row, and emits each group as a single logical event.
 * <p>
 * When LOB support is enabled, a single statement that changes a row with LOB, XML or extended string
 * columns is supplied as several events, for example:
 *
 * <pre>
 *     INSERT              the row, with EMPTY_CLOB() for each LOB column
 *     SEL_LOB_LOCATOR     names a LOB column of the row
 *     LOB_WRITE (n times) the column's value in pieces
 *     UPDATE              the remaining LOB columns that are stored in-line
 * </pre>
 *
 * The events of a group are cached in arrival order. The group is squashed into its first event and
 * emitted when an event arrives that cannot belong to it, or when the transaction ends. Only one group
 * is cached at a time, so events are always emitted in the order the changes were made.
 * <p>
 * A value written through an application's LOB locator, for example with {@code DBMS_LOB.WRITE}, is supplied
 * one call at a time, and a call only carries the piece it wrote, not the rest of the value. Calls for the
 * same row that directly follow one another are assembled as one group. The pieces are only emitted as the
 * column's value when the transaction established what the column held just before, with the row's insert
 * or a statement that set the column and no change to anything else in between, and the pieces replace all
 * of it. Otherwise, the column is emitted with the unavailable value placeholder.
 * <p>
 * When LOB support isn't enabled, events are passed through to the delegate.
 *
 * @author Chris Cranford
 */
public class StatementGroupCommitConsumer implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(StatementGroupCommitConsumer.class);

    private final Handler<LogMinerEvent> delegate;
    private final OracleConnectorConfig connectorConfig;
    private final OracleDatabaseSchema schema;
    private final LobValueResolver lobValueResolver;

    // Length of each LOB column of the row being changed, as established by the transaction so far
    private final Map<Integer, Integer> rowLobLengths = new HashMap<>();

    private Group group;
    // A statement's second locator for a column, held until the next event shows what it is for
    private PendingLocator pendingLocator;
    private long dispatchEventIndex = 0;
    private int totalEvents = 0;

    public StatementGroupCommitConsumer(Handler<LogMinerEvent> delegate, OracleConnectorConfig connectorConfig, OracleDatabaseSchema schema,
                                        LobValueResolver lobValueResolver) {
        this.delegate = delegate;
        this.connectorConfig = connectorConfig;
        this.schema = schema;
        this.lobValueResolver = lobValueResolver;
    }

    @Override
    public void close() throws InterruptedException {
        resolvePendingLocator(null);
        flush();
        rowLobLengths.clear();

        // For situations where the consumer instance is reused, reset internal state
        totalEvents = 0;
        dispatchEventIndex = 0;
    }

    public void accept(LogMinerEvent event, String transactionId, long transactionSequence) throws InterruptedException {
        totalEvents++;

        if (!connectorConfig.isLobEnabled()) {
            dispatch(event, transactionId, transactionSequence);
            return;
        }

        resolvePendingLocator(event);
        if (event instanceof DmlEvent dmlEvent) {
            acceptRowEvent(dmlEvent, transactionId, transactionSequence);
        }
        else {
            acceptFragmentEvent(event, transactionSequence);
        }
    }

    public int getTotalEvents() {
        return totalEvents;
    }

    private void acceptRowEvent(DmlEvent event, String transactionId, long transactionSequence) throws InterruptedException {
        final Table table = schema.tableFor(event.getTableId());
        if (table == null) {
            LOGGER.debug("Unable to locate table '{}' schema, ignoring event.", event.getTableId());
            return;
        }

        final Set<Integer> lobPositions = getLobColumnPositions(table);
        final boolean sameRow = isForGroupRow(event, lobPositions);
        switch (classify(event, lobPositions)) {
            case CONTINUES -> group.add(event, transactionSequence);
            case COMPLETES -> {
                group.add(event, transactionSequence);
                group.complete = true;
            }
            case DEFERRED -> pendingLocator = new PendingLocator(table, lobPositions, (SelectLobLocatorEvent) event, transactionId, transactionSequence);
            case STARTS -> {
                flush(sameRow);
                group = new Group(table, lobPositions, event, transactionId, transactionSequence);
            }
            case STARTS_COMPLETE -> {
                flush(sameRow);
                group = new Group(table, lobPositions, event, transactionId, transactionSequence);
                group.complete = true;
            }
            case STANDALONE -> {
                flush();
                dispatch(event, transactionId, transactionSequence);
            }
        }
    }

    private void acceptFragmentEvent(LogMinerEvent event, long transactionSequence) {
        if (group == null) {
            // Happens when streaming starts in the middle of a LOB operation (DBZ-4367)
            LOGGER.debug("Got {} without a preceding row event; ignoring {}.", event.getEventType(), event);
        }
        else if (event instanceof LobEraseEvent) {
            if (group.lobColumn) {
                LOGGER.warn("\t{} for table '{}' is not supported, the change is skipped.", event.getEventType(), event.getTableId());
                group = null;
                rowLobLengths.clear();
            }
        }
        else if (group.accepts(event)) {
            group.add(event, transactionSequence);
        }
        else {
            LOGGER.debug("Got {} without a preceding locator or begin event; ignoring {}.", event.getEventType(), event);
        }
    }

    /**
     * Decides how a row event relates to the cached group.
     */
    private Role classify(DmlEvent event, Set<Integer> lobPositions) {
        if (lobPositions.isEmpty()) {
            return Role.STANDALONE;
        }

        return switch (event.getEventType()) {
            case INSERT -> Role.STARTS;
            case UPDATE -> classifyUpdate(event, lobPositions);
            case SELECT_LOB_LOCATOR -> classifyLocator((SelectLobLocatorEvent) event, lobPositions);
            // A statement writes one document per XML column, so a second one is another statement
            case XML_BEGIN -> isForGroupRow(event, lobPositions) && !group.hasEndedXmlFor((XmlBeginEvent) event) ? Role.CONTINUES : Role.STARTS;
            // A statement writes an extended string column once, and never after its in-line update, so a
            // value that follows either is another statement
            case EXTENDED_STRING_BEGIN -> isForGroupRow(event, lobPositions) && !group.complete
                    && !group.hasExtendedStringFor((ExtendedStringBeginEvent) event)
                            ? Role.CONTINUES
                            : Role.STARTS;
            default -> Role.STANDALONE;
        };
    }

    private Role classifyLocator(SelectLobLocatorEvent event, Set<Integer> lobPositions) {
        if (!isForGroupRow(event, lobPositions) || group.complete) {
            return Role.STARTS;
        }
        // An update of only an out-of-line value has no UPDATE row, its locator is all that starts it.
        // The same locator can also precede the trim that ends the statement that is cached, so the
        // event that follows decides. An application's locator has a row id and always continues.
        return isStatementLocator(event) && group.hasLocatorFor(event) ? Role.DEFERRED : Role.CONTINUES;
    }

    /**
     * Decides what a held locator is for, now that the event that follows it is known.
     *
     * @param next the event that follows the locator, {@code null} at the end of the transaction
     */
    private void resolvePendingLocator(LogMinerEvent next) throws InterruptedException {
        if (pendingLocator == null) {
            return;
        }
        final PendingLocator pending = pendingLocator;
        pendingLocator = null;

        if (next instanceof LobWriteEvent) {
            // The locator is written to, which makes it the start of another statement for the same row
            flush(true);
            group = new Group(pending.table, pending.lobPositions, pending.event, pending.transactionId, pending.transactionSequence);
        }
        else {
            group.add(pending.event, pending.transactionSequence);
        }
    }

    private Role classifyUpdate(DmlEvent event, Set<Integer> lobPositions) {
        final Set<Integer> written = getWrittenLobPositions(event, lobPositions);
        if (written.isEmpty() || changesOtherColumns(event, lobPositions) || isPlaceholderOnly(event, written)) {
            // The first event of an UPDATE statement
            return Role.STARTS;
        }

        // An update of only in-line LOB values is the last row event of its statement, and directly follows
        // the statement's other events
        if (isForGroupRow(event, lobPositions) && !group.complete && !group.hasLocatorFor(written) && !group.hasExtendedStringFor(written)) {
            return Role.COMPLETES;
        }
        return Role.STARTS_COMPLETE;
    }

    private boolean isForGroupRow(DmlEvent event, Set<Integer> lobPositions) {
        if (group == null || !group.first.getTableId().equals(event.getTableId())) {
            return false;
        }
        if (hasRowId(group.first) && hasRowId(event) && !group.first.getRowId().equals(event.getRowId())) {
            return false;
        }

        final Object[] groupValues = group.first.getNewValues();
        final Object[] eventValues = event.getOldValues().length > 0 ? event.getOldValues() : event.getNewValues();

        final List<String> keyColumnNames = group.table.primaryKeyColumnNames();
        if (!keyColumnNames.isEmpty()) {
            for (String columnName : keyColumnNames) {
                if (!isSameValue(groupValues, eventValues, LogMinerHelper.getColumnIndexByName(columnName, group.table))) {
                    return false;
                }
            }
            return true;
        }

        // For keyless tables, compare the other columns
        for (Column column : group.table.columns()) {
            final int position = LogMinerHelper.getColumnIndexByName(column);
            if (!lobPositions.contains(position) && !isSameValue(groupValues, eventValues, position)) {
                return false;
            }
        }
        return true;
    }

    private void flush() throws InterruptedException {
        flush(false);
    }

    /**
     * @param sameRow whether the event that follows changes the same row as the cached group
     */
    private void flush(boolean sameRow) throws InterruptedException {
        if (group != null) {
            final Group flushed = group;
            group = null;

            final DmlEvent event = squash(flushed);
            if (event != null) {
                dispatch(event, flushed.transactionId, flushed.transactionSequence);
            }
        }

        if (!sameRow) {
            // What is known about the row's LOB values does not outlive a change to something else
            rowLobLengths.clear();
        }
    }

    /**
     * Folds the group's events into its first event, returning {@code null} if nothing should be emitted.
     */
    private DmlEvent squash(Group group) {
        final DmlEvent result = group.first;
        final Object[] values = result.getNewValues();

        // The values being assembled, by column position; the event only receives the finished values
        final Map<Integer, AbstractUnderConstruction<?>> constructions = new HashMap<>();
        // Columns written through an application's locator that have no value in the group to start from
        final Map<Integer, SelectLobLocatorEvent> withoutBase = new HashMap<>();
        LobUnderConstruction lob = null;
        TextUnderConstruction xml = null;
        TextUnderConstruction extendedString = null;

        for (LogMinerEvent event : group.events) {
            try {
                if (event instanceof SelectLobLocatorEvent locator) {
                    // Writes must not reach another column's value when this locator's value cannot be started
                    lob = null;
                    final int position = LogMinerHelper.getColumnIndexByName(locator.getColumnName(), group.table);
                    if (!constructions.containsKey(position) && hasRowId(locator)
                            && (event == result || OracleValueConverters.UNAVAILABLE_VALUE.equals(values[position]))) {
                        withoutBase.put(position, locator);
                    }
                    lob = (LobUnderConstruction) constructions.computeIfAbsent(position, p -> LobUnderConstruction.fromInitialValue(values[p], locator.isBinary()));
                }
                else if (event instanceof XmlBeginEvent begin) {
                    xml = new TextUnderConstruction();
                    constructions.put(LogMinerHelper.getColumnIndexByName(begin.getColumnName(), group.table), xml);
                }
                else if (event instanceof ExtendedStringBeginEvent begin) {
                    extendedString = new TextUnderConstruction();
                    constructions.put(LogMinerHelper.getColumnIndexByName(begin.getColumnName(), group.table), extendedString);
                }
                else if (event instanceof LobWriteEvent write) {
                    if (lob != null) {
                        lob.add(new LobFragment(write));
                    }
                    else {
                        LOGGER.debug("\tGot {} for a locator without a usable value; ignoring {}.", event.getEventType(), event);
                    }
                }
                else if (event instanceof XmlWriteEvent write) {
                    if (write.getXml() != null) {
                        xml.add(new Fragment(write.getXml()));
                    }
                }
                else if (event instanceof ExtendedStringWriteEvent write) {
                    extendedString.add(new Fragment(write.getData()));
                }
                else if (event instanceof DmlEvent update && event != result) {
                    final Object[] updateValues = update.getNewValues();
                    copyAvailableValues(updateValues, values);
                    // A value supplied by a later row event replaces what was assembled for the column so far
                    constructions.keySet().removeIf(position -> position < updateValues.length
                            && !OracleValueConverters.UNAVAILABLE_VALUE.equals(updateValues[position]));
                    withoutBase.keySet().retainAll(constructions.keySet());
                }
            }
            catch (DebeziumException e) {
                LOGGER.warn("\tInvalid {} event: {} ; ignoring {}", event.getEventType(), e, event);
            }
        }

        final Set<Integer> reselected = new HashSet<>();
        for (Map.Entry<Integer, AbstractUnderConstruction<?>> entry : constructions.entrySet()) {
            final int position = entry.getKey();
            final SelectLobLocatorEvent locator = withoutBase.get(position);
            if (locator != null && !isWholeValue((LobUnderConstruction) entry.getValue(), rowLobLengths.get(position))) {
                // What was written is known, but not the rest of the value, so the value is read from the database
                LOGGER.debug("\tLOB column at position {} of table '{}' was written through a locator without a known value; reselecting it.",
                        position, group.table.id());
                values[position] = lobValueResolver.resolve(group.table, group.table.columns().get(position), locator);
                reselected.add(position);
            }
            else {
                values[position] = entry.getValue().merge();
            }
        }

        for (int position : group.lobPositions) {
            if (reselected.contains(position)) {
                // The value is the one at commit, which says nothing about the column's length at this point
                rowLobLengths.remove(position);
            }
            else if (position < values.length && !OracleValueConverters.UNAVAILABLE_VALUE.equals(values[position])) {
                try {
                    rowLobLengths.put(position, getLobLength(values[position]));
                }
                catch (DebeziumException e) {
                    // A malformed value has no known length
                    rowLobLengths.remove(position);
                }
            }
        }

        if (reselected.isEmpty() && EventType.SELECT_LOB_LOCATOR == result.getEventType() && isUnchanged(result)) {
            LOGGER.trace("\tSkip emitting event {} {} because it's effectively a NOOP.", result.getEventType(), result);
            return null;
        }
        return result;
    }

    private void dispatch(LogMinerEvent event, String transactionId, long transactionSequence) throws InterruptedException {
        // Must be one based so that START_SCN/START_SCN_TS assignment works in delegate
        final long eventIndex = ++dispatchEventIndex;
        LOGGER.trace("\tEmitting event #{}: {} {}", eventIndex, event.getEventType(), event);
        delegate.accept(event, eventIndex, transactionId, transactionSequence, totalEvents);
    }

    private Set<Integer> getLobColumnPositions(Table table) {
        final Set<Integer> positions = new HashSet<>();
        for (Column column : schema.getLobColumnsForTable(table.id())) {
            positions.add(LogMinerHelper.getColumnIndexByName(column));
        }
        return positions;
    }

    private static Set<Integer> getWrittenLobPositions(DmlEvent event, Set<Integer> lobPositions) {
        final Object[] values = event.getNewValues();
        final Set<Integer> written = new HashSet<>();
        for (int position : lobPositions) {
            if (position < values.length && !OracleValueConverters.UNAVAILABLE_VALUE.equals(values[position])) {
                written.add(position);
            }
        }
        return written;
    }

    private static boolean changesOtherColumns(DmlEvent event, Set<Integer> lobPositions) {
        final Object[] oldValues = event.getOldValues();
        final Object[] newValues = event.getNewValues();
        for (int i = 0; i < Math.min(oldValues.length, newValues.length); i++) {
            if (!lobPositions.contains(i) && !Objects.equals(oldValues[i], newValues[i])) {
                return true;
            }
        }
        return false;
    }

    private static boolean isPlaceholderOnly(DmlEvent event, Set<Integer> written) {
        // Oracle sets a NULL LOB column to EMPTY_CLOB() or EMPTY_BLOB() before the statement writes its value
        final Object[] values = event.getNewValues();
        for (int position : written) {
            if (!OracleValueConverters.EMPTY_CLOB_FUNCTION.equals(values[position])
                    && !OracleValueConverters.EMPTY_BLOB_FUNCTION.equals(values[position])) {
                return false;
            }
        }
        return true;
    }

    private static boolean isWholeValue(LobUnderConstruction lob, Integer knownLength) {
        // The value is only known when the pieces replace everything the column held before
        return knownLength != null && lob.coversFromStart(knownLength);
    }

    private static int getLobLength(Object value) {
        if (value instanceof byte[] bytes) {
            return bytes.length;
        }
        if (value instanceof String text && !OracleValueConverters.EMPTY_CLOB_FUNCTION.equals(text)
                && !OracleValueConverters.EMPTY_BLOB_FUNCTION.equals(text)) {
            return new LobFragment(text).length();
        }
        return 0;
    }

    private static boolean isSameValue(Object[] left, Object[] right, int position) {
        return position >= left.length || position >= right.length || Objects.equals(left[position], right[position]);
    }

    private static boolean isUnchanged(DmlEvent event) {
        final Object[] oldValues = event.getOldValues();
        final Object[] newValues = event.getNewValues();
        for (int i = 0; i < newValues.length; i++) {
            if (!Objects.equals(oldValues[i], newValues[i])) {
                return false;
            }
        }
        return true;
    }

    private static void copyAvailableValues(Object[] from, Object[] into) {
        for (int i = 0; i < Math.min(from.length, into.length); i++) {
            if (!OracleValueConverters.UNAVAILABLE_VALUE.equals(from[i])) {
                into[i] = from[i];
            }
        }
    }

    private static boolean isStatementLocator(DmlEvent event) {
        // LogMiner supplies the locator of a statement without a row id
        return !hasRowId(event);
    }

    private static boolean hasRowId(DmlEvent event) {
        return event.getRowId() != null && !event.getRowId().equals(RowIdCodec.EMPTY_ROW_ID);
    }

    /**
     * How a row event relates to the cached group.
     */
    private enum Role {
        /** Belongs to the cached group, more events may follow. */
        CONTINUES,
        /** Belongs to the cached group and is the last row event of its statement. */
        COMPLETES,
        /** A locator that either belongs to the cached group or starts a new one, the next event decides. */
        DEFERRED,
        /** Ends the cached group and starts a new one. */
        STARTS,
        /** Ends the cached group and is a whole statement by itself. */
        STARTS_COMPLETE,
        /** Ends the cached group and is emitted as-is. */
        STANDALONE
    }

    /**
     * A locator and what is needed to add it to the cached group or to start a group with it.
     */
    private record PendingLocator(Table table, Set<Integer> lobPositions, SelectLobLocatorEvent event, String transactionId, long transactionSequence) {
    }

    /**
     * The events of one statement's change to one row, in arrival order.
     */
    private static class Group {
        final Table table;
        final Set<Integer> lobPositions;
        final DmlEvent first;
        final String transactionId;
        final List<LogMinerEvent> events = new ArrayList<>();
        final Set<Integer> locatorPositions = new HashSet<>();
        final Set<Integer> endedXmlPositions = new HashSet<>();
        final Set<Integer> extendedStringPositions = new HashSet<>();

        long transactionSequence;
        boolean complete;
        boolean lobColumn;
        boolean xmlColumn;
        boolean extendedStringColumn;
        int openXmlPosition = -1;

        Group(Table table, Set<Integer> lobPositions, DmlEvent first, String transactionId, long transactionSequence) {
            this.table = table;
            this.lobPositions = lobPositions;
            this.first = first;
            this.transactionId = transactionId;
            add(first, transactionSequence);
        }

        void add(LogMinerEvent event, long transactionSequence) {
            events.add(event);
            // The sequence of the last event, so a restart skips every event of the group
            this.transactionSequence = transactionSequence;

            if (event instanceof SelectLobLocatorEvent locator) {
                locatorPositions.add(LogMinerHelper.getColumnIndexByName(locator.getColumnName(), table));
                lobColumn = true;
            }
            else if (event instanceof XmlBeginEvent begin) {
                xmlColumn = true;
                openXmlPosition = LogMinerHelper.getColumnIndexByName(begin.getColumnName(), table);
            }
            else if (event instanceof XmlEndEvent && openXmlPosition != -1) {
                endedXmlPositions.add(openXmlPosition);
                openXmlPosition = -1;
            }
            else if (event instanceof ExtendedStringBeginEvent begin) {
                extendedStringColumn = true;
                extendedStringPositions.add(LogMinerHelper.getColumnIndexByName(begin.getColumnName(), table));
            }
        }

        boolean accepts(LogMinerEvent fragment) {
            if (fragment instanceof LobWriteEvent) {
                return lobColumn;
            }
            if (fragment instanceof ExtendedStringWriteEvent) {
                return extendedStringColumn;
            }
            return xmlColumn;
        }

        boolean hasEndedXmlFor(XmlBeginEvent begin) {
            return endedXmlPositions.contains(LogMinerHelper.getColumnIndexByName(begin.getColumnName(), table));
        }

        boolean hasExtendedStringFor(ExtendedStringBeginEvent begin) {
            return extendedStringPositions.contains(LogMinerHelper.getColumnIndexByName(begin.getColumnName(), table));
        }

        boolean hasExtendedStringFor(Set<Integer> positions) {
            for (int position : positions) {
                if (extendedStringPositions.contains(position)) {
                    return true;
                }
            }
            return false;
        }

        boolean hasLocatorFor(SelectLobLocatorEvent locator) {
            return locatorPositions.contains(LogMinerHelper.getColumnIndexByName(locator.getColumnName(), table));
        }

        boolean hasLocatorFor(Set<Integer> positions) {
            for (int position : positions) {
                if (locatorPositions.contains(position)) {
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * A piece of a column value that LogMiner supplied in its own event.
     */
    static class Fragment {
        String data;

        Fragment() {
        }

        Fragment(String data) {
            this.data = data;
        }
    }

    /**
     * A column value that is assembled from the fragments of a group, and merged into the final value.
     */
    abstract static class AbstractUnderConstruction<T extends Fragment> {
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

    /**
     * A piece of a LOB value, text for a CLOB or bytes for a BLOB, and the offset it was written at.
     */
    static class LobFragment extends Fragment {
        boolean binary;
        byte[] bytes;
        int offset;

        LobFragment(final LobWriteEvent writeEvent) {
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
                    data = data + " ".repeat(gap) + other.data;
                }
                else {
                    data = data + other.data;
                }
            }
        }

    }

    /**
     * The value of a LOB column, assembled from fragments that can be written at any offset and in any order.
     */
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
                for (LobFragment frag : fragments) {
                    System.arraycopy(frag.bytes, 0, buffer, frag.offset, frag.bytes.length);
                }
                return buffer;
            }
            else {
                StringBuilder builder = new StringBuilder();
                int offset = 0;
                for (LobFragment frag : fragments) {
                    if (offset < frag.offset) { // fill the holes between fragments
                        builder.append(" ".repeat(frag.offset - offset));
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

        /**
         * Whether the fragments cover the value from its beginning up to at least the given length without a gap.
         */
        boolean coversFromStart(int length) {
            int covered = 0;
            for (LobFragment fragment : fragments) {
                if (fragment.offset > covered) {
                    break;
                }
                covered = Math.max(covered, fragment.end());
            }
            return covered >= length;
        }

        @Override
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
            return fromInitialValue(value, false);
        }

        // The binary flag states whether the locator that writes to the column is a binary one.
        static LobUnderConstruction fromInitialValue(Object value, boolean binary) {
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
                else if (binary && !isHexToRaw(strval)) {
                    // Text cannot be the content of a binary LOB, the writes supply the value
                    lob.binary = true;
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

        private static boolean isHexToRaw(String value) {
            return value.startsWith(OracleValueConverters.HEXTORAW_FUNCTION_START) && value.endsWith(OracleValueConverters.HEXTORAW_FUNCTION_END);
        }
    }

    /**
     * The value of an XML or extended string column, supplied as consecutive pieces of text.
     */
    static class TextUnderConstruction extends AbstractUnderConstruction<Fragment> {
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

    /**
     * Supplies the value of a LOB column that the events of the transaction do not, as the transaction left it.
     */
    @FunctionalInterface
    public interface LobValueResolver {
        /**
         * @param event the locator event of the write, which carries the row's {@code ROWID}
         * @return the value, or {@link OracleValueConverters#UNAVAILABLE_VALUE} if it cannot be supplied
         */
        Object resolve(Table table, Column column, DmlEvent event);
    }

    /**
     * Receives the events emitted by the consumer.
     */
    @FunctionalInterface
    public interface Handler<T> {
        void accept(T event, long eventIndex, String eventTrxId, long eventTrxSeq, long eventsProcessed) throws InterruptedException;
    }
}
