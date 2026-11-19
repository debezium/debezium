/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleDefaultValueConverter;
import io.debezium.connector.oracle.OracleTaskContext;
import io.debezium.connector.oracle.OracleValueConverters;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.StreamingAdapter;
import io.debezium.connector.oracle.jdbc.OracleDatabaseSettings;
import io.debezium.connector.oracle.logminer.events.DmlEvent;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.ExtendedStringBeginEvent;
import io.debezium.connector.oracle.logminer.events.ExtendedStringWriteEvent;
import io.debezium.connector.oracle.logminer.events.LobEraseEvent;
import io.debezium.connector.oracle.logminer.events.LobWriteEvent;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;
import io.debezium.connector.oracle.logminer.events.SelectLobLocatorEvent;
import io.debezium.connector.oracle.logminer.events.XmlBeginEvent;
import io.debezium.connector.oracle.logminer.events.XmlEndEvent;
import io.debezium.connector.oracle.logminer.events.XmlWriteEvent;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.doc.FixFor;
import io.debezium.relational.Column;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.schema.SchemaTopicNamingStrategy;
import io.debezium.spi.topic.TopicNamingStrategy;

import oracle.jdbc.OracleTypes;
import oracle.sql.CharacterSet;

/**
 * Unit tests for {@link StatementGroupCommitConsumer}.
 * <p>
 * The first tests are ported from {@link StatementGroupCommitConsumerTest}. The remaining tests cover the event
 * shapes LogMiner supplies for a single statement, as captured for debezium/dbz#3100.
 *
 * @author Chris Cranford
 */
public class StatementGroupCommitConsumerTest {

    private static final TableId TABLE_ID = TableId.parse("ORCLPDB1.DEBEZIUM.TEST_XML");
    private static final TableId PLAIN_TABLE_ID = TableId.parse("ORCLPDB1.DEBEZIUM.TEST_PLAIN");
    private static final TableId EXTENDED_TABLE_ID = TableId.parse("ORCLPDB1.DEBEZIUM.TEST_EXTENDED");
    private static final TableId JSON_TABLE_ID = TableId.parse("ORCLPDB1.DEBEZIUM.TEST_JSON");
    private static final String ROW_ID = "AAAAAAAAAAAAAAAAAB";
    private static final String OTHER_ROW_ID = "AAAAAAAAAAAAAAAAAC";
    private static final String TRANSACTION_ID = "1234567890";
    private static final String XML_COLUMN = "XMLRECORD";
    private static final String CLOB_COLUMN = "DOC";
    private static final String EXTENDED_STRING_COLUMN = "DATA";
    private static final String JSON_COLUMN = "DOC";
    private static final int DATA_INDEX = 1;
    private static final int XML_INDEX = 2;
    private static final int DOC_INDEX = 3;

    private OracleConnectorConfig connectorConfig;
    private OracleDatabaseSchema schema;
    private List<LogMinerEvent> dispatched;
    private List<Long> dispatchedSequences;
    private long scn;

    @BeforeEach
    void before() {
        // A database connection is required by the value converters; a mock is used as these tests
        // only exercise the in-memory merge logic and never access the database.
        final OracleConnection connection = Mockito.mock(OracleConnection.class);
        Mockito.when(connection.getNationalCharacterSet()).thenReturn(CharacterSet.make(CharacterSet.UTF8_CHARSET));
        Mockito.when(connection.getDatabaseCharacterSet()).thenReturn(CharacterSet.make(CharacterSet.AL32UTF8_CHARSET));

        final Configuration configuration = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.LOB_ENABLED, true)
                .build();

        connectorConfig = new OracleConnectorConfig(configuration);
        schema = createSchema(configuration, connectorConfig, connection);
        dispatched = new ArrayList<>();
        dispatchedSequences = new ArrayList<>();
        scn = 1;
    }

    @AfterEach
    void after() {
        if (schema != null) {
            schema.close();
        }
    }

    @Test
    @FixFor({ "debezium/dbz#2159", "debezium/dbz#3100" })
    public void shouldDispatchSecondXmlDocumentOfRowAsSeparateEvent() throws Exception {
        try (StatementGroupCommitConsumer consumer = createConsumer()) {
            accept(consumer, insert());
            accept(consumer, xmlBegin());
            accept(consumer, xmlWrite("<row>a</row>"));
            accept(consumer, xmlEnd());
            // A statement writes one document per column, so this one belongs to a later update
            accept(consumer, xmlBegin());
            accept(consumer, xmlWrite("<row>b</row>"));
            accept(consumer, xmlEnd());
        }

        assertThat(dispatched).extracting(LogMinerEvent::getEventType).containsExactly(EventType.INSERT, EventType.XML_BEGIN);
        assertThat(((DmlEvent) dispatched.get(0)).getNewValues()[XML_INDEX]).isEqualTo("<row>a</row>");
        assertThat(((DmlEvent) dispatched.get(1)).getNewValues()[XML_INDEX]).isEqualTo("<row>b</row>");
    }

    @Test
    @FixFor("debezium/dbz#3100")
    public void shouldDispatchXmlDocumentSetToNullAsSeparateEvent() throws Exception {
        try (StatementGroupCommitConsumer consumer = createConsumer()) {
            accept(consumer, insert());
            accept(consumer, xmlBegin());
            accept(consumer, xmlWrite("<row>a</row>"));
            accept(consumer, xmlEnd());
            accept(consumer, xmlBegin());
            accept(consumer, xmlWrite(null));
            accept(consumer, xmlEnd());
        }

        assertThat(dispatched).extracting(LogMinerEvent::getEventType).containsExactly(EventType.INSERT, EventType.XML_BEGIN);
        assertThat(((DmlEvent) dispatched.get(0)).getNewValues()[XML_INDEX]).isEqualTo("<row>a</row>");
        assertThat(((DmlEvent) dispatched.get(1)).getNewValues()[XML_INDEX]).isNull();
    }

    @Test
    @FixFor("debezium/dbz#2159")
    public void shouldConcatenateMultipleXmlWritesOfSingleDocument() throws Exception {
        try (StatementGroupCommitConsumer consumer = createConsumer()) {
            accept(consumer, insert());
            accept(consumer, xmlBegin());
            accept(consumer, xmlWrite("<row>"));
            accept(consumer, xmlWrite("<c1>value1</c1>"));
            accept(consumer, xmlWrite("</row>"));
            accept(consumer, xmlEnd());
        }

        assertSingleEvent(EventType.INSERT, "<row><c1>value1</c1></row>");
    }

    @Test
    @FixFor("debezium/dbz#2159")
    public void shouldMergeUpdateOfScalarAndXmlColumnInSameStatement() throws Exception {
        try (StatementGroupCommitConsumer consumer = createConsumer()) {
            accept(consumer, update("updated"));
            accept(consumer, xmlBegin());
            accept(consumer, xmlWrite("<row>"));
            accept(consumer, xmlWrite("b</row>"));
            accept(consumer, xmlEnd());
        }

        assertSingleEvent(EventType.UPDATE, "<row>b</row>");
        assertThat(((DmlEvent) dispatched.get(0)).getNewValues()[DATA_INDEX]).isEqualTo("updated");
    }

    @Test
    @FixFor("debezium/dbz#2159")
    public void shouldIgnoreXmlWriteWhenRowMergeStateWasDiscarded() throws Exception {
        // LOB_ERASE is not supported, so the merged INSERT for the row is dropped on purpose. The test only
        // checks that the trailing write for the discarded row is ignored rather than failing.
        try (StatementGroupCommitConsumer consumer = createConsumer()) {
            accept(consumer, insert());
            accept(consumer, xmlBegin());
            accept(consumer, xmlWrite("<row>a</row>"));
            accept(consumer, selectClobLocator());
            accept(consumer, lobErase());
            accept(consumer, xmlWrite("<row>b</row>"));
            accept(consumer, xmlEnd());
        }

        assertThat(dispatched).isEmpty();
    }

    @Test
    @FixFor("debezium/dbz#2159")
    public void shouldIgnoreExtendedStringWriteWhenRowMergeStateWasDiscarded() throws Exception {
        // LOB_ERASE is not supported, so the merged INSERT for the row is dropped on purpose. The test only
        // checks that the trailing write for the discarded row is ignored rather than failing.
        try (StatementGroupCommitConsumer consumer = createConsumer()) {
            accept(consumer, insert());
            accept(consumer, extendedStringBegin());
            accept(consumer, extendedStringWrite("a"));
            accept(consumer, selectClobLocator());
            accept(consumer, lobErase());
            accept(consumer, extendedStringWrite("b"));
        }

        assertThat(dispatched).isEmpty();
    }

    @Test
    @FixFor("debezium/dbz#2159")
    public void testTrailingXmlWriteAfterNonMergeableDelete() throws Exception {
        try (StatementGroupCommitConsumer consumer = createConsumer()) {
            accept(consumer, insert());
            accept(consumer, xmlBegin());
            accept(consumer, xmlWrite("<row>a</row>"));
            accept(consumer, selectClobLocator());
            accept(consumer, delete());
            accept(consumer, xmlWrite("<row>b</row>"));
            accept(consumer, xmlEnd());
        }
        assertThat(dispatched).hasSize(2);
    }

    @Test
    @FixFor("debezium/dbz#2159")
    public void testTrailingXmlWriteAfterNonMergeableUpdate() throws Exception {
        try (StatementGroupCommitConsumer consumer = createConsumer()) {
            accept(consumer, update("a"));
            accept(consumer, xmlBegin());
            accept(consumer, xmlWrite("<row>a</row>"));
            accept(consumer, selectClobLocator());
            accept(consumer, update("b"));
            accept(consumer, xmlWrite("<row>b</row>"));
            accept(consumer, xmlEnd());
        }
        assertThat(dispatched).hasSize(2);
    }

    @Test
    @FixFor("debezium/dbz#3100")
    public void shouldDispatchEventsInArrivalOrderWhenLobRowIsHeld() throws Exception {
        try (StatementGroupCommitConsumer consumer = createConsumer()) {
            accept(consumer, insert());
            accept(consumer, plainInsert());
        }

        assertThat(dispatched).hasSize(2);
        assertThat(dispatched).extracting(LogMinerEvent::getTableId).containsExactly(TABLE_ID, PLAIN_TABLE_ID);
        assertThat(dispatched.get(0).getScn()).isLessThan(dispatched.get(1).getScn());
    }

    @Test
    @FixFor("debezium/dbz#3100")
    public void shouldDispatchEventsInArrivalOrderWhenAnotherLobRowIsReleasedEarly() throws Exception {
        final DmlEvent firstRowInsert = insert();
        final DmlEvent secondRowInsert = insert("test4");
        final DmlEvent secondRowDelete = delete("test4");

        try (StatementGroupCommitConsumer consumer = createConsumer()) {
            accept(consumer, firstRowInsert);
            accept(consumer, secondRowInsert);
            // Cannot merge into the held insert, so the second row's insert is released
            accept(consumer, secondRowDelete);
        }

        assertThat(dispatched).containsExactly(firstRowInsert, secondRowInsert, secondRowDelete);
    }

    @Test
    @FixFor("debezium/dbz#3100")
    public void shouldDispatchLaterUpdateOfRowAsSeparateEventWhenAnotherRowChangedInBetween() throws Exception {
        final DmlEvent firstRowInsert = insert();
        final DmlEvent secondRowInsert = insert("test4");
        final DmlEvent firstRowUpdate = update("updated");

        try (StatementGroupCommitConsumer consumer = createConsumer()) {
            accept(consumer, firstRowInsert);
            // A change for another row ends the first row's statement
            accept(consumer, secondRowInsert);
            accept(consumer, firstRowUpdate);
        }

        assertThat(dispatched).containsExactly(firstRowInsert, secondRowInsert, firstRowUpdate);
        assertThat(dispatchedSequences).isSorted();
    }

    @Test
    @FixFor("debezium/dbz#3100")
    public void shouldDispatchInlineLobUpdateSeparatelyWhenInsertWasAlreadyFilled() throws Exception {
        final Object unavailable = OracleValueConverters.UNAVAILABLE_VALUE;
        try (StatementGroupCommitConsumer consumer = createConsumer()) {
            accept(consumer, insert());
            // The in-line value of the insert, which completes the statement
            accept(consumer, lobUpdate(unavailable, "insert value"));
            accept(consumer, lobUpdate(unavailable, "update value"));
        }

        assertThat(dispatched).extracting(LogMinerEvent::getEventType).containsExactly(EventType.INSERT, EventType.UPDATE);
        assertThat(((DmlEvent) dispatched.get(0)).getNewValues()[DOC_INDEX]).isEqualTo("insert value");
        assertThat(((DmlEvent) dispatched.get(1)).getNewValues()[DOC_INDEX]).isEqualTo("update value");
        assertThat(dispatchedSequences).isSorted();
    }

    @Test
    @FixFor("debezium/dbz#3100")
    public void shouldMergeLocatorWritesAndTrailingInlineLobUpdateIntoInsert() throws Exception {
        try (StatementGroupCommitConsumer consumer = createConsumer()) {
            accept(consumer, insert());
            accept(consumer, selectClobLocator());
            accept(consumer, lobWrite("abc", 0));
            accept(consumer, lobWrite("def", 3));
            accept(consumer, lobUpdate("<x/>", OracleValueConverters.UNAVAILABLE_VALUE));
        }

        assertSingleEvent(EventType.INSERT, "<x/>");
        assertThat(((DmlEvent) dispatched.get(0)).getNewValues()[DOC_INDEX]).isEqualTo("abcdef");
    }

    @Test
    @FixFor("debezium/dbz#3100")
    public void shouldDispatchInlineUpdateOfColumnWrittenThroughLocatorSeparately() throws Exception {
        try (StatementGroupCommitConsumer consumer = createConsumer()) {
            accept(consumer, insert());
            accept(consumer, selectClobLocator());
            accept(consumer, lobWrite("abc", 0));
            // A statement writes a column once, so this is a second statement
            accept(consumer, lobUpdate(OracleValueConverters.UNAVAILABLE_VALUE, "small"));
        }

        assertThat(dispatched).extracting(LogMinerEvent::getEventType).containsExactly(EventType.INSERT, EventType.UPDATE);
        assertThat(((DmlEvent) dispatched.get(0)).getNewValues()[DOC_INDEX]).isEqualTo("abc");
        assertThat(((DmlEvent) dispatched.get(1)).getNewValues()[DOC_INDEX]).isEqualTo("small");
    }

    @Test
    @FixFor("debezium/dbz#3100")
    public void shouldMergePlaceholderUpdateAndItsInlineLobUpdate() throws Exception {
        final Object unavailable = OracleValueConverters.UNAVAILABLE_VALUE;
        try (StatementGroupCommitConsumer consumer = createConsumer()) {
            accept(consumer, lobUpdate(unavailable, OracleValueConverters.EMPTY_CLOB_FUNCTION));
            accept(consumer, lobUpdate(unavailable, "value"));
        }

        assertThat(dispatched).hasSize(1);
        assertThat(dispatched.get(0).getEventType()).isEqualTo(EventType.UPDATE);
        assertThat(((DmlEvent) dispatched.get(0)).getNewValues()[DOC_INDEX]).isEqualTo("value");
    }

    @Test
    @FixFor("debezium/dbz#3100")
    public void shouldAssembleLocatorWritesThatDirectlyFollowOneAnother() throws Exception {
        try (StatementGroupCommitConsumer consumer = createConsumer()) {
            accept(consumer, insert());
            accept(consumer, selectClobLocator());
            accept(consumer, lobWrite("abc", 0));
            accept(consumer, selectClobLocator());
            accept(consumer, lobWrite("def", 3));
        }

        assertThat(dispatched).extracting(LogMinerEvent::getEventType).containsExactly(EventType.INSERT);
        assertThat(((DmlEvent) dispatched.get(0)).getNewValues()[DOC_INDEX]).isEqualTo("abcdef");
    }

    @Test
    @FixFor("debezium/dbz#3100")
    public void shouldEmitUnavailableValueForLocatorWriteSeparatedByAnotherTableChange() throws Exception {
        final DmlEvent plain = plainInsert("p1");
        final List<LogMinerEvent> beforePlain = new ArrayList<>();
        try (StatementGroupCommitConsumer consumer = createConsumer()) {
            accept(consumer, insert());
            accept(consumer, selectClobLocator());
            accept(consumer, lobWrite("abc", 0));
            accept(consumer, plain);
            // Nothing is held back once another change arrives
            beforePlain.addAll(dispatched);
            accept(consumer, selectClobLocator());
            accept(consumer, lobWrite("def", 3));
        }

        assertThat(beforePlain).extracting(LogMinerEvent::getEventType).containsExactly(EventType.INSERT, EventType.INSERT);
        assertThat(dispatched).extracting(LogMinerEvent::getEventType)
                .containsExactly(EventType.INSERT, EventType.INSERT, EventType.SELECT_LOB_LOCATOR);
        assertThat(((DmlEvent) dispatched.get(0)).getNewValues()[DOC_INDEX]).isEqualTo("abc");
        assertThat(dispatched.get(1).getTableId()).isEqualTo(PLAIN_TABLE_ID);
        // The later piece alone is not the column's value
        assertThat(((DmlEvent) dispatched.get(2)).getNewValues()[DOC_INDEX]).isEqualTo(OracleValueConverters.UNAVAILABLE_VALUE);
        assertThat(dispatchedSequences).isSorted();
    }

    @Test
    @FixFor("debezium/dbz#3100")
    public void shouldEmitUnavailableValueForLocatorWritesThatAlternateBetweenRows() throws Exception {
        try (StatementGroupCommitConsumer consumer = createConsumer()) {
            accept(consumer, insert());
            accept(consumer, insert("test4"));
            accept(consumer, selectClobLocator("test3", ROW_ID));
            accept(consumer, lobWrite("abc", 0));
            accept(consumer, selectClobLocator("test4", OTHER_ROW_ID));
            accept(consumer, lobWrite("uvw", 0));
            accept(consumer, selectClobLocator("test3", ROW_ID));
            accept(consumer, lobWrite("def", 3));
            accept(consumer, selectClobLocator("test4", OTHER_ROW_ID));
            accept(consumer, lobWrite("xyz", 3));
        }

        assertThat(dispatched).extracting(LogMinerEvent::getEventType)
                .containsExactly(EventType.INSERT, EventType.INSERT, EventType.SELECT_LOB_LOCATOR, EventType.SELECT_LOB_LOCATOR,
                        EventType.SELECT_LOB_LOCATOR, EventType.SELECT_LOB_LOCATOR);
        assertThat(dispatched).extracting(e -> ((DmlEvent) e).getNewValues()[0])
                .containsExactly("test3", "test4", "test3", "test4", "test3", "test4");
        // The other row's change is in between each time, so what the column held before a write is not known
        for (int i = 2; i < 6; i++) {
            assertThat(((DmlEvent) dispatched.get(i)).getNewValues()[DOC_INDEX]).isEqualTo(OracleValueConverters.UNAVAILABLE_VALUE);
        }
        assertThat(dispatchedSequences).isSorted();
    }

    @Test
    @FixFor("debezium/dbz#3100")
    public void shouldAssembleLocatorWriteThatReplacesValueOfPrecedingStatementOnSameRow() throws Exception {
        try (StatementGroupCommitConsumer consumer = createConsumer()) {
            accept(consumer, lobUpdate(OracleValueConverters.UNAVAILABLE_VALUE, "abc"));
            // The value moves out of line, the locator write starts over from the beginning
            accept(consumer, selectClobLocator());
            accept(consumer, lobWrite("abc", 0));
            accept(consumer, lobWrite("def", 3));
        }

        assertThat(dispatched).extracting(LogMinerEvent::getEventType).containsExactly(EventType.UPDATE, EventType.SELECT_LOB_LOCATOR);
        assertThat(((DmlEvent) dispatched.get(0)).getNewValues()[DOC_INDEX]).isEqualTo("abc");
        assertThat(((DmlEvent) dispatched.get(1)).getNewValues()[DOC_INDEX]).isEqualTo("abcdef");
    }

    @Test
    @FixFor("debezium/dbz#3100")
    public void shouldEmitUnavailableValueForLocatorWriteThatDoesNotReplaceValueOfPrecedingStatement() throws Exception {
        try (StatementGroupCommitConsumer consumer = createConsumer()) {
            accept(consumer, lobUpdate(OracleValueConverters.UNAVAILABLE_VALUE, "abcdef"));
            // Only the first two characters are overwritten, the rest of the value is not supplied
            accept(consumer, selectClobLocator());
            accept(consumer, lobWrite("xy", 0));
        }

        assertThat(dispatched).extracting(LogMinerEvent::getEventType).containsExactly(EventType.UPDATE, EventType.SELECT_LOB_LOCATOR);
        assertThat(((DmlEvent) dispatched.get(1)).getNewValues()[DOC_INDEX]).isEqualTo(OracleValueConverters.UNAVAILABLE_VALUE);
    }

    @Test
    @FixFor("debezium/dbz#3100")
    public void shouldEmitUnavailableValueForLocatorWriteToValueFromBeforeTheTransaction() throws Exception {
        try (StatementGroupCommitConsumer consumer = createConsumer()) {
            // A write from the beginning, but how much the column held before is not known
            accept(consumer, selectClobLocator());
            accept(consumer, lobWrite("abc", 0));
        }

        assertThat(dispatched).extracting(LogMinerEvent::getEventType).containsExactly(EventType.SELECT_LOB_LOCATOR);
        assertThat(((DmlEvent) dispatched.get(0)).getNewValues()[DOC_INDEX]).isEqualTo(OracleValueConverters.UNAVAILABLE_VALUE);
    }

    @Test
    @FixFor("debezium/dbz#3100")
    public void shouldAssembleLocatorWriteWithGapWhenRowWasInsertedBefore() throws Exception {
        try (StatementGroupCommitConsumer consumer = createConsumer()) {
            accept(consumer, insert());
            accept(consumer, lobUpdate(OracleValueConverters.UNAVAILABLE_VALUE, "ab"));
            // Writing past the end of the value pads it with spaces
            accept(consumer, selectClobLocator());
            accept(consumer, lobWrite("ab", 0));
            accept(consumer, lobWrite("yz", 5));
        }

        assertThat(dispatched).extracting(LogMinerEvent::getEventType).containsExactly(EventType.INSERT, EventType.SELECT_LOB_LOCATOR);
        assertThat(((DmlEvent) dispatched.get(0)).getNewValues()[DOC_INDEX]).isEqualTo("ab");
        assertThat(((DmlEvent) dispatched.get(1)).getNewValues()[DOC_INDEX]).isEqualTo("ab   yz");
    }

    @Test
    @FixFor("debezium/dbz#3100")
    public void shouldDispatchSecondExtendedStringOfRowAsSeparateEvent() throws Exception {
        try (StatementGroupCommitConsumer consumer = createConsumer()) {
            accept(consumer, insert());
            accept(consumer, extendedStringBegin());
            accept(consumer, extendedStringWrite("first"));
            // A statement writes the column once, so this value belongs to a later update
            accept(consumer, extendedStringBegin());
            accept(consumer, extendedStringWrite("second"));
        }

        assertThat(dispatched).extracting(LogMinerEvent::getEventType).containsExactly(EventType.INSERT, EventType.EXTENDED_STRING_BEGIN);
        assertThat(((DmlEvent) dispatched.get(0)).getNewValues()[DATA_INDEX]).isEqualTo("first");
        assertThat(((DmlEvent) dispatched.get(1)).getNewValues()[DATA_INDEX]).isEqualTo("second");
    }

    @Test
    @FixFor("debezium/dbz#3100")
    public void shouldDispatchExtendedStringAfterInlineValueOfInsertAsSeparateEvent() throws Exception {
        try (StatementGroupCommitConsumer consumer = createConsumer()) {
            accept(consumer, extendedInsert());
            // The in-line value of the insert, which completes the statement
            accept(consumer, extendedInlineUpdate("small"));
            // An update to a large value has no row event of its own
            accept(consumer, extendedBegin());
            accept(consumer, extendedWrite("large"));
        }

        assertThat(dispatched).extracting(LogMinerEvent::getEventType).containsExactly(EventType.INSERT, EventType.EXTENDED_STRING_BEGIN);
        assertThat(((DmlEvent) dispatched.get(0)).getNewValues()[DATA_INDEX]).isEqualTo("small");
        assertThat(((DmlEvent) dispatched.get(1)).getNewValues()[DATA_INDEX]).isEqualTo("large");
        assertThat(dispatchedSequences).isSorted();
    }

    @Test
    @FixFor("debezium/dbz#3100")
    public void shouldDispatchInlineUpdateOfExtendedStringWrittenByInsertAsSeparateEvent() throws Exception {
        try (StatementGroupCommitConsumer consumer = createConsumer()) {
            accept(consumer, extendedInsert());
            accept(consumer, extendedBegin());
            accept(consumer, extendedWrite("large"));
            // The insert already supplied the column, so this is a later update to a small value
            accept(consumer, extendedInlineUpdate("small"));
        }

        assertThat(dispatched).extracting(LogMinerEvent::getEventType).containsExactly(EventType.INSERT, EventType.UPDATE);
        assertThat(((DmlEvent) dispatched.get(0)).getNewValues()[DATA_INDEX]).isEqualTo("large");
        assertThat(((DmlEvent) dispatched.get(1)).getNewValues()[DATA_INDEX]).isEqualTo("small");
        assertThat(dispatchedSequences).isSorted();
    }

    @Test
    @FixFor("debezium/dbz#3100")
    public void shouldDispatchSecondStatementLocatorOfColumnAsSeparateEvent() throws Exception {
        final String statementRowId = "AAAAAAAAAAAAAAAAAA";
        try (StatementGroupCommitConsumer consumer = createConsumer()) {
            accept(consumer, insert());
            accept(consumer, selectClobLocator("test3", statementRowId));
            accept(consumer, lobWrite("insert value", 0));
            // An update of only the out-of-line value has no row event of its own, only another locator
            accept(consumer, selectClobLocator("test3", statementRowId));
            accept(consumer, lobWrite("update value", 0));
        }

        assertThat(dispatched).extracting(LogMinerEvent::getEventType).containsExactly(EventType.INSERT, EventType.SELECT_LOB_LOCATOR);
        assertThat(((DmlEvent) dispatched.get(0)).getNewValues()[DOC_INDEX]).isEqualTo("insert value");
        assertThat(((DmlEvent) dispatched.get(1)).getNewValues()[DOC_INDEX]).isEqualTo("update value");
        assertThat(dispatchedSequences).isSorted();
    }

    @Test
    @FixFor("debezium/dbz#3100")
    public void shouldKeepSecondStatementLocatorOfColumnThatIsNotWrittenTo() throws Exception {
        final String statementRowId = "AAAAAAAAAAAAAAAAAA";
        try (StatementGroupCommitConsumer consumer = createConsumer()) {
            accept(consumer, insert());
            accept(consumer, selectClobLocator("test3", statementRowId));
            accept(consumer, lobWrite("insert value", 0));
            // With BasicFile storage the trim of a statement has a locator of its own, and no writes
            accept(consumer, selectClobLocator("test3", statementRowId));
            accept(consumer, plainInsert());
        }

        assertThat(dispatched).extracting(LogMinerEvent::getEventType).containsExactly(EventType.INSERT, EventType.INSERT);
        assertThat(((DmlEvent) dispatched.get(0)).getNewValues()[DOC_INDEX]).isEqualTo("insert value");
        assertThat(dispatchedSequences).isSorted();
    }

    @Test
    @FixFor("debezium/dbz#3100")
    public void shouldKeepSecondStatementLocatorOfColumnAtEndOfTransaction() throws Exception {
        final String statementRowId = "AAAAAAAAAAAAAAAAAA";
        try (StatementGroupCommitConsumer consumer = createConsumer()) {
            accept(consumer, insert());
            accept(consumer, selectClobLocator("test3", statementRowId));
            accept(consumer, lobWrite("insert value", 0));
            accept(consumer, selectClobLocator("test3", statementRowId));
        }

        assertSingleEvent(EventType.INSERT, null);
        assertThat(((DmlEvent) dispatched.get(0)).getNewValues()[DOC_INDEX]).isEqualTo("insert value");
    }

    @Test
    @FixFor("debezium/dbz#3100")
    public void shouldDispatchSecondJsonLocatorOfRowAsSeparateEvent() throws Exception {
        try (StatementGroupCommitConsumer consumer = createConsumer()) {
            accept(consumer, new DmlEvent(EventType.INSERT, nextScn(), JSON_TABLE_ID, ROW_ID, "rsId", Instant.now(),
                    new Object[0], new Object[]{ "test3", "/* JSON */ EMPTY_BLOB()" }));
            accept(consumer, jsonLocator());
            accept(consumer, jsonWrite("HEXTORAW('0102')"));
            // An update of the document has no row event of its own, only another locator
            accept(consumer, jsonLocator());
            accept(consumer, jsonWrite("HEXTORAW('0304')"));
        }

        assertThat(dispatched).extracting(LogMinerEvent::getEventType).containsExactly(EventType.INSERT, EventType.SELECT_LOB_LOCATOR);
        assertThat(((DmlEvent) dispatched.get(0)).getNewValues()[DATA_INDEX]).isEqualTo(new byte[]{ 1, 2 });
        assertThat(((DmlEvent) dispatched.get(1)).getNewValues()[DATA_INDEX]).isEqualTo(new byte[]{ 3, 4 });
        assertThat(dispatchedSequences).isSorted();
    }

    @Test
    @FixFor("debezium/dbz#3100")
    public void shouldIgnoreLobWritesWhenValueOfLocatorColumnIsMalformed() throws Exception {
        final String malformed = "HEXTORAW('ZZ')";
        try (StatementGroupCommitConsumer consumer = createConsumer()) {
            accept(consumer, new DmlEvent(EventType.INSERT, nextScn(), JSON_TABLE_ID, ROW_ID, "rsId", Instant.now(),
                    new Object[0], new Object[]{ "test3", malformed }));
            // The column's value cannot be started from the malformed literal, so the writes have nowhere to go
            accept(consumer, jsonLocator());
            accept(consumer, jsonWrite("HEXTORAW('0102')"));
        }

        assertThat(dispatched).extracting(LogMinerEvent::getEventType).containsExactly(EventType.INSERT);
        assertThat(((DmlEvent) dispatched.get(0)).getNewValues()[DATA_INDEX]).isEqualTo(malformed);
    }

    private DmlEvent delete() {
        return new DmlEvent(EventType.DELETE, nextScn(), TABLE_ID, ROW_ID, "rsId", Instant.now(),
                new Object[]{ "test3", "data", null, null }, new Object[0]);
    }

    private void assertSingleEvent(EventType eventType, String expectedXml) {
        assertThat(dispatched).hasSize(1);
        final DmlEvent event = (DmlEvent) dispatched.get(0);
        assertThat(event.getEventType()).isEqualTo(eventType);
        assertThat(event.getNewValues()[XML_INDEX]).isEqualTo(expectedXml);
    }

    private StatementGroupCommitConsumer createConsumer() {
        return createConsumer((table, column, event) -> OracleValueConverters.UNAVAILABLE_VALUE);
    }

    private StatementGroupCommitConsumer createConsumer(StatementGroupCommitConsumer.LobValueResolver lobValueResolver) {
        return new StatementGroupCommitConsumer(
                (event, eventIndex, eventTrxId, eventTrxSeq, eventsProcessed) -> {
                    dispatched.add(event);
                    dispatchedSequences.add(eventTrxSeq);
                },
                connectorConfig,
                schema,
                lobValueResolver);
    }

    private void accept(StatementGroupCommitConsumer consumer, LogMinerEvent event) throws InterruptedException {
        consumer.accept(event, TRANSACTION_ID, scn);
    }

    private DmlEvent insert() {
        return new DmlEvent(EventType.INSERT, nextScn(), TABLE_ID, ROW_ID, "rsId", Instant.now(),
                new Object[0], new Object[]{ "test3", "data", null, null });
    }

    private DmlEvent insert(String recId) {
        return new DmlEvent(EventType.INSERT, nextScn(), TABLE_ID, OTHER_ROW_ID, "rsId", Instant.now(),
                new Object[0], new Object[]{ recId, "data", null, null });
    }

    private DmlEvent delete(String recId) {
        return new DmlEvent(EventType.DELETE, nextScn(), TABLE_ID, OTHER_ROW_ID, "rsId", Instant.now(),
                new Object[]{ recId, "data", null, null }, new Object[0]);
    }

    private DmlEvent plainInsert() {
        return plainInsert("test3");
    }

    private DmlEvent plainInsert(String recId) {
        return new DmlEvent(EventType.INSERT, nextScn(), PLAIN_TABLE_ID, OTHER_ROW_ID, "rsId", Instant.now(),
                new Object[0], new Object[]{ recId, "data" });
    }

    private SelectLobLocatorEvent selectClobLocator(String recId, String rowId) {
        return new SelectLobLocatorEvent(EventType.SELECT_LOB_LOCATOR, nextScn(), TABLE_ID, rowId, "rsId", Instant.now(),
                new Object[]{ recId, "data", null, null }, new Object[]{ recId, "data", null, null }, CLOB_COLUMN, false);
    }

    private DmlEvent update(String data) {
        return new DmlEvent(EventType.UPDATE, nextScn(), TABLE_ID, ROW_ID, "rsId", Instant.now(),
                new Object[]{ "test3", "data", OracleValueConverters.UNAVAILABLE_VALUE, OracleValueConverters.UNAVAILABLE_VALUE },
                new Object[]{ "test3", data, OracleValueConverters.UNAVAILABLE_VALUE, OracleValueConverters.UNAVAILABLE_VALUE });
    }

    private DmlEvent lobUpdate(Object xml, Object doc) {
        return new DmlEvent(EventType.UPDATE, nextScn(), TABLE_ID, ROW_ID, "rsId", Instant.now(),
                new Object[]{ "test3", "data", OracleValueConverters.UNAVAILABLE_VALUE, OracleValueConverters.UNAVAILABLE_VALUE },
                new Object[]{ "test3", "data", xml, doc });
    }

    private LobWriteEvent lobWrite(String data, int offset) {
        return new LobWriteEvent(EventType.LOB_WRITE, nextScn(), TABLE_ID, ROW_ID, "rsId", Instant.now(), data, offset, data.length());
    }

    private XmlBeginEvent xmlBegin() {
        return new XmlBeginEvent(EventType.XML_BEGIN, nextScn(), TABLE_ID, ROW_ID, "rsId", Instant.now(),
                new Object[]{ "test3", "data", null, null }, new Object[]{ "test3", "data", null, null }, XML_COLUMN, scn);
    }

    private ExtendedStringBeginEvent extendedStringBegin() {
        return new ExtendedStringBeginEvent(EventType.EXTENDED_STRING_BEGIN, nextScn(), TABLE_ID, ROW_ID, "rsId", Instant.now(),
                new Object[]{ "test3", null, null, null }, new Object[]{ "test3", null, null, null }, EXTENDED_STRING_COLUMN);
    }

    private ExtendedStringWriteEvent extendedStringWrite(String data) {
        return new ExtendedStringWriteEvent(EventType.EXTENDED_STRING_WRITE, nextScn(), TABLE_ID, ROW_ID, "rsId", Instant.now(), data);
    }

    private DmlEvent extendedInsert() {
        return new DmlEvent(EventType.INSERT, nextScn(), EXTENDED_TABLE_ID, ROW_ID, "rsId", Instant.now(),
                new Object[0], new Object[]{ "test3", null });
    }

    private DmlEvent extendedInlineUpdate(String data) {
        return new DmlEvent(EventType.UPDATE, nextScn(), EXTENDED_TABLE_ID, ROW_ID, "rsId", Instant.now(),
                new Object[]{ "test3", OracleValueConverters.UNAVAILABLE_VALUE }, new Object[]{ "test3", data });
    }

    private ExtendedStringBeginEvent extendedBegin() {
        return new ExtendedStringBeginEvent(EventType.EXTENDED_STRING_BEGIN, nextScn(), EXTENDED_TABLE_ID, ROW_ID, "rsId", Instant.now(),
                new Object[]{ "test3", null }, new Object[]{ "test3", null }, EXTENDED_STRING_COLUMN);
    }

    private ExtendedStringWriteEvent extendedWrite(String data) {
        return new ExtendedStringWriteEvent(EventType.EXTENDED_STRING_WRITE, nextScn(), EXTENDED_TABLE_ID, ROW_ID, "rsId", Instant.now(), data);
    }

    private SelectLobLocatorEvent jsonLocator() {
        // The locator of a statement has no row id
        return new SelectLobLocatorEvent(EventType.SELECT_LOB_LOCATOR, nextScn(), JSON_TABLE_ID, "AAAAAAAAAAAAAAAAAA", "rsId", Instant.now(),
                new Object[]{ "test3", null }, new Object[]{ "test3", null }, JSON_COLUMN, true);
    }

    private LobWriteEvent jsonWrite(String data) {
        return new LobWriteEvent(EventType.LOB_WRITE, nextScn(), JSON_TABLE_ID, "AAAAAAAAAAAAAAAAAA", "rsId", Instant.now(), data, 0, 2);
    }

    private SelectLobLocatorEvent selectClobLocator() {
        return new SelectLobLocatorEvent(EventType.SELECT_LOB_LOCATOR, nextScn(), TABLE_ID, ROW_ID, "rsId", Instant.now(),
                new Object[]{ "test3", "data", null, null }, new Object[]{ "test3", "data", null, null }, CLOB_COLUMN, false);
    }

    private LobEraseEvent lobErase() {
        return new LobEraseEvent(EventType.LOB_ERASE, nextScn(), TABLE_ID, ROW_ID, "rsId", Instant.now());
    }

    private XmlWriteEvent xmlWrite(String xml) {
        return new XmlWriteEvent(EventType.XML_WRITE, nextScn(), TABLE_ID, ROW_ID, "rsId", Instant.now(), xml, xml == null ? 0 : xml.length());
    }

    private XmlEndEvent xmlEnd() {
        return new XmlEndEvent(EventType.XML_END, nextScn(), TABLE_ID, ROW_ID, "rsId", Instant.now(), scn);
    }

    private Scn nextScn() {
        return Scn.valueOf(++scn);
    }

    private static OracleDatabaseSchema createSchema(Configuration configuration, OracleConnectorConfig connectorConfig, OracleConnection connection) {
        final TopicNamingStrategy topicNamingStrategy = SchemaTopicNamingStrategy.create(connectorConfig);
        final SchemaNameAdjuster schemaNameAdjuster = connectorConfig.schemaNameAdjuster();
        final OracleValueConverters converters = connectorConfig.getAdapter().getValueConverter(connectorConfig, connection,
                new OracleDatabaseSettings(CharacterSet.make(CharacterSet.AL32UTF8_CHARSET), CharacterSet.make(CharacterSet.UTF8_CHARSET), true, false));
        final OracleDefaultValueConverter defaultValueConverter = new OracleDefaultValueConverter(converters, connection);

        final OracleDatabaseSchema schema = new OracleDatabaseSchema(connectorConfig,
                converters,
                defaultValueConverter,
                schemaNameAdjuster,
                topicNamingStrategy,
                StreamingAdapter.TableNameCaseSensitivity.INSENSITIVE,
                new CustomConverterRegistry(List.of()),
                new OracleTaskContext(configuration, connectorConfig));

        schema.refresh(Table.editor()
                .tableId(TABLE_ID)
                .addColumn(Column.editor().name("RECID").type("VARCHAR2").jdbcType(OracleTypes.VARCHAR).length(255).position(1).optional(false).create())
                .addColumn(Column.editor().name("DATA").type("VARCHAR2").jdbcType(OracleTypes.VARCHAR).length(50).position(2).create())
                .addColumn(Column.editor().name(XML_COLUMN).type("XMLTYPE").jdbcType(OracleTypes.SQLXML).position(3).create())
                .addColumn(Column.editor().name(CLOB_COLUMN).type("CLOB").jdbcType(OracleTypes.CLOB).position(4).create())
                .setPrimaryKeyNames("RECID")
                .create());

        schema.refresh(Table.editor()
                .tableId(PLAIN_TABLE_ID)
                .addColumn(Column.editor().name("RECID").type("VARCHAR2").jdbcType(OracleTypes.VARCHAR).length(255).position(1).optional(false).create())
                .addColumn(Column.editor().name("DATA").type("VARCHAR2").jdbcType(OracleTypes.VARCHAR).length(50).position(2).create())
                .setPrimaryKeyNames("RECID")
                .create());

        schema.refresh(Table.editor()
                .tableId(EXTENDED_TABLE_ID)
                .addColumn(Column.editor().name("RECID").type("VARCHAR2").jdbcType(OracleTypes.VARCHAR).length(255).position(1).optional(false).create())
                .addColumn(Column.editor().name(EXTENDED_STRING_COLUMN).type("VARCHAR2").jdbcType(OracleTypes.VARCHAR).length(8000).position(2).create())
                .setPrimaryKeyNames("RECID")
                .create());

        schema.refresh(Table.editor()
                .tableId(JSON_TABLE_ID)
                .addColumn(Column.editor().name("RECID").type("VARCHAR2").jdbcType(OracleTypes.VARCHAR).length(255).position(1).optional(false).create())
                .addColumn(Column.editor().name(JSON_COLUMN).type("JSON").jdbcType(OracleTypes.JSON).position(2).create())
                .setPrimaryKeyNames("RECID")
                .create());

        return schema;
    }
}
