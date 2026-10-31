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
import io.debezium.connector.oracle.logminer.events.DmlEvent;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.ExtendedStringBeginEvent;
import io.debezium.connector.oracle.logminer.events.ExtendedStringWriteEvent;
import io.debezium.connector.oracle.logminer.events.LobEraseEvent;
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
 * Unit tests for {@link TransactionCommitConsumer} merging of XML and extended string events when LOB support is enabled.
 * <p>
 * The concatenation reported in debezium/dbz#2159 was fixed by debezium/dbz#2368. The tests here lock in the merge
 * behavior for that scenario and harden the discard of a row's merge state.
 *
 * @author Chris Cranford
 */
public class TransactionCommitConsumerTest {

    private static final TableId TABLE_ID = TableId.parse("ORCLPDB1.DEBEZIUM.TEST_XML");
    private static final String ROW_ID = "AAAAAAAAAAAAAAAAAB";
    private static final String TRANSACTION_ID = "1234567890";
    private static final String XML_COLUMN = "XMLRECORD";
    private static final String CLOB_COLUMN = "DOC";
    private static final String EXTENDED_STRING_COLUMN = "DATA";
    private static final int DATA_INDEX = 1;
    private static final int XML_INDEX = 2;

    private OracleConnectorConfig connectorConfig;
    private OracleDatabaseSchema schema;
    private List<LogMinerEvent> dispatched;
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
        scn = 1;
    }

    @AfterEach
    void after() {
        if (schema != null) {
            schema.close();
        }
    }

    @Test
    @FixFor("debezium/dbz#2159")
    public void shouldUseLastXmlDocumentWhenEachDocumentHasItsOwnXmlBegin() throws Exception {
        try (TransactionCommitConsumer consumer = createConsumer()) {
            accept(consumer, insert());
            accept(consumer, xmlBegin());
            accept(consumer, xmlWrite("<row>a</row>"));
            accept(consumer, xmlEnd());
            accept(consumer, xmlBegin());
            accept(consumer, xmlWrite("<row>b</row>"));
            accept(consumer, xmlEnd());
        }

        assertSingleEvent(EventType.INSERT, "<row>b</row>");
    }

    @Test
    @FixFor("debezium/dbz#2159")
    public void shouldConcatenateMultipleXmlWritesOfSingleDocument() throws Exception {
        try (TransactionCommitConsumer consumer = createConsumer()) {
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
        try (TransactionCommitConsumer consumer = createConsumer()) {
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
        try (TransactionCommitConsumer consumer = createConsumer()) {
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
        try (TransactionCommitConsumer consumer = createConsumer()) {
            accept(consumer, insert());
            accept(consumer, extendedStringBegin());
            accept(consumer, extendedStringWrite("a"));
            accept(consumer, selectClobLocator());
            accept(consumer, lobErase());
            accept(consumer, extendedStringWrite("b"));
        }

        assertThat(dispatched).isEmpty();
    }

    private void assertSingleEvent(EventType eventType, String expectedXml) {
        assertThat(dispatched).hasSize(1);
        final DmlEvent event = (DmlEvent) dispatched.get(0);
        assertThat(event.getEventType()).isEqualTo(eventType);
        assertThat(event.getNewValues()[XML_INDEX]).isEqualTo(expectedXml);
    }

    private TransactionCommitConsumer createConsumer() {
        return new TransactionCommitConsumer(
                (event, eventIndex, eventTrxId, eventTrxSeq, eventsProcessed) -> dispatched.add(event),
                connectorConfig,
                schema);
    }

    private void accept(TransactionCommitConsumer consumer, LogMinerEvent event) throws InterruptedException {
        consumer.accept(event, TRANSACTION_ID, scn);
    }

    private DmlEvent insert() {
        return new DmlEvent(EventType.INSERT, nextScn(), TABLE_ID, ROW_ID, "rsId", Instant.now(),
                new Object[0], new Object[]{ "test3", "data", null, null });
    }

    private DmlEvent update(String data) {
        return new DmlEvent(EventType.UPDATE, nextScn(), TABLE_ID, ROW_ID, "rsId", Instant.now(),
                new Object[]{ "test3", "data", OracleValueConverters.UNAVAILABLE_VALUE, OracleValueConverters.UNAVAILABLE_VALUE },
                new Object[]{ "test3", data, OracleValueConverters.UNAVAILABLE_VALUE, OracleValueConverters.UNAVAILABLE_VALUE });
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
        final OracleValueConverters converters = connectorConfig.getAdapter().getValueConverter(connectorConfig, connection);
        final OracleDefaultValueConverter defaultValueConverter = new OracleDefaultValueConverter(converters, connection);

        final OracleDatabaseSchema schema = new OracleDatabaseSchema(connectorConfig,
                converters,
                defaultValueConverter,
                schemaNameAdjuster,
                topicNamingStrategy,
                StreamingAdapter.TableNameCaseSensitivity.INSENSITIVE,
                false,
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

        return schema;
    }
}
