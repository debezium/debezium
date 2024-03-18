/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.junit.SkipTestDependingOnStrategyRule;
import io.debezium.connector.oracle.junit.SkipWhenLogMiningStrategyIs;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.util.Testing;

import oracle.jdbc.OracleTypes;
import oracle.xdb.XMLType;
import oracle.xml.parser.v2.XMLDocument;

/**
 * Integration tests for XML data type support.
 *
 * @author Chris Cranford
 */
@SkipWhenLogMiningStrategyIs(value = SkipWhenLogMiningStrategyIs.Strategy.HYBRID, reason = "Hybrid does not support XML")
public class OracleXmlDataTypesIT extends AbstractConnectorTest {

    // Short XML files
    private static final String XML_DATA = Testing.Files.readResourceAsString("data/test_xml_data_short.xml");
    private static final String XML_DATA2 = Testing.Files.readResourceAsString("data/test_xml_data_short2.xml");

    // Long XML files
    private static final String XML_LONG_DATA = Testing.Files.readResourceAsString("data/test_xml_data_long.xml");
    private static final String XML_LONG_DATA2 = Testing.Files.readResourceAsString("data/test_xml_data_long2.xml");

    @Rule
    public final TestRule skipStrategyRule = new SkipTestDependingOnStrategyRule();

    private OracleConnection connection;

    @Before
    public void before() {
        connection = TestHelper.testConnection();
        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
    }

    @After
    public void after() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    @FixFor("DBZ-3605")
    public void shouldSnapshotTableWithXmlTypeColumnWithSimpleXmlData() throws Exception {
        TestHelper.dropTable(connection, "dbz3605");
        try {
            connection.execute("CREATE TABLE DBZ3605 (ID numeric(9,0), DATA xmltype, primary key(ID))");
            TestHelper.streamTable(connection, "dbz3605");

            final String xml = "<?xml version=\"1.0\"?><warehouse></warehouse>";
            connection.execute("insert into dbz3605 values (1, xmltype('" + xml + "'))");

            Configuration config = getDefaultXmlConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ3605")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            SourceRecords records = consumeRecordsByTopic(1);
            List<SourceRecord> topicRecords = records.recordsForTopic(topicName("DBZ3605"));
            assertThat(topicRecords).hasSize(1);

            SourceRecord record = topicRecords.get(0);
            VerifyRecord.isValidRead(record, "ID", 1);

            Struct after = after(record);
            assertThat(after.get("ID")).isEqualTo(1);
            assertXmlFieldIsEqual(after, "DATA", xml);
        }
        finally {
            TestHelper.dropTable(connection, "dbz3605");
        }
    }

    @Test
    @FixFor("DBZ-3605")
    public void shouldSnapshotTableWithXmlTypeColumnWithShortXmlData() throws Exception {
        TestHelper.dropTable(connection, "dbz3605");
        try {
            connection.execute("CREATE TABLE DBZ3605 (ID numeric(9,0), DATA xmltype, primary key(ID))");
            TestHelper.streamTable(connection, "dbz3605");

            final String xml = XML_DATA;
            connection.prepareQuery("insert into dbz3605 values (1,xmltype(?))", ps -> ps.setObject(1, xml), null);
            connection.commit();

            Configuration config = getDefaultXmlConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ3605")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            SourceRecords records = consumeRecordsByTopic(1);
            List<SourceRecord> topicRecords = records.recordsForTopic(topicName("DBZ3605"));
            assertThat(topicRecords).hasSize(1);

            SourceRecord record = topicRecords.get(0);
            VerifyRecord.isValidRead(record, "ID", 1);

            Struct after = after(record);
            assertThat(after.get("ID")).isEqualTo(1);
            assertXmlFieldIsEqual(after, "DATA", xml);
        }
        finally {
            TestHelper.dropTable(connection, "dbz3605");
        }
    }

    @Test
    @FixFor("DBZ-3605")
    public void shouldSnapshotTableWithXmlTypeColumnWithLongXmlData() throws Exception {
        TestHelper.dropTable(connection, "dbz3605");
        try {
            connection.execute("CREATE TABLE DBZ3605 (ID numeric(9,0), DATA xmltype, primary key(ID))");
            TestHelper.streamTable(connection, "dbz3605");

            final String xml = XML_LONG_DATA;
            connection.prepareQuery("insert into dbz3605 values (1,?)", ps -> ps.setObject(1, toXmlType(xml)), null);
            connection.commit();

            Configuration config = getDefaultXmlConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ3605")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            SourceRecords records = consumeRecordsByTopic(1);
            List<SourceRecord> topicRecords = records.recordsForTopic(topicName("DBZ3605"));
            assertThat(topicRecords).hasSize(1);

            SourceRecord record = topicRecords.get(0);
            VerifyRecord.isValidRead(record, "ID", 1);

            Struct after = after(record);
            assertThat(after.get("ID")).isEqualTo(1);
            assertXmlFieldIsEqual(after, "DATA", xml);
        }
        finally {
            TestHelper.dropTable(connection, "dbz3605");
        }
    }

    @Test
    @FixFor("DBZ-3605")
    public void shouldStreamTableWithXmlTypeColumnWithSimpleXmlData() throws Exception {
        TestHelper.dropTable(connection, "dbz3605");
        try {
            connection.execute("CREATE TABLE DBZ3605 (ID numeric(9,0), DATA xmltype, primary key(ID))");
            TestHelper.streamTable(connection, "dbz3605");

            Configuration config = getDefaultXmlConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ3605")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            final String xml = "<?xml version=\"1.0\"?><warehouse></warehouse>";
            connection.execute("insert into dbz3605 values (1, xmltype('" + xml + "'))");

            SourceRecords records = consumeRecordsByTopic(1);
            List<SourceRecord> topicRecords = records.recordsForTopic(topicName("DBZ3605"));
            assertThat(topicRecords).hasSize(1);

            SourceRecord record = topicRecords.get(0);
            VerifyRecord.isValidInsert(record, "ID", 1);

            Struct after = after(record);
            assertThat(after.get("ID")).isEqualTo(1);
            assertXmlFieldIsEqual(after, "DATA", xml);

            final String updateXml = "<?xml version=\"1.0\"?><warehouse><dept>25</dept></warehouse>";
            connection.execute("UPDATE dbz3605 SET data = xmltype('" + updateXml + "') WHERE id = 1");

            records = consumeRecordsByTopic(1);
            topicRecords = records.recordsForTopic(topicName("DBZ3605"));
            assertThat(topicRecords).hasSize(1);

            record = topicRecords.get(0);
            VerifyRecord.isValidUpdate(record, "ID", 1);

            after = after(record);
            assertThat(after.get("ID")).isEqualTo(1);
            assertXmlFieldIsEqual(after, "DATA", updateXml);

            connection.execute("DELETE FROM dbz3605 WHERE id = 1");

            records = consumeRecordsByTopic(1);
            topicRecords = records.recordsForTopic(topicName("DBZ3605"));
            assertThat(topicRecords).hasSize(1);

            record = topicRecords.get(0);
            VerifyRecord.isValidDelete(record, "ID", 1);

            after = before(record);
            assertThat(after.get("ID")).isEqualTo(1);
            assertFieldIsUnavailablePlaceholder(after, "DATA", config);

            assertThat(after(record)).isNull();
        }
        finally {
            TestHelper.dropTable(connection, "dbz3605");
        }
    }

    @Test
    @FixFor("DBZ-3605")
    public void shouldStreamTableWithXmlTypeColumnWithShortXmlData() throws Exception {
        TestHelper.dropTable(connection, "dbz3605");
        try {
            connection.execute("CREATE TABLE DBZ3605 (ID numeric(9,0), DATA xmltype, primary key(ID))");
            TestHelper.streamTable(connection, "dbz3605");

            Configuration config = getDefaultXmlConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ3605")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            final String xml = XML_DATA;
            connection.prepareQuery("insert into dbz3605 values (1, xmltype(?))", ps -> ps.setObject(1, xml), null);
            connection.commit();

            SourceRecords records = consumeRecordsByTopic(1);
            List<SourceRecord> topicRecords = records.recordsForTopic(topicName("DBZ3605"));
            assertThat(topicRecords).hasSize(1);

            SourceRecord record = topicRecords.get(0);
            VerifyRecord.isValidInsert(record, "ID", 1);

            Struct after = after(record);
            assertThat(after.get("ID")).isEqualTo(1);
            assertXmlFieldIsEqual(after, "DATA", xml);

            final String updateXml = XML_DATA2;
            connection.prepareQuery("UPDATE dbz3605 SET data = xmltype(?) WHERE id=1", ps -> ps.setObject(1, updateXml), null);
            connection.commit();

            records = consumeRecordsByTopic(1);
            topicRecords = records.recordsForTopic(topicName("DBZ3605"));
            assertThat(topicRecords).hasSize(1);

            record = topicRecords.get(0);
            VerifyRecord.isValidUpdate(record, "ID", 1);

            after = after(record);
            assertThat(after.get("ID")).isEqualTo(1);
            assertXmlFieldIsEqual(after, "DATA", updateXml);

            connection.execute("DELETE FROM dbz3605 WHERE id = 1");

            records = consumeRecordsByTopic(1);
            topicRecords = records.recordsForTopic(topicName("DBZ3605"));
            assertThat(topicRecords).hasSize(1);

            record = topicRecords.get(0);
            VerifyRecord.isValidDelete(record, "ID", 1);

            after = before(record);
            assertThat(after.get("ID")).isEqualTo(1);
            assertFieldIsUnavailablePlaceholder(after, "DATA", config);

            assertThat(after(record)).isNull();
        }
        finally {
            TestHelper.dropTable(connection, "dbz3605");
        }
    }

    @Test
    @FixFor("DBZ-3605")
    public void shouldStreamTableWithXmlTypeColumnWithLongXmlData() throws Exception {
        TestHelper.dropTable(connection, "dbz3605");
        try {
            connection.execute("CREATE TABLE DBZ3605 (ID numeric(9,0), DATA xmltype, primary key(ID))");
            TestHelper.streamTable(connection, "dbz3605");

            Configuration config = getDefaultXmlConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ3605")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            final String xml = XML_LONG_DATA;
            connection.prepareQuery("insert into dbz3605 values (1,?)", ps -> ps.setObject(1, toXmlType(xml)), null);
            connection.commit();

            SourceRecords records = consumeRecordsByTopic(1);
            List<SourceRecord> topicRecords = records.recordsForTopic(topicName("DBZ3605"));
            assertThat(topicRecords).hasSize(1);

            SourceRecord record = topicRecords.get(0);
            VerifyRecord.isValidInsert(record, "ID", 1);

            Struct after = after(record);
            assertThat(after.get("ID")).isEqualTo(1);
            assertXmlFieldIsEqual(after, "DATA", xml);

            final String updateXml = XML_LONG_DATA2;
            connection.prepareQuery("UPDATE dbz3605 SET data = ? WHERE id=1", ps -> ps.setObject(1, toXmlType(updateXml)), null);
            connection.commit();

            records = consumeRecordsByTopic(1);
            topicRecords = records.recordsForTopic(topicName("DBZ3605"));
            assertThat(topicRecords).hasSize(1);

            record = topicRecords.get(0);
            VerifyRecord.isValidUpdate(record, "ID", 1);

            after = after(record);
            assertThat(after.get("ID")).isEqualTo(1);
            assertXmlFieldIsEqual(after, "DATA", updateXml);

            connection.execute("DELETE FROM dbz3605 WHERE id = 1");

            records = consumeRecordsByTopic(1);
            topicRecords = records.recordsForTopic(topicName("DBZ3605"));
            assertThat(topicRecords).hasSize(1);

            record = topicRecords.get(0);
            VerifyRecord.isValidDelete(record, "ID", 1);

            after = before(record);
            assertThat(after.get("ID")).isEqualTo(1);
            assertFieldIsUnavailablePlaceholder(after, "DATA", config);

            assertThat(after(record)).isNull();
        }
        finally {
            TestHelper.dropTable(connection, "dbz3605");
        }
    }

    @Test
    @FixFor("DBZ-3605")
    public void shouldStreamTableWithXmlTypeColumnAndOtherNonLobColumns() throws Exception {
        // This tests makes sure there are no special requirements when a table is keyless to be able
        // to perform the merge operations of the multiple XML_WRITE fragments.

        TestHelper.dropTable(connection, "dbz3605");
        try {
            // Explicitly no key.
            connection.execute("CREATE TABLE DBZ3605 (ID numeric(9,0), DATA xmltype, DATA2 varchar2(50))");
            TestHelper.streamTable(connection, "dbz3605");

            Configuration config = getDefaultXmlConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ3605")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            final String xml = XML_LONG_DATA;
            connection.prepareQuery("insert into dbz3605 values (1,?,'Acme')", ps -> ps.setObject(1, toXmlType(xml)), null);
            connection.commit();

            SourceRecords records = consumeRecordsByTopic(1);
            List<SourceRecord> topicRecords = records.recordsForTopic(topicName("DBZ3605"));
            assertThat(topicRecords).hasSize(1);

            SourceRecord record = topicRecords.get(0);
            VerifyRecord.isValidInsert(record, false);

            Struct after = after(record);
            assertThat(after.get("ID")).isEqualTo(1);
            assertXmlFieldIsEqual(after, "DATA", xml);
            assertThat(after.get("DATA2")).isEqualTo("Acme");

            // Update only XML
            final String updateXml = XML_LONG_DATA2;
            connection.prepareQuery("UPDATE dbz3605 SET data = ? WHERE id=1", ps -> ps.setObject(1, toXmlType(updateXml)), null);
            connection.commit();

            records = consumeRecordsByTopic(1);
            topicRecords = records.recordsForTopic(topicName("DBZ3605"));
            assertThat(topicRecords).hasSize(1);

            record = topicRecords.get(0);
            VerifyRecord.isValidUpdate(record, false);

            after = after(record);
            assertThat(after.get("ID")).isEqualTo(1);
            assertXmlFieldIsEqual(after, "DATA", updateXml);
            assertThat(after.get("DATA2")).isEqualTo("Acme");

            // Update XML and non-XML
            connection.prepareQuery("UPDATE dbz3605 SET data = ?, DATA2 = 'Data' WHERE id=1", ps -> ps.setObject(1, toXmlType(xml)), null);
            connection.commit();

            records = consumeRecordsByTopic(1);
            topicRecords = records.recordsForTopic(topicName("DBZ3605"));
            assertThat(topicRecords).hasSize(1);

            record = topicRecords.get(0);
            VerifyRecord.isValidUpdate(record, false);

            after = after(record);
            assertThat(after.get("ID")).isEqualTo(1);
            assertXmlFieldIsEqual(after, "DATA", xml);
            assertThat(after.get("DATA2")).isEqualTo("Data");

            // Update only non-XML
            connection.execute("UPDATE dbz3605 SET DATA2 = 'Acme' WHERE id=1");

            records = consumeRecordsByTopic(1);
            topicRecords = records.recordsForTopic(topicName("DBZ3605"));
            assertThat(topicRecords).hasSize(1);

            record = topicRecords.get(0);
            VerifyRecord.isValidUpdate(record, false);

            after = after(record);
            assertThat(after.get("ID")).isEqualTo(1);
            assertFieldIsUnavailablePlaceholder(after, "DATA", config);
            assertThat(after.get("DATA2")).isEqualTo("Acme");

            connection.execute("DELETE FROM dbz3605 WHERE id = 1");

            records = consumeRecordsByTopic(1);
            topicRecords = records.recordsForTopic(topicName("DBZ3605"));
            assertThat(topicRecords).hasSize(1);

            record = topicRecords.get(0);
            VerifyRecord.isValidDelete(record, false);

            after = before(record);
            assertThat(after.get("ID")).isEqualTo(1);
            assertFieldIsUnavailablePlaceholder(after, "DATA", config);
            assertThat(after.get("DATA2")).isEqualTo("Acme");

            assertThat(after(record)).isNull();
        }
        finally {
            TestHelper.dropTable(connection, "dbz3605");
        }
    }

    @Test
    @FixFor("DBZ-3605")
    public void shouldStreamTableWithNoPrimaryKeyWithXmlTypeColumn() throws Exception {
        // This tests makes sure there are no special requirements when a table is keyless to be able
        // to perform the merge operations of the multiple XML_WRITE fragments.

        TestHelper.dropTable(connection, "dbz3605");
        try {
            // Explicitly no key.
            connection.execute("CREATE TABLE DBZ3605 (ID numeric(9,0), DATA xmltype)");
            TestHelper.streamTable(connection, "dbz3605");

            Configuration config = getDefaultXmlConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ3605")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            final String xml = XML_LONG_DATA;
            connection.prepareQuery("insert into dbz3605 values (1,?)", ps -> ps.setObject(1, toXmlType(xml)), null);
            connection.commit();

            SourceRecords records = consumeRecordsByTopic(1);
            List<SourceRecord> topicRecords = records.recordsForTopic(topicName("DBZ3605"));
            assertThat(topicRecords).hasSize(1);

            SourceRecord record = topicRecords.get(0);
            VerifyRecord.isValidInsert(record, false);

            Struct after = after(record);
            assertThat(after.get("ID")).isEqualTo(1);
            assertXmlFieldIsEqual(after, "DATA", xml);

            final String updateXml = XML_LONG_DATA2;
            connection.prepareQuery("UPDATE dbz3605 SET data = ? WHERE id=1", ps -> ps.setObject(1, toXmlType(updateXml)), null);
            connection.commit();

            records = consumeRecordsByTopic(1);
            topicRecords = records.recordsForTopic(topicName("DBZ3605"));
            assertThat(topicRecords).hasSize(1);

            record = topicRecords.get(0);
            VerifyRecord.isValidUpdate(record, false);

            after = after(record);
            assertThat(after.get("ID")).isEqualTo(1);
            assertXmlFieldIsEqual(after, "DATA", updateXml);

            connection.execute("DELETE FROM dbz3605 WHERE id = 1");

            records = consumeRecordsByTopic(1);
            topicRecords = records.recordsForTopic(topicName("DBZ3605"));
            assertThat(topicRecords).hasSize(1);

            record = topicRecords.get(0);
            VerifyRecord.isValidDelete(record, false);

            after = before(record);
            assertThat(after.get("ID")).isEqualTo(1);
            assertFieldIsUnavailablePlaceholder(after, "DATA", config);

            assertThat(after(record)).isNull();
        }
        finally {
            TestHelper.dropTable(connection, "dbz3605");
        }
    }

    @Test
    @FixFor("DBZ-3605")
    public void shouldStreamTableWithXmlTypeColumnAndAnotherLobColumn() throws Exception {
        // For simplicity, pair large XML with a large CLOB data column for multi-fragment processing

        TestHelper.dropTable(connection, "dbz3605");
        try {
            // Explicitly no key.
            connection.execute("CREATE TABLE DBZ3605 (ID numeric(9,0), DATA xmltype, DATA2 clob)");
            TestHelper.streamTable(connection, "dbz3605");

            Configuration config = getDefaultXmlConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ3605")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            final String xml = XML_LONG_DATA;
            final Clob clob = connection.connection().createClob();
            clob.setString(1, XML_LONG_DATA);
            connection.prepareQuery("insert into dbz3605 values (1,?,?)",
                    ps -> {
                        ps.setObject(1, toXmlType(xml));
                        ps.setClob(2, clob);
                    }, null);
            connection.commit();

            SourceRecords records = consumeRecordsByTopic(1);
            List<SourceRecord> topicRecords = records.recordsForTopic(topicName("DBZ3605"));
            assertThat(topicRecords).hasSize(1);

            SourceRecord record = topicRecords.get(0);
            VerifyRecord.isValidInsert(record, false);

            Struct after = after(record);
            assertThat(after.get("ID")).isEqualTo(1);
            assertXmlFieldIsEqual(after, "DATA", xml);
            assertThat(after.get("DATA2")).isEqualTo(clob.getSubString(1, (int) clob.length()));

            final String updateXml = XML_LONG_DATA2;
            connection.prepareQuery("UPDATE dbz3605 SET data = ? WHERE id=1", ps -> ps.setObject(1, toXmlType(updateXml)), null);
            connection.commit();

            records = consumeRecordsByTopic(1);
            topicRecords = records.recordsForTopic(topicName("DBZ3605"));
            assertThat(topicRecords).hasSize(1);

            record = topicRecords.get(0);
            VerifyRecord.isValidUpdate(record, false);

            after = after(record);
            assertThat(after.get("ID")).isEqualTo(1);
            assertXmlFieldIsEqual(after, "DATA", updateXml);
            assertFieldIsUnavailablePlaceholder(after, "DATA2", config);

            connection.execute("DELETE FROM dbz3605 WHERE id = 1");

            records = consumeRecordsByTopic(1);
            topicRecords = records.recordsForTopic(topicName("DBZ3605"));
            assertThat(topicRecords).hasSize(1);

            record = topicRecords.get(0);
            VerifyRecord.isValidDelete(record, false);

            after = before(record);
            assertThat(after.get("ID")).isEqualTo(1);
            assertFieldIsUnavailablePlaceholder(after, "DATA", config);
            assertFieldIsUnavailablePlaceholder(after, "DATA2", config);

            assertThat(after(record)).isNull();
        }
        finally {
            TestHelper.dropTable(connection, "dbz3605");
        }
    }

    @Test
    @FixFor("DBZ-6782")
    public void shouldProperlyResolveAddedXmlColumnTypeAndStreamChanges() throws Exception {
        TestHelper.dropTable(connection, "dbz6782");
        try {
            // Explicitly no key.
            connection.execute("CREATE TABLE dbz6782 (ID numeric(9,0), DATA xmltype, primary key(ID))");
            TestHelper.streamTable(connection, "dbz6782");

            final String xml = XML_LONG_DATA;
            connection.prepareQuery("insert into dbz6782 values (1,?)", ps -> ps.setObject(1, toXmlType(xml)), null);
            connection.commit();

            Configuration config = getDefaultXmlConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ6782")
                    .with(OracleConnectorConfig.INCLUDE_SCHEMA_CHANGES, "true")
                    .with(SchemaHistory.STORE_ONLY_CAPTURED_TABLES_DDL, "true")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("ALTER TABLE dbz6782 add DATA2 xmltype");

            final String xml2 = XML_LONG_DATA2;
            connection.prepareQuery("insert into dbz6782 values (2,?,?)",
                    ps -> {
                        ps.setObject(1, toXmlType(xml));
                        ps.setObject(2, toXmlType(xml2));
                    }, null);
            connection.commit();

            // Schema changes + data changes
            SourceRecords records = consumeRecordsByTopic(4);
            List<SourceRecord> topicRecords = records.recordsForTopic(topicName("DBZ6782"));
            assertThat(topicRecords).hasSize(2);

            // Snapshot
            SourceRecord record = topicRecords.get(0);
            VerifyRecord.isValidRead(record, "ID", 1);

            Struct after = after(record);
            assertThat(after.get("ID")).isEqualTo(1);
            assertXmlFieldIsEqual(after, "DATA", xml);

            // Insert during streaming
            record = topicRecords.get(1);
            VerifyRecord.isValidInsert(record, "ID", 2);

            after = after(record);
            assertThat(after.get("ID")).isEqualTo(2);
            assertXmlFieldIsEqual(after, "DATA", xml);
            assertXmlFieldIsEqual(after, "DATA2", xml2);

            // Schema changes
            List<SourceRecord> schemaChanges = records.recordsForTopic("server1");

            List<Object> tableChanges = ((Struct) schemaChanges.get(1).value()).getArray("tableChanges");
            assertThat(tableChanges).hasSize(1);

            Struct tableChange = (Struct) tableChanges.get(0);
            assertThat(tableChange.getString("type")).isEqualTo("ALTER");
            assertThat(tableChange.getString("id")).contains("\"DBZ6782\"");

            // Verify columns
            for (Object column : tableChange.getStruct("table").getArray("columns")) {
                Struct columnStruct = (Struct) column;
                if (columnStruct.getString("name").startsWith("DATA")) {
                    assertThat(columnStruct.get("jdbcType")).isEqualTo(OracleTypes.SQLXML);
                    assertThat(columnStruct.get("typeName")).isEqualTo("XMLTYPE");
                    assertThat(columnStruct.get("typeExpression")).isEqualTo("XMLTYPE");
                }
            }

            assertNoRecordsToConsume();
        }
        finally {
            TestHelper.dropTable(connection, "dbz6782");
        }
    }

    @Test
    @FixFor("DBZ-7489")
    public void shouldHandleStreamingSettingXmlColumnToNull() throws Exception {
        TestHelper.dropTable(connection, "dbz7489");
        try {
            connection.execute("CREATE TABLE dbz7489 (ID numeric(9,0), DATA xmltype, primary key(ID))");
            TestHelper.streamTable(connection, "dbz7489");

            Configuration config = getDefaultXmlConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ7489")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            final String xml = XML_LONG_DATA;
            connection.prepareQuery("insert into dbz7489 values (1,?)", ps -> ps.setObject(1, toXmlType(xml)), null);
            connection.commit();

            SourceRecords records = consumeRecordsByTopic(1);
            List<SourceRecord> topicRecords = records.recordsForTopic(topicName("DBZ7489"));
            assertThat(topicRecords).hasSize(1);

            SourceRecord record = topicRecords.get(0);
            VerifyRecord.isValidInsert(record, "ID", 1);

            Struct after = after(record);
            assertThat(after.get("ID")).isEqualTo(1);
            assertXmlFieldIsEqual(after, "DATA", xml);

            connection.execute("UPDATE dbz7489 SET data = NULL where id = 1");

            records = consumeRecordsByTopic(1);
            topicRecords = records.recordsForTopic(topicName("DBZ7489"));
            assertThat(topicRecords).hasSize(1);

            record = topicRecords.get(0);
            VerifyRecord.isValidUpdate(record, "ID", 1);

            after = after(record);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("DATA")).isNull();
        }
        finally {
            TestHelper.dropTable(connection, "dbz7489");
        }
    }

    private Configuration.Builder getDefaultXmlConfig() {
        return TestHelper.defaultConfig().with(OracleConnectorConfig.LOB_ENABLED, true);
    }

    private XMLType toXmlType(String data) throws SQLException {
        return XMLType.createXML(connection.connection(), data, XMLDocument.THIN);
    }

    private static void assertFieldIsUnavailablePlaceholder(Struct after, String fieldName, Configuration config) {
        assertThat(after.getString(fieldName)).isEqualTo(config.getString(OracleConnectorConfig.UNAVAILABLE_VALUE_PLACEHOLDER));
    }

    private static void assertXmlFieldIsEqual(Struct after, String fieldName, String expected) {
        assertThat(formatToOracleXml(after.getString(fieldName))).isEqualTo(formatToOracleXml(expected));
    }

    private static String formatToOracleXml(String data) {
        if (data == null) {
            return null;
        }

        try {
            final TransformerFactory transformerFactory = TransformerFactory.newInstance();
            final InputStream xslt = Testing.Files.readResourceAsStream("xml-format.xslt");
            final Transformer transformer = transformerFactory.newTransformer(new StreamSource(xslt));

            final Source in = new StreamSource(new StringReader(data));
            final StreamResult out = new StreamResult(new StringWriter());
            transformer.transform(in, out);
            return out.getWriter().toString();
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to parse XML: " + data, e);
        }
    }

    private static String topicName(String tableName) {
        return TestHelper.SERVER_NAME + ".DEBEZIUM." + tableName;
    }

    private static Struct before(SourceRecord record) {
        return ((Struct) record.value()).getStruct(Envelope.FieldName.BEFORE);
    }

    private static Struct after(SourceRecord record) {
        return ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
    }

}
