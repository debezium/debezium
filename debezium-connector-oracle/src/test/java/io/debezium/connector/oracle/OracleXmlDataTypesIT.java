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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.junit.SkipWhenLogMiningStrategyIs;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.relational.Table;
import io.debezium.relational.Tables;
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
public class OracleXmlDataTypesIT extends AbstractAsyncEngineConnectorTest {

    // Short XML files
    private static final String XML_DATA = Testing.Files.readResourceAsString("data/test_xml_data_short.xml");
    private static final String XML_DATA2 = Testing.Files.readResourceAsString("data/test_xml_data_short2.xml");

    // Long XML files
    private static final String XML_LONG_DATA = Testing.Files.readResourceAsString("data/test_xml_data_long.xml");
    private static final String XML_LONG_DATA2 = Testing.Files.readResourceAsString("data/test_xml_data_long2.xml");

    private static final String DBZ1160_XML_SCHEMA_URL = "http://debezium.io/dbz1160.xsd";

    private OracleConnection connection;

    @BeforeEach
    void before() {
        connection = TestHelper.testConnection();
        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
    }

    @AfterEach
    void after() throws Exception {
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

    @Test
    @FixFor("dbz#1373")
    public void shouldHandleStreamingXmlDocumentStoredAsClob() throws Exception {
        TestHelper.dropTable(connection, "dbz1373");
        try {
            // Tests CLOB storage in DATA and combines with BLOB storage in DATA2
            connection.execute("CREATE TABLE dbz1373 (ID numeric(9,0), DATA xmltype, DATA2 xmltype, primary key(ID)) xmltype column data store as securefile clob");
            TestHelper.streamTable(connection, "dbz1373");

            connection.prepareUpdate("INSERT INTO dbz1373 values (1,?,?)", ps -> {
                ps.setObject(1, toXmlType(XML_LONG_DATA));
                ps.setObject(2, toXmlType(XML_LONG_DATA));
            });
            connection.commit();

            Configuration config = getDefaultXmlConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ1373")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            List<SourceRecord> records = consumeRecordsByTopic(1).recordsForTopic("server1.DEBEZIUM.DBZ1373");
            assertThat(records).hasSize(1);

            Struct after = after(records.get(0));
            assertThat(after.get("ID")).isEqualTo(1);
            assertXmlFieldIsEqual(after, "DATA", XML_LONG_DATA);

            connection.prepareUpdate("INSERT INTO dbz1373 values (2,?,?)", ps -> {
                ps.setObject(1, toXmlType(XML_LONG_DATA2));
                ps.setObject(2, toXmlType(XML_LONG_DATA2));
            });
            connection.commit();

            records = consumeRecordsByTopic(1).recordsForTopic("server1.DEBEZIUM.DBZ1373");
            assertThat(records).hasSize(1);

            after = after(records.get(0));
            assertThat(after.get("ID")).isEqualTo(2);
            assertXmlFieldIsEqual(after, "DATA", XML_LONG_DATA2);

            connection.prepareUpdate("UPDATE dbz1373 SET DATA = ? WHERE ID = 2", ps -> ps.setObject(1, toXmlType(XML_LONG_DATA)));
            connection.commit();

            records = consumeRecordsByTopic(1).recordsForTopic("server1.DEBEZIUM.DBZ1373");
            assertThat(records).hasSize(1);

            after = after(records.get(0));
            assertThat(after.get("ID")).isEqualTo(2);
            assertXmlFieldIsEqual(after, "DATA", XML_LONG_DATA);
            assertFieldIsUnavailablePlaceholder(after, "DATA2", config);

            connection.execute("DELETE FROM dbz1373 WHERE ID = 2");

            records = consumeRecordsByTopic(1).recordsForTopic("server1.DEBEZIUM.DBZ1373");
            assertThat(records).hasSize(1);

            Struct before = before(records.get(0));
            assertThat(before.get("ID")).isEqualTo(2);
            assertFieldIsUnavailablePlaceholder(before, "DATA", config);
            assertFieldIsUnavailablePlaceholder(before, "DATA2", config);

            after = after(records.get(0));
            assertThat(after).isNull();
        }
        finally {
            TestHelper.dropTable(connection, "dbz1373");
        }
    }

    @Test
    @FixFor("dbz#2656")
    public void shouldSnapshotTableWithBinaryXmlStorageXmlTypeColumn() throws Exception {
        TestHelper.dropTable(connection, "dbz2656");
        try {
            connection.execute("CREATE TABLE dbz2656 (ID numeric(9,0), DATA xmltype, primary key(ID)) " +
                    "XMLTYPE COLUMN DATA STORE AS SECUREFILE BINARY XML (CHUNK 8192) " +
                    "ALLOW NONSCHEMA DISALLOW ANYSCHEMA");
            TestHelper.streamTable(connection, "dbz2656");

            connection.prepareUpdate("INSERT INTO dbz2656 values (1,?)", ps -> ps.setObject(1, toXmlType(XML_DATA)));
            connection.commit();

            Configuration config = getDefaultXmlConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ2656")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            List<SourceRecord> topicRecords = consumeRecordsByTopic(1).recordsForTopic(topicName("DBZ2656"));
            assertThat(topicRecords).hasSize(1);

            SourceRecord record = topicRecords.get(0);
            VerifyRecord.isValidRead(record, "ID", 1);

            Struct after = after(record);
            assertThat(after.get("ID")).isEqualTo(1);
            assertXmlFieldIsEqual(after, "DATA", XML_DATA);
        }
        finally {
            TestHelper.dropTable(connection, "dbz2656");
        }
    }

    @Test
    @FixFor("dbz#2656")
    public void shouldStreamTableCreatedWithBinaryXmlStorageXmlTypeColumn() throws Exception {
        TestHelper.dropTable(connection, "dbz2656");
        try {
            Configuration config = getDefaultXmlConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ2656")
                    .with(OracleConnectorConfig.INCLUDE_SCHEMA_CHANGES, "true")
                    .with(SchemaHistory.STORE_ONLY_CAPTURED_TABLES_DDL, "true")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("CREATE TABLE dbz2656 (ID numeric(9,0), DATA xmltype, primary key(ID)) " +
                    "XMLTYPE COLUMN DATA STORE AS SECUREFILE BINARY XML (CHUNK 8192) " +
                    "ALLOW NONSCHEMA DISALLOW ANYSCHEMA");
            TestHelper.streamTable(connection, "dbz2656");

            List<SourceRecord> schemaChanges = consumeRecordsByTopic(2).recordsForTopic(TestHelper.SERVER_NAME);
            assertThat(schemaChanges).hasSize(2);

            List<Object> tableChanges = ((Struct) schemaChanges.get(0).value()).getArray("tableChanges");
            assertThat(tableChanges).hasSize(1);

            Struct tableChange = (Struct) tableChanges.get(0);
            assertThat(tableChange.getString("type")).isEqualTo("CREATE");
            assertThat(tableChange.getString("id")).contains("\"DBZ2656\"");

            Struct dataColumn = null;
            for (Object column : tableChange.getStruct("table").getArray("columns")) {
                if ("DATA".equals(((Struct) column).getString("name"))) {
                    dataColumn = (Struct) column;
                }
            }
            assertThat(dataColumn).isNotNull();
            assertThat(dataColumn.get("jdbcType")).isEqualTo(OracleTypes.SQLXML);
            assertThat(dataColumn.get("typeName")).isEqualTo("XMLTYPE");

            connection.prepareUpdate("INSERT INTO dbz2656 values (1,?)", ps -> ps.setObject(1, toXmlType(XML_DATA)));
            connection.commit();

            List<SourceRecord> topicRecords = consumeRecordsByTopic(1).recordsForTopic(topicName("DBZ2656"));
            assertThat(topicRecords).hasSize(1);

            SourceRecord record = topicRecords.get(0);
            VerifyRecord.isValidInsert(record, "ID", 1);

            Struct after = after(record);
            assertThat(after.get("ID")).isEqualTo(1);
            assertXmlFieldIsEqual(after, "DATA", XML_DATA);

            connection.prepareUpdate("UPDATE dbz2656 SET DATA = ? WHERE ID = 1", ps -> ps.setObject(1, toXmlType(XML_DATA2)));
            connection.commit();

            topicRecords = consumeRecordsByTopic(1).recordsForTopic(topicName("DBZ2656"));
            assertThat(topicRecords).hasSize(1);

            record = topicRecords.get(0);
            VerifyRecord.isValidUpdate(record, "ID", 1);

            after = after(record);
            assertThat(after.get("ID")).isEqualTo(1);
            assertXmlFieldIsEqual(after, "DATA", XML_DATA2);

            assertNoRecordsToConsume();
        }
        finally {
            TestHelper.dropTable(connection, "dbz2656");
        }
    }

    @Test
    @FixFor("debezium/dbz#1160")
    public void shouldReadXmlSchemaBasedTableWithObjectAttributePrimaryKeyAsKeyless() throws Exception {
        TestHelper.dropTable(connection, "dbz1160");
        try {
            createXmlSchemaBasedTableWithObjectAttributePrimaryKey();

            final Tables tables = new Tables();
            connection.readSchema(tables, null, "DEBEZIUM", Tables.TableFilter.fromPredicate(id -> "DBZ1160".equals(id.table())), null, false);

            final Table table = tables.tableIds().stream().filter(id -> "DBZ1160".equals(id.table())).findFirst().map(tables::forTable).orElse(null);
            assertThat(table).isNotNull();
            assertThat(table.primaryKeyColumnNames()).isEmpty();
        }
        finally {
            dropXmlSchemaBasedTable();
        }
    }

    @Test
    @FixFor("debezium/dbz#1160")
    public void shouldSnapshotWhenSchemaContainsXmlSchemaBasedTableWithObjectAttributePrimaryKey() throws Exception {
        TestHelper.dropTables(connection, "dbz1160", "dbz1160_data");
        try {
            createXmlSchemaBasedTableWithObjectAttributePrimaryKey();

            connection.execute("CREATE TABLE dbz1160_data (ID numeric(9,0) primary key, NAME varchar2(50))");
            TestHelper.streamTable(connection, "dbz1160_data");
            connection.execute("INSERT INTO dbz1160_data values (1, 'Debezium')");
            connection.execute("COMMIT");

            // The XML schema-based table is not captured, but its schema is read during the snapshot
            // because the connector stores the structure of all tables by default.
            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ1160_DATA")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            List<SourceRecord> topicRecords = consumeRecordsByTopic(1).recordsForTopic(topicName("DBZ1160_DATA"));
            assertThat(topicRecords).hasSize(1);
            VerifyRecord.isValidRead(topicRecords.get(0), "ID", 1);

            assertNoRecordsToConsume();
        }
        finally {
            TestHelper.dropTable(connection, "dbz1160_data");
            dropXmlSchemaBasedTable();
        }
    }

    private Configuration.Builder getDefaultXmlConfig() {
        return TestHelper.defaultConfig().with(OracleConnectorConfig.LOB_ENABLED, true);
    }

    private void createXmlSchemaBasedTableWithObjectAttributePrimaryKey() throws SQLException {
        // Registering the XML schema generates object types for its object-relational storage.
        TestHelper.grantRole("CREATE ANY TYPE");

        final String xsd = "<xs:schema xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:xdb=\"http://xmlns.oracle.com/xdb\" " +
                "elementFormDefault=\"qualified\" version=\"1.0\">" +
                "<xs:element name=\"Order\">" +
                "<xs:complexType xdb:SQLType=\"DBZ1160_ORDER_T\">" +
                "<xs:sequence>" +
                "<xs:element name=\"Reference\" xdb:SQLName=\"REFERENCE\">" +
                "<xs:simpleType><xs:restriction base=\"xs:string\"><xs:maxLength value=\"30\"/></xs:restriction></xs:simpleType>" +
                "</xs:element>" +
                "</xs:sequence>" +
                "</xs:complexType>" +
                "</xs:element>" +
                "</xs:schema>";

        connection.execute("BEGIN DBMS_XMLSCHEMA.registerSchema(SCHEMAURL => '" + DBZ1160_XML_SCHEMA_URL + "', " +
                "SCHEMADOC => '" + xsd + "', LOCAL => TRUE, GENTYPES => TRUE, GENTABLES => FALSE); END;");

        // The primary key is defined on an attribute of the object-relational storage, which is
        // reported by the JDBC metadata as "XMLDATA"."REFERENCE" and is not a column of the table.
        connection.execute("CREATE TABLE dbz1160 OF XMLTYPE XMLTYPE STORE AS OBJECT RELATIONAL " +
                "XMLSCHEMA \"" + DBZ1160_XML_SCHEMA_URL + "\" ELEMENT \"Order\"");
        connection.execute("ALTER TABLE dbz1160 ADD CONSTRAINT dbz1160_pk PRIMARY KEY (XMLDATA.\"REFERENCE\")");
        connection.execute("GRANT SELECT ON DEBEZIUM.DBZ1160 TO " + TestHelper.getConnectorUserName());

        connection.execute("INSERT INTO dbz1160 values (xmltype('<Order><Reference>ORDER-1</Reference></Order>'))");
        connection.execute("COMMIT");
    }

    private void dropXmlSchemaBasedTable() {
        TestHelper.dropTable(connection, "dbz1160");
        try {
            connection.execute("BEGIN DBMS_XMLSCHEMA.deleteSchema('" + DBZ1160_XML_SCHEMA_URL + "', DBMS_XMLSCHEMA.DELETE_CASCADE_FORCE); END;");
        }
        catch (SQLException e) {
            // The schema was not registered
        }
        TestHelper.revokeRole("CREATE ANY TYPE");
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
