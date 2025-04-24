/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import static io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot.AdapterName.LOGMINER;
import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnector;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.logminer.buffered.processor.AbstractLogMinerEventProcessor;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.junit.logging.LogInterceptor;

import ch.qos.logback.classic.Level;

/**
 * Integration tests for the LogMiner {@code CLIENT_ID} filter configuration options.
 *
 * @author Chris Cranford
 */
@SkipWhenAdapterNameIsNot(value = LOGMINER)
public class ClientIdFilterIT extends AbstractAsyncEngineConnectorTest {

    @Rule
    public final TestRule skipAdapterRule = new SkipTestDependingOnAdapterNameRule();

    private static OracleConnection connection;

    @BeforeClass
    public static void beforeClass() throws SQLException {
        connection = TestHelper.testConnection();
    }

    @AfterClass
    public static void closeConnection() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    @FixFor("DBZ-8904")
    public void shouldExcludeTransactionWithAnExcludedClientId() throws Exception {
        TestHelper.dropTable(connection, "dbz8904");
        try {
            connection.execute("CREATE TABLE dbz8904 (id numeric(9,0) primary key, data varchar2(50))");
            TestHelper.streamTable(connection, "dbz8904");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ8904")
                    .with(OracleConnectorConfig.LOG_MINING_CLIENTID_EXCLUDE_LIST, "abc,xyz")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            LogInterceptor logInterceptor = new LogInterceptor(AbstractLogMinerEventProcessor.class);
            logInterceptor.setLoggerLevel(AbstractLogMinerEventProcessor.class, Level.DEBUG);

            try (OracleConnection testConnection = TestHelper.testConnection()) {
                testConnection.connection().setClientInfo("OCSID.CLIENTID", "abc");
                testConnection.execute("INSERT INTO dbz8904 (id,data) values (1,'abc')");
            }

            try (OracleConnection testConnection = TestHelper.testConnection()) {
                testConnection.connection().setClientInfo("OCSID.CLIENTID", "xyz");
                testConnection.execute("INSERT INTO dbz8904 (id,data) values (2,'xyz')");
            }

            Awaitility.await()
                    .atMost(Duration.ofSeconds(TestHelper.defaultMessageConsumerPollTimeout()))
                    .until(() -> logInterceptor.containsMessage("Skipped transaction with excluded client id abc") &&
                            logInterceptor.containsMessage("Skipped transaction with excluded client id xyz"));

            connection.execute("INSERT INTO dbz8904 (id,data) values (3,'none')");

            List<SourceRecord> records = consumeRecordsByTopic(1).recordsForTopic("server1.DEBEZIUM.DBZ8904");
            assertThat(records).hasSize(1);
            assertThat(getAfter(records.get(0)).get("ID")).isEqualTo(3);
            assertThat(getAfter(records.get(0)).get("DATA")).isEqualTo("none");

            assertNoRecordsToConsume();
        }
        finally {
            TestHelper.dropTable(connection, "dbz8904");
        }
    }

    @Test
    @FixFor("DBZ-8904")
    public void shouldExcludeTransactionWithoutLoggingWithAnExcludedClientId() throws Exception {
        TestHelper.dropTable(connection, "dbz8904");
        try {
            connection.execute("CREATE TABLE dbz8904 (id numeric(9,0) primary key, data varchar2(50))");
            TestHelper.streamTable(connection, "dbz8904");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ8904")
                    .with(OracleConnectorConfig.LOG_MINING_CLIENTID_EXCLUDE_LIST, "abc,xyz")
                    .with(OracleConnectorConfig.LOG_MINING_QUERY_FILTER_MODE, "in")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            LogInterceptor logInterceptor = new LogInterceptor(AbstractLogMinerEventProcessor.class);
            logInterceptor.setLoggerLevel(AbstractLogMinerEventProcessor.class, Level.DEBUG);

            try (OracleConnection testConnection = TestHelper.testConnection()) {
                testConnection.connection().setClientInfo("OCSID.CLIENTID", "abc");
                testConnection.execute("INSERT INTO dbz8904 (id,data) values (1,'abc')");
            }

            try (OracleConnection testConnection = TestHelper.testConnection()) {
                testConnection.connection().setClientInfo("OCSID.CLIENTID", "xyz");
                testConnection.execute("INSERT INTO dbz8904 (id,data) values (2,'xyz')");
            }

            connection.execute("INSERT INTO dbz8904 (id,data) values (3,'none')");

            List<SourceRecord> records = consumeRecordsByTopic(1).recordsForTopic("server1.DEBEZIUM.DBZ8904");
            assertThat(records).hasSize(1);
            assertThat(getAfter(records.get(0)).get("ID")).isEqualTo(3);
            assertThat(getAfter(records.get(0)).get("DATA")).isEqualTo("none");

            assertThat(logInterceptor.containsMessage("Skipped transaction with excluded client id abc")).isFalse();
            assertThat(logInterceptor.containsMessage("Skipped transaction with excluded client id xyz")).isFalse();

            assertNoRecordsToConsume();
        }
        finally {
            TestHelper.dropTable(connection, "dbz8904");
        }
    }

    @Test
    @FixFor("DBZ-8904")
    public void shouldExcludeTransactionWithAnIncludedClientId() throws Exception {
        TestHelper.dropTable(connection, "dbz8904");
        try {
            connection.execute("CREATE TABLE dbz8904 (id numeric(9,0) primary key, data varchar2(50))");
            TestHelper.streamTable(connection, "dbz8904");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ8904")
                    .with(OracleConnectorConfig.LOG_MINING_CLIENTID_INCLUDE_LIST, "abc")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            LogInterceptor logInterceptor = new LogInterceptor(AbstractLogMinerEventProcessor.class);
            logInterceptor.setLoggerLevel(AbstractLogMinerEventProcessor.class, Level.DEBUG);

            try (OracleConnection testConnection = TestHelper.testConnection()) {
                testConnection.connection().setClientInfo("OCSID.CLIENTID", "abc");
                testConnection.execute("INSERT INTO dbz8904 (id,data) values (1,'abc')");
            }

            try (OracleConnection testConnection = TestHelper.testConnection()) {
                testConnection.connection().setClientInfo("OCSID.CLIENTID", "xyz");
                testConnection.execute("INSERT INTO dbz8904 (id,data) values (2,'xyz')");
            }

            Awaitility.await()
                    .atMost(Duration.ofSeconds(TestHelper.defaultMessageConsumerPollTimeout()))
                    .until(() -> logInterceptor.containsMessage("Skipped transaction with client id xyz"));

            connection.execute("INSERT INTO dbz8904 (id,data) values (3,'none')");

            List<SourceRecord> records = consumeRecordsByTopic(1).recordsForTopic("server1.DEBEZIUM.DBZ8904");
            assertThat(records).hasSize(1);
            assertThat(getAfter(records.get(0)).get("ID")).isEqualTo(1);
            assertThat(getAfter(records.get(0)).get("DATA")).isEqualTo("abc");

            assertNoRecordsToConsume();
        }
        finally {
            TestHelper.dropTable(connection, "dbz8904");
        }
    }

    @Test
    @FixFor("DBZ-8904")
    public void shouldExcludeTransactionWithoutLoggingWithAnIncludedClientId() throws Exception {
        TestHelper.dropTable(connection, "dbz8904");
        try {
            connection.execute("CREATE TABLE dbz8904 (id numeric(9,0) primary key, data varchar2(50))");
            TestHelper.streamTable(connection, "dbz8904");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ8904")
                    .with(OracleConnectorConfig.LOG_MINING_CLIENTID_INCLUDE_LIST, "abc")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            LogInterceptor logInterceptor = new LogInterceptor(AbstractLogMinerEventProcessor.class);
            logInterceptor.setLoggerLevel(AbstractLogMinerEventProcessor.class, Level.DEBUG);

            try (OracleConnection testConnection = TestHelper.testConnection()) {
                testConnection.connection().setClientInfo("OCSID.CLIENTID", "abc");
                testConnection.execute("INSERT INTO dbz8904 (id,data) values (1,'abc')");
            }

            try (OracleConnection testConnection = TestHelper.testConnection()) {
                testConnection.connection().setClientInfo("OCSID.CLIENTID", "xyz");
                testConnection.execute("INSERT INTO dbz8904 (id,data) values (2,'xyz')");
            }

            connection.execute("INSERT INTO dbz8904 (id,data) values (3,'none')");

            List<SourceRecord> records = consumeRecordsByTopic(1).recordsForTopic("server1.DEBEZIUM.DBZ8904");
            assertThat(records).hasSize(1);
            assertThat(getAfter(records.get(0)).get("ID")).isEqualTo(1);
            assertThat(getAfter(records.get(0)).get("DATA")).isEqualTo("abc");

            assertThat(logInterceptor.containsMessage("Skipped transaction with excluded client id xyz")).isFalse();

            assertNoRecordsToConsume();
        }
        finally {
            TestHelper.dropTable(connection, "dbz8904");
        }
    }

    @Test
    @FixFor("DBZ-8904")
    public void shouldThrowConfigurationExceptionWhenClientIdIncludeExcludeBothSpecified() throws Exception {
        TestHelper.dropTable(connection, "dbz8904");
        try {
            connection.execute("CREATE TABLE dbz8904 (id numeric(9,0) primary key, data varchar2(50))");
            TestHelper.streamTable(connection, "dbz8904");

            LogInterceptor logInterceptor = new LogInterceptor(ClientIdFilterIT.class);

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ8904")
                    .with(OracleConnectorConfig.LOG_MINING_CLIENTID_INCLUDE_LIST, "abc")
                    .with(OracleConnectorConfig.LOG_MINING_CLIENTID_EXCLUDE_LIST, "xyz")
                    .build();

            start(OracleConnector.class, config);

            Awaitility.await()
                    .atMost(Duration.ofSeconds(TestHelper.defaultMessageConsumerPollTimeout()))
                    .until(() -> logInterceptor.containsErrorMessage("Connector configuration is not valid. The " +
                            "'log.mining.clientid.exclude.list' value is invalid: \"log.mining.clientid.include.list\": is already specified"));
        }
        finally {
            TestHelper.dropTable(connection, "dbz8904");
        }
    }

    private static Struct getAfter(SourceRecord record) {
        return ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
    }
}
