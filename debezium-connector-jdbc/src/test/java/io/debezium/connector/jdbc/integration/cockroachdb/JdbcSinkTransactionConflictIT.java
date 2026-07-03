/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration.cockroachdb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Schema;
import org.hibernate.exception.LockAcquisitionException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import io.debezium.connector.jdbc.JdbcKafkaSinkRecord;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.integration.AbstractJdbcSinkTest;
import io.debezium.connector.jdbc.junit.TestHelper;
import io.debezium.connector.jdbc.junit.jupiter.CockroachDbSinkDatabaseContextProvider;
import io.debezium.connector.jdbc.junit.jupiter.Sink;
import io.debezium.connector.jdbc.junit.jupiter.SinkRecordFactoryArgumentsProvider;
import io.debezium.connector.jdbc.util.SinkRecordFactory;
import io.debezium.doc.FixFor;

/**
 * Transaction conflict tests for CockroachDB.
 *
 * CockroachDB runs SERIALIZABLE isolation and reports conflicts through the SQLSTATE 40001/40P01/55P03
 * family, which Hibernate maps to {@code TransactionSerializationException} and
 * {@code LockAcquisitionException} (including its {@code LockTimeoutException} subclass). The
 * CockroachDB dialect registers these as retriable, so the sink's flush retry loop, governed by
 * {@code flush.max.retries} and {@code flush.retry.delay.ms}, transparently retries conflicting
 * flushes. These tests verify both sides of that contract: a conflicting flush recovers once the
 * contending transaction ends, and setting {@code flush.max.retries=0} disables retries and fails
 * fast.
 *
 * @author Virag Tripathi
 */
@Tag("all")
@Tag("it")
@Tag("it-cockroachdb")
@ExtendWith(CockroachDbSinkDatabaseContextProvider.class)
public class JdbcSinkTransactionConflictIT extends AbstractJdbcSinkTest {

    private static final String LOCK_TIMEOUT_OPTIONS = "-c lock_timeout=500ms";

    public JdbcSinkTransactionConflictIT(Sink sink) {
        super(sink);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("DBZ-2180")
    public void testConflictingFlushRecoversWithRetriesEnabled(SinkRecordFactory factory) throws SQLException {
        final String tableName = randomTableName();
        final Connection connection = getSink().getConnection();

        try (Statement st = connection.createStatement()) {
            st.execute("CREATE TABLE " + tableName + " (id int8 NOT NULL, content varchar(200) DEFAULT NULL, PRIMARY KEY (id))");
            st.execute("INSERT INTO " + tableName + " (id, content) VALUES (1, 'c1'), (2, 'c2'), (3, 'c3')");
        }

        // Hold an exclusive lock on the row the sink is about to upsert.
        connection.setAutoCommit(false);
        try (Statement st = connection.createStatement()) {
            st.execute("SELECT * FROM " + tableName + " WHERE id = 1 FOR UPDATE");
        }

        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_VALUE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_FIELDS, "id");
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.UPSERT.getValue());
        properties.put(JdbcSinkConnectorConfig.COLLECTION_NAME_FORMAT, tableName);
        // Fail lock waits quickly so each flush attempt surfaces a retriable conflict instead of blocking.
        properties.put(JdbcSinkConnectorConfig.CONNECTION_URL, getSink().getJdbcUrl(Map.of("options", LOCK_TIMEOUT_OPTIONS)));
        properties.put(JdbcSinkConnectorConfig.FLUSH_MAX_RETRIES, "10");
        properties.put(JdbcSinkConnectorConfig.FLUSH_RETRY_DELAY_MS, "500");

        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String topicName = topicName("server1", "schema", tableName);

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);
        final JdbcKafkaSinkRecord updateRecord = factory.updateRecordWithSchemaValue(
                topicName,
                (byte) 1,
                "content",
                Schema.OPTIONAL_STRING_SCHEMA,
                "c11",
                config);

        // Release the lock while the sink is retrying, so a later attempt succeeds.
        final CompletableFuture<Void> lockRelease = CompletableFuture.runAsync(() -> {
            try {
                connection.rollback();
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }, CompletableFuture.delayedExecutor(2, TimeUnit.SECONDS));

        try {
            consume(updateRecord);
            consume(Collections.emptyList());

            lockRelease.join();
            TestHelper.assertTable(assertDbConnection(), tableName)
                    .exists()
                    .hasNumberOfRows(3)
                    .column("content").hasValues("c11", "c2", "c3");
        }
        finally {
            connection.close();
            stopSinkConnector();
        }
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("DBZ-2180")
    public void testConflictingFlushFailsFastWithRetriesDisabled(SinkRecordFactory factory) throws SQLException {
        final String tableName = randomTableName();
        final Connection connection = getSink().getConnection();

        try (Statement st = connection.createStatement()) {
            st.execute("CREATE TABLE " + tableName + " (id int8 NOT NULL, content varchar(200) DEFAULT NULL, PRIMARY KEY (id))");
            st.execute("INSERT INTO " + tableName + " (id, content) VALUES (1, 'c1'), (2, 'c2'), (3, 'c3')");
        }

        // Hold an exclusive lock on the row the sink is about to upsert for the whole test.
        connection.setAutoCommit(false);
        try (Statement st = connection.createStatement()) {
            st.execute("SELECT * FROM " + tableName + " WHERE id = 1 FOR UPDATE");
        }

        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_VALUE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_FIELDS, "id");
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.UPSERT.getValue());
        properties.put(JdbcSinkConnectorConfig.COLLECTION_NAME_FORMAT, tableName);
        properties.put(JdbcSinkConnectorConfig.CONNECTION_URL, getSink().getJdbcUrl(Map.of("options", LOCK_TIMEOUT_OPTIONS)));
        // Retries disabled: the first conflicting attempt must fail the flush.
        properties.put(JdbcSinkConnectorConfig.FLUSH_MAX_RETRIES, "0");
        properties.put(JdbcSinkConnectorConfig.FLUSH_RETRY_DELAY_MS, "100");

        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String topicName = topicName("server1", "schema", tableName);

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);
        final JdbcKafkaSinkRecord updateRecord = factory.updateRecordWithSchemaValue(
                topicName,
                (byte) 1,
                "content",
                Schema.OPTIONAL_STRING_SCHEMA,
                "c11",
                config);

        try {
            consume(updateRecord);
            // The flush failure is rethrown by the following put.
            consume(Collections.emptyList());
            fail();
        }
        catch (Exception e) {
            assertThat(e.getCause().getMessage()).matches(
                    "Exceeded max retries 0 times, failed to flush records for table '" + tableName + "'");
            // The conflict surfaces as a retriable CockroachDB conflict exception.
            assertThat(e.getCause().getCause()).isInstanceOf(LockAcquisitionException.class);
        }
        finally {
            connection.rollback();
            connection.close();
            stopSinkConnector();
        }
    }

}
