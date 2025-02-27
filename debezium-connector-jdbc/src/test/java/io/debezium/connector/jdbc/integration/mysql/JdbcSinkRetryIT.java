/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration.mysql;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.hibernate.PessimisticLockException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import io.debezium.bindings.kafka.KafkaDebeziumSinkRecord;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.integration.AbstractJdbcSinkTest;
import io.debezium.connector.jdbc.junit.jupiter.MySqlSinkDatabaseContextProvider;
import io.debezium.connector.jdbc.junit.jupiter.Sink;
import io.debezium.connector.jdbc.junit.jupiter.SinkRecordFactoryArgumentsProvider;
import io.debezium.connector.jdbc.util.SinkRecordFactory;
import io.debezium.doc.FixFor;

/**
 * JDBC Sink tests for MySQL retry.
 *
 * @author Inki Hwang
 */
@Tag("all")
@Tag("it")
@Tag("it-mysql")
@ExtendWith(MySqlSinkDatabaseContextProvider.class)
public class JdbcSinkRetryIT extends AbstractJdbcSinkTest {

    public JdbcSinkRetryIT(Sink sink) {
        super(sink);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("DBZ-7291")
    public void testRetryToFlushBufferWhenRetriableExceptionOccurred(SinkRecordFactory factory) throws SQLException {
        String tableName = randomTableName();
        Connection connection = getSink().getConnection();

        // create table and insert initial data
        try (Statement st = connection.createStatement()) {
            st.execute("CREATE TABLE `" + tableName + "` (\n" +
                    "  `id` bigint(20) NOT NULL,\n" +
                    "  `content` varchar(200) DEFAULT NULL,\n" +
                    "  `dt` datetime DEFAULT NULL,\n" +
                    "  PRIMARY KEY (`id`),\n" +
                    "  KEY `dt_idx` (`dt`)" +
                    ")");

            st.execute("INSERT INTO " + tableName + " (`id`, `content`, `dt`) VALUES (1, 'c1', now()), (2, 'c2', now()), (3, 'c3', now())");
        }

        connection.setAutoCommit(false);
        Statement st = connection.createStatement();
        st.execute("START TRANSACTION");
        // acquire shared lock about PRIMARY index id=1
        st.execute("SELECT * FROM " + tableName + " WHERE id=1 LOCK IN SHARE MODE;");

        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_VALUE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_FIELDS, "id");
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.UPSERT.getValue());
        // rollback after 5 seconds waiting for lock.
        properties.put(JdbcSinkConnectorConfig.CONNECTION_URL, getSink().getJdbcUrl(Map.of("sessionVariables", "innodb_lock_wait_timeout=5")));
        properties.put(JdbcSinkConnectorConfig.COLLECTION_NAME_FORMAT, tableName);
        properties.put(JdbcSinkConnectorConfig.FLUSH_MAX_RETRIES, "1");
        properties.put(JdbcSinkConnectorConfig.FLUSH_RETRY_DELAY_MS, "1000");

        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String topicName = topicName("server1", "schema", tableName);

        final KafkaDebeziumSinkRecord updateRecord = factory.updateRecordWithSchemaValue(
                topicName,
                (byte) 1,
                "content",
                Schema.OPTIONAL_STRING_SCHEMA,
                "c11");
        try {
            // it waits the lock of PRIMARY index id=1 is released to acquire exclusive lock for update.
            // and exceeded innodb_lock_wait_timeout during each retry.
            consume(updateRecord);
            // consume again because the exception will be thrown next put method.
            consume(Collections.emptyList());
            fail();
        }
        catch (Exception e) {
            assertThat(e.getCause().getMessage()).matches(
                    "Exceeded max retries [0-9]* times, failed to process sink records");
            // PessimisticLockException exception is retriable in mysql dialect.
            assertEquals(e.getCause().getCause().getClass(), PessimisticLockException.class);
            assertThat(e.getCause().getCause().getCause().getMessage()).matches(
                    "Lock wait timeout exceeded; try restarting transaction");
        }
        finally {
            connection.close();
            stopSinkConnector();
        }
    }
}
