/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import java.sql.SQLException;
import java.time.Duration;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Assert;
import org.junit.Test;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.doc.FixFor;
import io.debezium.schema.SchemaTopicNamingStrategy;
import io.debezium.spi.topic.TopicNamingStrategy;

/**
 * Integration test for {@link PostgresConnectorTask} class.
 */
public class PostgresConnectorTaskIT {

    @Test
    @FixFor("DBZ-519")
    public void shouldNotThrowNullPointerExceptionDuringCommit() throws Exception {
        PostgresConnectorTask postgresConnectorTask = new PostgresConnectorTask();
        postgresConnectorTask.commit();
    }

    class FakeContext extends PostgresTaskContext {
        FakeContext(PostgresConnectorConfig postgresConnectorConfig, PostgresSchema postgresSchema) {
            super(postgresConnectorConfig, postgresSchema, null);
        }

        @Override
        protected ReplicationConnection createReplicationConnection(PostgresConnection jdbcConnection) throws SQLException {
            throw new SQLException("Could not connect");
        }
    }

    @Test(expected = ConnectException.class)
    @FixFor("DBZ-1426")
    public void retryOnFailureToCreateConnection() throws Exception {
        PostgresConnectorTask postgresConnectorTask = new PostgresConnectorTask();
        PostgresConnectorConfig config = new PostgresConnectorConfig(TestHelper.defaultConfig().build());
        long startTime = System.currentTimeMillis();
        postgresConnectorTask.createReplicationConnection(new FakeContext(config, new PostgresSchema(
                config,
                null,
                (TopicNamingStrategy) SchemaTopicNamingStrategy.create(config), null)), 3, Duration.ofSeconds(2));

        // Verify retry happened for 10 seconds
        long endTime = System.currentTimeMillis();
        long timeElapsed = endTime - startTime;
        Assert.assertTrue(timeElapsed > 5);
    }
}
