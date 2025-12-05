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

    static class FakeTask extends PostgresConnectorTask {
        @Override
        protected ReplicationConnection buildReplicationConnection(PostgresConnection jdbcConnection, PostgresSchema schema, PostgresConnectorConfig connectorConfig)
                throws SQLException {
            throw new SQLException("Could not connect");
        }
    }

    @Test(expected = ConnectException.class)
    @FixFor("DBZ-1426")
    public void retryOnFailureToCreateConnection() throws Exception {
        FakeTask postgresConnectorTask = new FakeTask();
        PostgresConnectorConfig config = new PostgresConnectorConfig(TestHelper.defaultConfig().build());
        long startTime = System.currentTimeMillis();
        postgresConnectorTask.createReplicationConnection(config, 3, Duration.ofSeconds(2));

        // Verify retry happened for 10 seconds
        long endTime = System.currentTimeMillis();
        long timeElapsed = endTime - startTime;
        Assert.assertTrue(timeElapsed > 5);
    }
}
