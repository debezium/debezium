/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.assertions;

import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_MYSQL_PASSWORD;
import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_MYSQL_USERNAME;
import static io.debezium.testing.system.tools.WaitConditions.scaled;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import org.awaitility.core.ThrowingRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.system.tools.databases.SqlDatabaseClient;
import io.debezium.testing.system.tools.databases.SqlDatabaseController;

public class JdbcAssertions {
    SqlDatabaseController databaseController;
    Logger LOGGER = LoggerFactory.getLogger(JdbcAssertions.class);

    public static void awaitAssert(ThrowingRunnable assertion) {
        await()
                .pollDelay(5, TimeUnit.SECONDS)
                .pollInterval(10, TimeUnit.SECONDS)
                .atMost(scaled(1), TimeUnit.MINUTES)
                .untilAsserted(assertion);
    }

    public JdbcAssertions(SqlDatabaseController databaseController) {
        this.databaseController = databaseController;
    }

    public void assertRowsCount(int expectedCount, String table) throws SQLException {
        SqlDatabaseClient client = databaseController.getDatabaseClient(DATABASE_MYSQL_USERNAME, DATABASE_MYSQL_PASSWORD);
        String sql = "SELECT count(*) FROM " + table;
        int databaseCount = client.executeQuery("inventory", sql, rs -> {
            try {
                rs.next();
                return rs.getInt(1);
            }
            catch (SQLException e) {
                throw new AssertionError(e);
            }
        });
        assertThat(databaseCount).withFailMessage("Expecting table '%s' to have <%d> rows but it had <%d>.", table, expectedCount, databaseCount)
                .isEqualTo(expectedCount);
    }

    public void assertRowsContain(String table, String column, String content) throws SQLException {
        SqlDatabaseClient client = databaseController.getDatabaseClient(DATABASE_MYSQL_USERNAME, DATABASE_MYSQL_PASSWORD);
        String sql = String.format("SELECT * FROM %s WHERE %s = \"%s\"", table, column, content);
        boolean containsContent = client.executeQuery("inventory", sql, rs -> {
            try {
                return rs.next();
            }
            catch (SQLException e) {
                throw new AssertionError(e);
            }
        });
        assertThat(containsContent).withFailMessage("Table '%s' does not contain row with column '%s' containing <%s>.", table, column, content).isTrue();
    }

}
