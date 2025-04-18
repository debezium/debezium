/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import java.sql.SQLException;
import java.sql.SQLTimeoutException;

import org.junit.Test;

import io.debezium.connector.binlog.BinlogConnectionIT;
import io.debezium.connector.binlog.util.BinlogTestConnection;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;

/**
 * @author Chris Cranford
 */
public class ConnectionIT extends BinlogConnectionIT<MariaDbConnector> implements MariaDbCommon {

    @Test
    public void whenQueryTakesMoreThenConfiguredQueryTimeoutAnExceptionMustBeThrown() throws SQLException {

        final UniqueDatabase DATABASE = TestHelper.getUniqueDatabase("readbinlog", "readbinlog_test");
        DATABASE.createAndInitialize();
        try (BinlogTestConnection conn = getTestDatabaseConnection(DATABASE.getDatabaseName(), 1000)) {
            conn.connect();

            assertThatThrownBy(() -> conn.execute("SELECT SLEEP(10)"))
                    .isInstanceOf(SQLTimeoutException.class)
                    .hasMessageContaining("Query execution was interrupted (max_statement_time exceeded)");

        }
    }
}
