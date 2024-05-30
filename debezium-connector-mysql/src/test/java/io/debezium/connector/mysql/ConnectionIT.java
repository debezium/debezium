/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import java.sql.SQLException;

import org.junit.Test;

import com.mysql.cj.jdbc.exceptions.MySQLTimeoutException;

import io.debezium.connector.binlog.BinlogConnectionIT;
import io.debezium.connector.binlog.util.BinlogTestConnection;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;

public class ConnectionIT extends BinlogConnectionIT<MySqlConnector> implements MySqlCommon {

    @Test
    public void whenQueryTakesMoreThenConfiguredQueryTimeoutAnExceptionMustBeThrown() throws SQLException {

        final UniqueDatabase DATABASE = TestHelper.getUniqueDatabase("readbinlog", "readbinlog_test");
        DATABASE.createAndInitialize();
        try (BinlogTestConnection conn = getTestDatabaseConnection(DATABASE.getDatabaseName(), 1000)) {
            conn.connect();

            assertThatThrownBy(() -> conn.execute("SELECT SLEEP(10)"))
                    .isInstanceOf(MySQLTimeoutException.class)
                    .hasMessage("Statement cancelled due to timeout or client request");
        }
    }
}
