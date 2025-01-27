/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration.db2;

import java.sql.SQLException;

import org.assertj.db.api.TableAssert;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.debezium.connector.jdbc.integration.AbstractJdbcSinkDeleteEnabledTest;
import io.debezium.connector.jdbc.junit.TestHelper;
import io.debezium.connector.jdbc.junit.jupiter.Db2SinkDatabaseContextProvider;
import io.debezium.connector.jdbc.junit.jupiter.Sink;

/**
 * Delete enabled tests for DB2.
 *
 * @author Chris Cranford
 */
@Tag("all")
@Tag("it")
@Tag("it-db2")
@ExtendWith(Db2SinkDatabaseContextProvider.class)
public class JdbcSinkDeleteEnabledIT extends AbstractJdbcSinkDeleteEnabledTest {

    public JdbcSinkDeleteEnabledIT(Sink sink) {
        super(sink);
    }

    @Test
    public void testShouldHandleTruncateTableStatementWithoutHibernate() throws SQLException {
        Sink sink = getSink();
        sink.execute("create table TEST_TRUNCATE_DB2_TABLE(id int not null, name varchar(255), primary key(id))");
        sink.execute("insert into TEST_TRUNCATE_DB2_TABLE(id,name) values(1,'jdbc')");

        TableAssert tableAssert = TestHelper.assertTable(assertDbConnection(), "TEST_TRUNCATE_DB2_TABLE");
        tableAssert.exists().hasNumberOfRows(1).hasNumberOfColumns(2);

        sink.execute("truncate table TEST_TRUNCATE_DB2_TABLE IMMEDIATE");
        tableAssert = TestHelper.assertTable(assertDbConnection(), "TEST_TRUNCATE_DB2_TABLE");
        tableAssert.exists().hasNumberOfRows(0).hasNumberOfColumns(2);
    }

}
