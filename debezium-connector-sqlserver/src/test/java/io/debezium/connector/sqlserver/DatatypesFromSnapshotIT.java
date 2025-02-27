/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.sql.SQLException;

import org.junit.Before;
import org.junit.BeforeClass;

import io.debezium.jdbc.TemporalPrecisionMode;

/**
 * Integration test to verify different SQL Server datatypes.
 * The types are discovered during snapshotting phase.
 *
 * @author Jiri Pechanec
 */
public class DatatypesFromSnapshotIT extends AbstractSqlServerDatatypesTest {

    @BeforeClass
    public static void beforeClass() throws SQLException {
        AbstractSqlServerDatatypesTest.beforeClass();

        createTables();

        insertIntTypes();
        insertFpTypes();
        insertStringTypes();
        insertTimeTypes();
        insertXmlTypes();
    }

    @Before
    public void before() throws Exception {
        init(TemporalPrecisionMode.ADAPTIVE, true);
    }
}
