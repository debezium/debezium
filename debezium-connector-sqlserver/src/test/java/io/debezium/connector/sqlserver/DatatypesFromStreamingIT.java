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
 * The types are discovered during streaming phase.
 *
 * @author Chris Cranford
 */
public class DatatypesFromStreamingIT extends AbstractSqlServerDatatypesTest {

    @BeforeClass
    public static void beforeClass() throws SQLException {
        AbstractSqlServerDatatypesTest.beforeClass();
    }

    @Before
    public void before() throws Exception {
        dropAllTables();
        createTables();
        init(TemporalPrecisionMode.ADAPTIVE, false);
    }
}
