/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.legacy;

import static org.fest.assertions.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;

import io.debezium.doc.FixFor;
import io.debezium.relational.TableId;

/**
 * @author Randall Hauch
 */
public class FiltersTest {

    private Configurator build;
    private Filters filters;

    @Before
    public void beforeEach() {
        build = new Configurator();
        filters = null;
    }

    @Test
    public void shouldAllowDatabaseListedWithLiteralInWhitelistAndNoDatabaseBlacklist() {
        filters = build.includeDatabases("connector_test").createFilters();
        assertDatabaseIncluded("connector_test");
        assertDatabaseExcluded("other");
        assertSystemDatabasesExcluded();
    }

    @Test
    public void shouldAllowDatabaseListedWithMultipleLiteralsInWhitelistAndNoDatabaseBlacklist() {
        filters = build.includeDatabases("connector_test,another_included").createFilters();
        assertDatabaseIncluded("connector_test");
        assertDatabaseIncluded("another_included");
        assertDatabaseExcluded("other");
        assertSystemDatabasesExcluded();
    }

    @Test
    public void shouldAllowDatabaseListedWithMultipleRegexInWhitelistAndNoDatabaseBlacklist() {
        filters = build.includeDatabases("connector.*_test,another_{1}.*").createFilters();
        assertDatabaseIncluded("connector_test");
        assertDatabaseIncluded("another_included");
        assertDatabaseIncluded("another__test");
        assertDatabaseExcluded("conn_test");
        assertDatabaseExcluded("connector-test");
        assertDatabaseExcluded("other");
        assertSystemDatabasesExcluded();
    }

    @Test
    public void shouldAllowDatabaseListedWithWildcardInWhitelistAndNoDatabaseBlacklist() {
        filters = build.includeDatabases(".*").createFilters();
        assertDatabaseIncluded("connector_test");
        assertDatabaseIncluded("another_included");
        assertDatabaseIncluded("other");
        assertSystemDatabasesExcluded();
    }

    @Test
    public void shouldAllowAllDatabaseExceptSystemWhenWhitelistIsBlank() {
        filters = build.includeDatabases("").createFilters();
        assertDatabaseIncluded("connector_test");
        assertDatabaseIncluded("other");
        assertSystemDatabasesExcluded();
    }

    @Test
    public void shouldNotAllowDatabaseListedWithLiteralInBlacklistAndNoDatabaseWhitelist() {
        filters = build.excludeDatabases("connector_test").createFilters();
        assertDatabaseExcluded("connector_test");
        assertDatabaseIncluded("other");
        assertSystemDatabasesExcluded();
    }

    @Test
    public void shouldNotAllowDatabaseListedWithMultipleLiteralsInBlacklistAndNoDatabaseWhitelist() {
        filters = build.excludeDatabases("connector_test,another_included").createFilters();
        assertDatabaseExcluded("connector_test");
        assertDatabaseExcluded("another_included");
        assertDatabaseIncluded("other");
        assertDatabaseIncluded("something-else");
        assertSystemDatabasesExcluded();
    }

    @Test
    public void shouldNotAllowDatabaseListedWithMultipleRegexInBlacklistAndNoDatabaseWhitelist() {
        filters = build.excludeDatabases("connector.*_test,another_{1}.*").createFilters();
        assertDatabaseExcluded("connector_test");
        assertDatabaseExcluded("another_included");
        assertDatabaseExcluded("another__test");
        assertDatabaseIncluded("conn_test");
        assertDatabaseIncluded("connector-test");
        assertDatabaseIncluded("other");
        assertSystemDatabasesExcluded();
    }

    @Test
    public void shouldNotAllowDatabaseListedWithWildcardInBlacklistAndNoDatabaseWhitelist() {
        filters = build.excludeDatabases(".*").createFilters();
        assertDatabaseExcluded("connector_test");
        assertDatabaseExcluded("another_included");
        assertDatabaseExcluded("other");
        assertSystemDatabasesExcluded();
    }

    @Test
    public void shouldRespectOnlyDatabaseWhitelistWithDatabaseBlacklistAlsoSpecified() {
        filters = build.includeDatabases("A,B,C,D.*").excludeDatabases("C,B,E").createFilters();
        assertDatabaseIncluded("A");
        assertDatabaseIncluded("B");
        assertDatabaseIncluded("C");
        assertDatabaseIncluded("D");
        assertDatabaseIncluded("D1");
        assertDatabaseIncluded("D_3");
        assertDatabaseExcluded("E");
        assertDatabaseExcluded("another__test");
        assertSystemDatabasesExcluded();
    }

    @Test
    public void shouldAllowAllDatabaseWhenBlacklistIsBlank() {
        filters = build.excludeDatabases("").createFilters();
        assertDatabaseIncluded("connector_test");
        assertDatabaseIncluded("other");
        assertSystemDatabasesExcluded();
    }

    @Test
    public void shouldIgnoreDatabaseBlacklistWhenDatabaseWhitelistIsNonEmpty() {
        filters = build.includeDatabases(".*").excludeDatabases("connector_test,other").createFilters();
        assertDatabaseIncluded("connector_test");
        assertDatabaseIncluded("other");
        assertDatabaseIncluded("something_else");
        assertSystemDatabasesExcluded();
    }

    @Test
    public void shouldAllowTableListedWithLiteralInWhitelistAndNoTableBlacklistWhenDatabaseIncluded() {
        filters = build.includeTables("connector_test.table1").createFilters();
        assertTableIncluded("connector_test.table1");
        assertTableExcluded("connector_test.table2");
        assertTableExcluded("connector_test.table3");
        assertDatabaseIncluded("connector_test");
        assertDatabaseIncluded("other_test");
        assertSystemTablesExcluded();
        assertSystemDatabasesExcluded();
    }

    @Test
    public void shouldAllowTableListedWithLiteralWithEscapedPeriodInWhitelistAndNoTableBlacklistWhenDatabaseIncluded() {
        filters = build.includeTables("connector_test[.]table1").createFilters();
        assertTableIncluded("connector_test.table1");
        assertTableExcluded("connector_test.table2");
        assertTableExcluded("connector_test.table3");
        assertDatabaseIncluded("connector_test");
        assertDatabaseIncluded("other_test");
        assertSystemTablesExcluded();
        assertSystemDatabasesExcluded();

        filters = build.includeTables("connector_test\\.table1").createFilters();
        assertTableIncluded("connector_test.table1");
        assertTableExcluded("connector_test.table2");
        assertTableExcluded("connector_test.table3");
        assertDatabaseIncluded("connector_test");
        assertDatabaseIncluded("other_test");
        assertSystemTablesExcluded();
        assertSystemDatabasesExcluded();
    }

    @Test
    public void shouldAllowTableListedWithMultipleLiteralsInWhitelistAndNoTableBlacklistWhenDatabaseIncluded() {
        filters = build.includeTables("connector_test.table1,connector_test.table2").createFilters();
        assertTableIncluded("connector_test.table1");
        assertTableIncluded("connector_test.table2");
        assertTableExcluded("connector_test.table3");
        assertDatabaseIncluded("connector_test");
        assertDatabaseIncluded("other_test");
        assertSystemTablesExcluded();
        assertSystemDatabasesExcluded();
    }

    @Test
    @FixFor("DBZ-1546")
    public void shouldAllowTableListedWithWhitespaceCharactersInWhitelistAndNoTableBlacklistWhenDatabaseIncluded() {
        filters = build.includeTables("connector_test.table1, connector_test.table2").createFilters();
        assertTableIncluded("connector_test.table1");
        assertTableIncluded("connector_test.table2");
        assertTableExcluded("connector_test.table3");
    }

    @Test
    public void shouldAllowTableListedWithMultipleRegexInWhitelistAndNoTableBlacklistWhenDatabaseIncluded() {
        filters = build.includeTables("connector_test.table[x]?1,connector_test[.](.*)2").createFilters();
        assertTableIncluded("connector_test.table1");
        assertTableIncluded("connector_test.table2");
        assertTableExcluded("connector_test.table3");
        assertDatabaseIncluded("connector_test");
        assertDatabaseIncluded("other_test");
        assertSystemTablesExcluded();
        assertSystemDatabasesExcluded();
    }

    @Test
    public void shouldAllowTableListedWithWildcardInWhitelistAndNoTableBlacklistWhenDatabaseIncluded() {
        filters = build.includeTables("connector_test[.](.*)").createFilters();
        assertTableIncluded("connector_test.table1");
        assertTableIncluded("connector_test.table2");
        assertTableIncluded("connector_test.table3");
        assertTableIncluded("connector_test.ABC");
        assertDatabaseIncluded("connector_test");
        assertDatabaseIncluded("other_test");
        assertSystemTablesExcluded();
        assertSystemDatabasesExcluded();
    }

    @FixFor("DBZ-242")
    @Test
    public void shouldAllowTableListedWithLiteralInWhitelistAndNoTableBlacklistWhenDatabaseIncludedButSystemTablesExcluded() {
        filters = build.includeTables("connector_test.table1,connector_test.table2").includeBuiltInTables().createFilters();
        assertTableIncluded("connector_test.table1");
        assertTableIncluded("connector_test.table2");
        assertTableExcluded("connector_test.table3");
        assertTableExcluded("other.table1");
        assertDatabaseIncluded("connector_test");
        assertDatabaseIncluded("other_test");
        assertSystemTablesExcluded(); // not specifically included
        assertSystemDatabasesIncluded();
    }

    @FixFor("DBZ-242")
    @Test
    public void shouldAllowTableListedWithLiteralInWhitelistAndTableWhitelistWhenDatabaseIncludedButSystemTablesIncluded() {
        filters = build.includeTables("connector_test.table1,connector_test.table2").includeDatabases("connector_test,mysql").includeBuiltInTables().createFilters();
        assertTableIncluded("connector_test.table1");
        assertTableIncluded("connector_test.table2");
        assertTableExcluded("connector_test.table3");
        assertTableExcluded("other.table1");
        assertDatabaseIncluded("connector_test");
        assertDatabaseExcluded("other_test");
        assertDatabaseIncluded("mysql");
        assertSystemTablesExcluded(); // not specifically included
    }

    @FixFor("DBZ-242")
    @Test
    public void shouldAllowTableListedWithLiteralInWhitelistAndNoTableBlacklistWhenDatabaseIncludedButSystemTablesIncluded() {
        filters = build.includeTables("connector_test.table1,connector_test.table2,mysql[.].*,performance_schema[.].*,sys[.].*,information_schema[.].*")
                .includeBuiltInTables().createFilters();
        assertTableIncluded("connector_test.table1");
        assertTableIncluded("connector_test.table2");
        assertTableExcluded("connector_test.table3");
        assertTableExcluded("other.table1");
        assertDatabaseIncluded("connector_test");
        assertDatabaseIncluded("other_test");
        assertSystemTablesIncluded(); // specifically included
        assertSystemDatabasesIncluded();
    }

    @Test
    public void shouldNotAllowTableWhenNotIncludedInDatabaseWhitelist() {
        filters = build.includeTables("db1.table1,db2.table1,db3.*").includeDatabases("db1,db3").createFilters();
        assertTableIncluded("db1.table1");
        assertTableExcluded("db1.table2");
        assertTableExcluded("db2.table1");
        assertTableExcluded("db2.table2");
        assertTableIncluded("db3.table1");
        assertTableIncluded("db3.table2");
        assertTableExcluded("db4.table1");
        assertTableExcluded("db4.table2");
        assertDatabaseIncluded("db1");
        assertDatabaseIncluded("db3");
        assertDatabaseExcluded("db2");
        assertSystemTablesExcluded();
        assertSystemDatabasesExcluded();
    }

    @Test
    public void shouldNotAllowTableWhenExcludedInDatabaseWhitelist() {
        filters = build.includeTables("db1.table1,db2.table1,db3.*").excludeDatabases("db2").createFilters();
        assertTableIncluded("db1.table1");
        assertTableExcluded("db1.table2"); // not explicitly included in the tables
        assertTableExcluded("db2.table1");
        assertTableExcluded("db2.table2");
        assertTableIncluded("db3.table1");
        assertTableIncluded("db3.table2");
        assertTableExcluded("db4.table1"); // not explicitly included in the tables
        assertTableExcluded("db4.table2"); // not explicitly included in the tables
        assertDatabaseIncluded("db1");
        assertDatabaseIncluded("db3");
        assertDatabaseExcluded("db2");
        assertSystemTablesExcluded();
        assertSystemDatabasesExcluded();
    }

    @Test
    @FixFor("DBZ-1939")
    public void shouldNotAllowIgnoredTable() {
        filters = build.includeBuiltInTables().createFilters();
        assertIgnoredTableExcluded("mysql.rds_configuration");
        assertNonIgnoredTableIncluded("mysql.table1");
    }

    protected void assertDatabaseIncluded(String databaseName) {
        assertThat(filters.databaseFilter().test(databaseName)).isTrue();
    }

    protected void assertDatabaseExcluded(String databaseName) {
        assertThat(filters.databaseFilter().test(databaseName)).isFalse();
    }

    protected void assertSystemDatabasesExcluded() {
        Filters.BUILT_IN_DB_NAMES.forEach(this::assertDatabaseExcluded);
    }

    protected void assertSystemDatabasesIncluded() {
        Filters.BUILT_IN_DB_NAMES.forEach(this::assertDatabaseIncluded);
    }

    protected void assertSystemTablesExcluded() {
        Filters.BUILT_IN_DB_NAMES.forEach(dbName -> {
            assertTableExcluded(dbName + ".foo");
        });
    }

    protected void assertSystemTablesIncluded() {
        Filters.BUILT_IN_DB_NAMES.forEach(dbName -> {
            assertTableIncluded(dbName + ".foo");
        });
    }

    protected void assertTableIncluded(String fullyQualifiedTableName) {
        TableId id = TableId.parse(fullyQualifiedTableName);
        assertThat(filters.tableFilter().test(id)).isTrue();
    }

    protected void assertTableExcluded(String fullyQualifiedTableName) {
        TableId id = TableId.parse(fullyQualifiedTableName);
        assertThat(filters.tableFilter().test(id)).isFalse();
    }

    protected void assertIgnoredTableExcluded(String fullyQualifiedTableName) {
        TableId id = TableId.parse(fullyQualifiedTableName);
        assertThat(filters.tableFilter().test(id)).isFalse();
        assertThat(filters.ignoredTableFilter().test(id)).isTrue();
    }

    protected void assertNonIgnoredTableIncluded(String fullyQualifiedTableName) {
        TableId id = TableId.parse(fullyQualifiedTableName);
        assertThat(filters.tableFilter().test(id)).isTrue();
        assertThat(filters.ignoredTableFilter().test(id)).isFalse();
    }
}
