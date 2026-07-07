/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.function.Predicate;

import org.junit.jupiter.api.Test;

/**
 * @author Randall Hauch
 */
public class SelectorsTest {

    private Predicate<TableId> filter;

    @Test
    public void shouldCreateFilterWithAllLists() {
        filter = Selectors.tableSelector()
                .includeDatabases("connector_test")
                .excludeDatabases("")
                .includeTables("")
                .excludeTables("")
                .build();
        assertAllowed(filter, "connector_test", "A");
        assertAllowed(filter, "connector_test", "B");
        assertNotAllowed(filter, "other_test", "A");
        assertNotAllowed(filter, "other_test", "B");
    }

    @Test
    public void shouldCreateFilterWithDatabaseAllowlistAndTableAllowlist() {
        filter = Selectors.tableSelector()
                .includeDatabases("db1,db2")
                .includeTables("db1\\.A,db1\\.B,db2\\.C")
                .build();

        assertAllowed(filter, "db1", "A");
        assertAllowed(filter, "db1", "B");
        assertNotAllowed(filter, "db1", "D");
        assertNotAllowed(filter, "db1", "E");
        assertNotAllowed(filter, "db1", "F");

        assertAllowed(filter, "db2", "C");
        assertNotAllowed(filter, "db2", "G");
        assertNotAllowed(filter, "db2", "H");

        assertNotAllowed(filter, "db3", "A");
        assertNotAllowed(filter, "db4", "A");
    }

    @Test
    public void shouldCreateFilterWithDatabaseAllowlistAndTableBlocklist() {
        filter = Selectors.tableSelector()
                .includeDatabases("db1,db2")
                .excludeTables("db1\\.A,db1\\.B,db2\\.C")
                .build();

        assertNotAllowed(filter, "db1", "A");
        assertNotAllowed(filter, "db1", "B");
        assertAllowed(filter, "db1", "D");
        assertAllowed(filter, "db1", "E");
        assertAllowed(filter, "db1", "F");

        assertNotAllowed(filter, "db2", "C");
        assertAllowed(filter, "db2", "G");
        assertAllowed(filter, "db2", "H");

        assertNotAllowed(filter, "db3", "A");
        assertNotAllowed(filter, "db4", "A");
    }

    @Test
    public void shouldCreateFilterWithDatabaseBlocklistAndTableAllowlist() {
        filter = Selectors.tableSelector()
                .excludeDatabases("db3,db4")
                .includeTables("db1\\.A,db1\\.B,db2\\.C")
                .build();

        assertAllowed(filter, "db1", "A");
        assertAllowed(filter, "db1", "B");
        assertNotAllowed(filter, "db1", "D");
        assertNotAllowed(filter, "db1", "E");
        assertNotAllowed(filter, "db1", "F");

        assertAllowed(filter, "db2", "C");
        assertNotAllowed(filter, "db2", "G");
        assertNotAllowed(filter, "db2", "H");

        assertNotAllowed(filter, "db3", "A");
        assertNotAllowed(filter, "db4", "A");
    }

    @Test
    public void shouldCreateFilterWithDatabaseBlocklistAndTableBlocklist() {
        filter = Selectors.tableSelector()
                .excludeDatabases("db3,db4")
                .excludeTables("db1\\.A,db1\\.B,db2\\.C")
                .build();

        assertNotAllowed(filter, "db1", "A");
        assertNotAllowed(filter, "db1", "B");
        assertAllowed(filter, "db1", "D");
        assertAllowed(filter, "db1", "E");
        assertAllowed(filter, "db1", "F");

        assertNotAllowed(filter, "db2", "C");
        assertAllowed(filter, "db2", "G");
        assertAllowed(filter, "db2", "H");

        assertNotAllowed(filter, "db3", "A");
        assertNotAllowed(filter, "db4", "A");
    }

    @Test
    public void shouldCreateFilterWithNoDatabaseFilterAndTableAllowlist() {
        filter = Selectors.tableSelector()
                .includeTables("db1\\.A,db1\\.B,db2\\.C")
                .build();

        assertAllowed(filter, "db1", "A");
        assertAllowed(filter, "db1", "B");
        assertNotAllowed(filter, "db1", "D");
        assertNotAllowed(filter, "db1", "E");
        assertNotAllowed(filter, "db1", "F");

        assertAllowed(filter, "db2", "C");
        assertNotAllowed(filter, "db2", "G");
        assertNotAllowed(filter, "db2", "H");

        assertNotAllowed(filter, "db3", "A");
        assertNotAllowed(filter, "db4", "A");
    }

    @Test
    public void shouldCreateFilterWithNoDatabaseFilterAndTableBlocklist() {
        filter = Selectors.tableSelector()
                .excludeTables("db1\\.A,db1\\.B,db2\\.C")
                .build();

        assertNotAllowed(filter, "db1", "A");
        assertNotAllowed(filter, "db1", "B");
        assertAllowed(filter, "db1", "D");
        assertAllowed(filter, "db1", "E");
        assertAllowed(filter, "db1", "F");

        assertNotAllowed(filter, "db2", "C");
        assertAllowed(filter, "db2", "G");
        assertAllowed(filter, "db2", "H");

        assertAllowed(filter, "db3", "A");
        assertAllowed(filter, "db4", "A");
    }

    @Test
    public void shouldCreateFilterWithDatabaseAllowlistAndNoTableFilter() {
        filter = Selectors.tableSelector()
                .includeDatabases("db1,db2")
                .build();

        assertAllowed(filter, "db1", "A");
        assertAllowed(filter, "db2", "A");
        assertNotAllowed(filter, "db3", "A");
        assertNotAllowed(filter, "db4", "A");
    }

    @Test
    public void shouldCreateFilterWithDatabaseBlocklistAndNoTableFilter() {
        filter = Selectors.tableSelector()
                .excludeDatabases("db1,db2")
                .build();

        assertNotAllowed(filter, "db1", "A");
        assertNotAllowed(filter, "db2", "A");
        assertAllowed(filter, "db3", "A");
        assertAllowed(filter, "db4", "A");
    }

    @Test
    public void shouldCreateFilterWithSchemaBlocklistAndNoTableFilter() {
        filter = Selectors.tableSelector()
                .excludeSchemas("sc1,sc2")
                .build();

        assertNotAllowed(filter, "db1", "sc1", "A");
        assertNotAllowed(filter, "db2", "sc2", "A");
        assertAllowed(filter, "db1", "sc3", "A");
        assertAllowed(filter, "db2", "sc4", "A");
    }

    @Test
    public void shouldCreateFilterWithSchemaAllowlistAndNoTableFilter() {
        filter = Selectors.tableSelector()
                .includeSchemas("sc1,sc2")
                .build();

        assertAllowed(filter, "db1", "sc1", "A");
        assertAllowed(filter, "db2", "sc2", "A");
        assertNotAllowed(filter, "db1", "sc3", "A");
        assertNotAllowed(filter, "db2", "sc4", "A");
    }

    @Test
    public void shouldCreateFilterWithSchemaAllowlistAndTableAllowlist() {
        filter = Selectors.tableSelector()
                .includeSchemas("sc1,sc2")
                .includeTables("db\\.sc1\\.A,db\\.sc2\\.B")
                .build();

        assertAllowed(filter, "db", "sc1", "A");
        assertNotAllowed(filter, "db", "sc1", "B");
        assertAllowed(filter, "db", "sc2", "B");
        assertNotAllowed(filter, "db", "sc2", "A");
        assertNotAllowed(filter, "db", "sc1", "C");
        assertNotAllowed(filter, "db2", "sc2", "D");
        assertNotAllowed(filter, "db", "sc3", "A");
        assertNotAllowed(filter, "db2", "sc4", "B");
    }

    @Test
    public void shouldCreateFilterWithEscapedDatabaseNamesContainingSpecialCharacters() {
        // Test escaped literals: users must escape special regex chars to match literals
        filter = Selectors.tableSelector()
                .includeDatabases("Test\\$user,db\\[1\\],db\\.2")
                .build();

        assertAllowed(filter, "Test$user", "A");
        assertAllowed(filter, "db[1]", "A");
        assertAllowed(filter, "db.2", "A");
        assertNotAllowed(filter, "other_db", "A");
    }

    @Test
    public void shouldCreateFilterWithDatabaseRegexPatterns() {
        // Test unescaped patterns: users don't escape to use patterns
        filter = Selectors.tableSelector()
                .includeDatabases("db.*,prod_.*,test.*,db[0-9]")
                .build();

        assertAllowed(filter, "db", "A");
        assertAllowed(filter, "db1", "A");
        assertAllowed(filter, "dbtest", "A");
        assertAllowed(filter, "prod_main", "A");
        assertAllowed(filter, "prod_backup", "A");
        assertAllowed(filter, "test1", "A");
        assertAllowed(filter, "db0", "A");
        assertAllowed(filter, "db5", "A");
        assertNotAllowed(filter, "other_db", "A");
    }

    @Test
    public void shouldCreateFilterWithMixedEscapedLiteralsAndPatterns() {
        // Mix of escaped literals and unescaped patterns
        filter = Selectors.tableSelector()
                .includeDatabases("Test\\$user,db.*,prod_main,prod[0-9]+")
                .build();

        // Escaped literal matches exactly
        assertAllowed(filter, "Test$user", "A");

        // Unescaped patterns work as regex
        assertAllowed(filter, "db1", "A");
        assertAllowed(filter, "db_test", "A");
        assertAllowed(filter, "dbany", "A");

        // Literal string matches
        assertAllowed(filter, "prod_main", "A");

        // Pattern [0-9]+ matches one or more digits
        assertAllowed(filter, "prod1", "A");
        assertAllowed(filter, "prod123", "A");

        // These don't match
        assertNotAllowed(filter, "prod", "A");
        assertNotAllowed(filter, "prod_backup", "A");
        assertNotAllowed(filter, "other_db", "A");
    }

    @Test
    public void shouldEscapeAllMetacharactersInDatabaseNames() {
        // Test that all regex metacharacters can be escaped to match literals
        filter = Selectors.tableSelector()
                .includeDatabases("test\\$,test\\.,test\\[1\\],test\\(inner\\),test\\{v1\\},test\\|,test\\^,test\\-,multi\\{v1\\}\\.db\\$prod")
                .build();

        // Individual escaped metacharacters
        assertAllowed(filter, "test$", "A");
        assertAllowed(filter, "test.", "A");
        assertAllowed(filter, "test[1]", "A");
        assertAllowed(filter, "test(inner)", "A");
        assertAllowed(filter, "test{v1}", "A");
        assertAllowed(filter, "test|", "A");
        assertAllowed(filter, "test^", "A");
        assertAllowed(filter, "test-", "A");

        // Multiple escapes in one value
        assertAllowed(filter, "multi{v1}.db$prod", "A");

        assertNotAllowed(filter, "testX", "A");
    }

    @Test
    public void shouldHandleEscapingForSchemasAndExclusions() {
        // Test escaping works for schemas and exclude filters
        filter = Selectors.tableSelector()
                .includeSchemas("pub\\$lic,sys\\[core\\]")
                .build();

        // Schema filtering with escapes
        assertAllowed(filter, "mydb", "pub$lic", "tbl");
        assertAllowed(filter, "mydb", "sys[core]", "tbl");
        assertNotAllowed(filter, "mydb", "public", "tbl");

        // Database exclusion with escapes
        filter = Selectors.tableSelector()
                .includeDatabases("test,sys,other")
                .excludeDatabases("test\\$backup,sys\\[temp\\]")
                .build();

        assertNotAllowed(filter, "test$backup", "tbl");
        assertNotAllowed(filter, "sys[temp]", "tbl");
        assertAllowed(filter, "test", "tbl");
    }

    @Test
    public void shouldPreserveUnescapedRegexPatterns() {
        // Test that unescaped patterns still work as regex
        filter = Selectors.tableSelector()
                .includeDatabases("db.*,test[0-9],prod_.*")
                .build();

        // Regex patterns without escapes
        assertAllowed(filter, "db", "tbl");
        assertAllowed(filter, "db_main", "tbl");
        assertAllowed(filter, "test0", "tbl");
        assertAllowed(filter, "test9", "tbl");
        assertAllowed(filter, "prod_backup", "tbl");

        assertNotAllowed(filter, "testX", "tbl");
    }

    @Test
    public void shouldMixEscapedLiteralsAndPatterns() {
        // Test configuration with both escaped literals and regex patterns
        filter = Selectors.tableSelector()
                .includeDatabases("user\\$db,app_[a-z]+,sys\\[internal\\],prod.*")
                .build();

        // Escaped literals
        assertAllowed(filter, "user$db", "A");
        assertAllowed(filter, "sys[internal]", "A");

        // Regex patterns
        assertAllowed(filter, "app_main", "A");
        assertAllowed(filter, "app_service", "A");
        assertAllowed(filter, "prod", "A");
        assertAllowed(filter, "prod_backup", "A");

        assertNotAllowed(filter, "user", "A");
        assertNotAllowed(filter, "app_1", "A");
    }

    protected void assertAllowed(Predicate<TableId> filter, String dbName, String tableName) {
        TableId id = new TableId(dbName, null, tableName);
        assertThat(filter.test(id)).isTrue();
    }

    protected void assertAllowed(Predicate<TableId> filter, String dbName, String schemaName, String tableName) {
        TableId id = new TableId(dbName, schemaName, tableName);
        assertThat(filter.test(id)).isTrue();
    }

    protected void assertNotAllowed(Predicate<TableId> filter, String dbName, String tableName) {
        TableId id = new TableId(dbName, null, tableName);
        assertThat(filter.test(id)).isFalse();
    }

    protected void assertNotAllowed(Predicate<TableId> filter, String dbName, String schemaName, String tableName) {
        TableId id = new TableId(dbName, schemaName, tableName);
        assertThat(filter.test(id)).isFalse();
    }
}
