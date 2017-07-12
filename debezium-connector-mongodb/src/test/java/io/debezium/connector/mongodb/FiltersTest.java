/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.fest.assertions.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;

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
    public void shouldIncludeDatabaseCoveredByLiteralInWhitelist() {
        filters = build.includeDatabases("db1").createFilters();
        assertThat(filters.databaseFilter().test("db1")).isTrue();
    }

    @Test
    public void shouldIncludeDatabaseCoveredByMultipleLiteralsInWhitelist() {
        filters = build.includeDatabases("db1,db2").createFilters();
        assertThat(filters.databaseFilter().test("db1")).isTrue();
        assertThat(filters.databaseFilter().test("db2")).isTrue();
    }

    @Test
    public void shouldIncludeDatabaseCoveredByWildcardInWhitelist() {
        filters = build.includeDatabases("db.*").createFilters();
        assertThat(filters.databaseFilter().test("db1")).isTrue();
    }

    @Test
    public void shouldIncludeDatabaseCoveredByMultipleWildcardsInWhitelist() {
        filters = build.includeDatabases("db.*,mongo.*").createFilters();
        assertThat(filters.databaseFilter().test("db1")).isTrue();
        assertThat(filters.databaseFilter().test("mongo2")).isTrue();
    }

    @Test
    public void shouldExcludeDatabaseCoveredByLiteralInBlacklist() {
        filters = build.excludeDatabases("db1").createFilters();
        assertThat(filters.databaseFilter().test("db1")).isFalse();
    }

    @Test
    public void shouldExcludeDatabaseCoveredByMultipleLiteralsInBlacklist() {
        filters = build.excludeDatabases("db1,db2").createFilters();
        assertThat(filters.databaseFilter().test("db1")).isFalse();
        assertThat(filters.databaseFilter().test("db2")).isFalse();
    }

    @Test
    public void shouldNotExcludeDatabaseNotCoveredByLiteralInBlacklist() {
        filters = build.excludeDatabases("db1").createFilters();
        assertThat(filters.databaseFilter().test("db2")).isTrue();
    }

    @Test
    public void shouldExcludeDatabaseCoveredByWildcardInBlacklist() {
        filters = build.excludeDatabases("db.*").createFilters();
        assertThat(filters.databaseFilter().test("db1")).isFalse();
    }

    @Test
    public void shouldExcludeDatabaseCoveredByMultipleWildcardsInBlacklist() {
        filters = build.excludeDatabases("db.*,mongo.*").createFilters();
        assertThat(filters.databaseFilter().test("db1")).isFalse();
        assertThat(filters.databaseFilter().test("mongo2")).isFalse();
    }

    @Test
    public void shouldIncludeCollectionCoveredByLiteralWithPeriodAsWildcardInWhitelistAndNoBlacklist() {
        filters = build.includeCollections("db1.coll[.]?ection[x]?A,db1[.](.*)B").createFilters();
        assertCollectionIncluded("db1xcoll.ectionA"); // first '.' is an unescaped wildcard in regex
        assertCollectionIncluded("db1.collectionA");
    }

    @Test
    public void shouldIncludeCollectionCoveredByLiteralInWhitelistAndNoBlacklist() {
        filters = build.includeCollections("db1.collectionA").createFilters();
        assertCollectionIncluded("db1.collectionA");
        assertCollectionExcluded("db1.collectionB");
        assertCollectionExcluded("db2.collectionA");
    }

    @Test
    public void shouldIncludeCollectionCoveredByLiteralWithEscapedPeriodInWhitelistAndNoBlacklist() {
        filters = build.includeCollections("db1[.]collectionA").createFilters();
        assertCollectionIncluded("db1.collectionA");
        assertCollectionExcluded("db1.collectionB");
        assertCollectionExcluded("db2.collectionA");

        filters = build.includeCollections("db1\\.collectionA").createFilters();
        assertCollectionIncluded("db1.collectionA");
        assertCollectionExcluded("db1.collectionB");
        assertCollectionExcluded("db2.collectionA");
    }

    @Test
    public void shouldIncludeCollectionCoveredByMultipleLiteralsInWhitelistAndNoBlacklist() {
        filters = build.includeCollections("db1.collectionA,db1.collectionB").createFilters();
        assertCollectionIncluded("db1.collectionA");
        assertCollectionIncluded("db1.collectionB");
        assertCollectionExcluded("db2.collectionA");
        assertCollectionExcluded("db2.collectionB");
    }

    @Test
    public void shouldIncludeCollectionCoveredByMultipleRegexInWhitelistAndNoBlacklist() {
        filters = build.includeCollections("db1.collection[x]?A,db1[.](.*)B").createFilters();
        assertCollectionIncluded("db1.collectionA");
        assertCollectionIncluded("db1.collectionxA");
        assertCollectionExcluded("db1.collectionx");
        assertCollectionExcluded("db1.collectioxA");
        assertCollectionIncluded("db1.B");
        assertCollectionIncluded("db1.collB");
        assertCollectionIncluded("db1.collectionB");
        assertCollectionExcluded("db2.collectionA");
        assertCollectionExcluded("db2.collectionxA");
        assertCollectionExcluded("db2.B");
        assertCollectionExcluded("db2.collB");
        assertCollectionExcluded("db2.collectionB");
    }

    @Test
    public void shouldIncludeCollectionCoveredByRegexWithWildcardInWhitelistAndNoBlacklist() {
        filters = build.includeCollections("db1[.](.*)").createFilters();
        assertCollectionIncluded("db1.collectionA");
        assertCollectionIncluded("db1.collectionxA");
        assertCollectionIncluded("db1.collectionx");
        assertCollectionIncluded("db1.collectioxA");
        assertCollectionIncluded("db1.B");
        assertCollectionIncluded("db1.collB");
        assertCollectionIncluded("db1.collectionB");
        assertCollectionExcluded("db2.collectionA");
        assertCollectionExcluded("db2.collectionxA");
        assertCollectionExcluded("db12.B");
        assertCollectionExcluded("db12.collB");
        assertCollectionExcluded("db12.collectionB");
    }

    @Test
    public void shouldExcludeCollectionCoveredByLiteralInBlacklist() {
        filters = build.excludeCollections("db1.collectionA").createFilters();
        assertCollectionExcluded("db1.collectionA");
        assertCollectionIncluded("db1.collectionB");
        assertCollectionIncluded("db2.collectionA");
    }

    protected void assertCollectionIncluded(String fullyQualifiedTableName) {
        CollectionId id = CollectionId.parse("rs1." + fullyQualifiedTableName);
        assertThat(id).isNotNull();
        assertThat(filters.collectionFilter().test(id)).isTrue();
    }

    protected void assertCollectionExcluded(String fullyQualifiedTableName) {
        CollectionId id = CollectionId.parse("rs1." + fullyQualifiedTableName);
        assertThat(id).isNotNull();
        assertThat(filters.collectionFilter().test(id)).isFalse();
    }

}
