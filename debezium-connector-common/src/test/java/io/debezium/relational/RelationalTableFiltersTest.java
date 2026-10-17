/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.doc.FixFor;

/**
 * @author ggaborg
 */
public class RelationalTableFiltersTest {

    private Configurator build;
    private RelationalTableFilters filters;

    @BeforeEach
    public void beforeEach() {
        build = new Configurator();
        filters = null;
    }

    @Test
    public void shouldIncludeDatabaseCoveredByLiteralInIncludeList() {
        filters = build.includeDatabases("db1").createFilters();
        assertThat(filters.databaseFilter().test("db1")).isTrue();
    }

    @Test
    public void shouldIncludeDatabaseCoveredByMultipleLiteralsInIncludeList() {
        filters = build.includeDatabases("db1,db2").createFilters();
        assertThat(filters.databaseFilter().test("db1")).isTrue();
        assertThat(filters.databaseFilter().test("db2")).isTrue();
    }

    @Test
    public void shouldIncludeAllCapturedDatabaseDdlInSchemaSnapshot() {
        filters = build.includeDatabases("db1").createFilters();
        assertCollectionIncludedInSchemaSnapshot("db1.collectionA");
        assertCollectionIncludedInSchemaSnapshot("db2.collectionA");
    }

    @Test
    public void shouldIncludeOnlyCapturedDatabaseDdlInSchemaSnapshot() {
        filters = build.includeDatabases("db1").storeOnlyCapturedDatabasesDdl("true").createFilters();
        assertCollectionIncludedInSchemaSnapshot("db1.collectionA");
        assertCollectionExcludedFromSchemaSnapshot("db2.collectionA");
    }

    @Test
    public void shouldIncludeDatabaseCoveredByWildcardInIncludeList() {
        filters = build.includeDatabases("db.*").createFilters();
        assertThat(filters.databaseFilter().test("db1")).isTrue();
    }

    @Test
    public void shouldIncludeDatabaseCoveredByMultipleWildcardsInIncludeList() {
        filters = build.includeDatabases("db.*,mongo.*").createFilters();
        assertThat(filters.databaseFilter().test("db1")).isTrue();
        assertThat(filters.databaseFilter().test("mongo2")).isTrue();
    }

    @Test
    public void shouldExcludeDatabaseCoveredByLiteralInExcludeList() {
        filters = build.excludeDatabases("db1").createFilters();
        assertThat(filters.databaseFilter().test("db1")).isFalse();
    }

    @Test
    public void shouldExcludeDatabaseCoveredByMultipleLiteralsInExcludeList() {
        filters = build.excludeDatabases("db1,db2").createFilters();
        assertThat(filters.databaseFilter().test("db1")).isFalse();
        assertThat(filters.databaseFilter().test("db2")).isFalse();
    }

    @Test
    public void shouldNotExcludeDatabaseNotCoveredByLiteralInExcludeList() {
        filters = build.excludeDatabases("db1").createFilters();
        assertThat(filters.databaseFilter().test("db2")).isTrue();
    }

    @Test
    public void shouldExcludeDatabaseCoveredByWildcardInExcludeList() {
        filters = build.excludeDatabases("db.*").createFilters();
        assertThat(filters.databaseFilter().test("db1")).isFalse();
    }

    @Test
    public void shouldExcludeDatabaseCoveredByMultipleWildcardsInExcludeList() {
        filters = build.excludeDatabases("db.*,mongo.*").createFilters();
        assertThat(filters.databaseFilter().test("db1")).isFalse();
        assertThat(filters.databaseFilter().test("mongo2")).isFalse();
    }

    @Test
    public void shouldIncludeCollectionCoveredByLiteralWithPeriodAsWildcardInIncludeListAndNoExcludeList() {
        filters = build.includeCollections("db1.coll[.]?ection[x]?A,db1[.](.*)B").createFilters();
        assertCollectionIncluded("db1xcoll.ectionA"); // first '.' is an unescaped wildcard in regex
        assertCollectionIncluded("db1.collectionA");
    }

    @Test
    public void shouldIncludeCollectionCoveredByLiteralInIncludeListAndNoExcludeList() {
        filters = build.includeCollections("db1.collectionA").createFilters();
        assertCollectionIncluded("db1.collectionA");
        assertCollectionExcluded("db1.collectionB");
        assertCollectionExcluded("db2.collectionA");
    }

    @Test
    public void shouldIncludeCollectionCoveredByLiteralWithEscapedPeriodInIncludeListAndNoExcludeList() {
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
    public void shouldIncludeCollectionCoveredByMultipleLiteralsInIncludeListAndNoExcludeList() {
        filters = build.includeCollections("db1.collectionA,db1.collectionB").createFilters();
        assertCollectionIncluded("db1.collectionA");
        assertCollectionIncluded("db1.collectionB");
        assertCollectionExcluded("db2.collectionA");
        assertCollectionExcluded("db2.collectionB");
    }

    @Test
    public void shouldIncludeCollectionCoveredByMultipleRegexInIncludeListAndNoExcludeList() {
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
    public void shouldIncludeCollectionCoveredByRegexWithWildcardInIncludeListAndNoExcludeList() {
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
    public void shouldExcludeCollectionCoveredByLiteralInExcludeList() {
        filters = build.excludeCollections("db1.collectionA").createFilters();
        assertCollectionExcluded("db1.collectionA");
        assertCollectionIncluded("db1.collectionB");
        assertCollectionIncluded("db2.collectionA");
    }

    @Test
    public void shouldIncludeSignalingCollectionAndNoIncludeListAndNoExcludeList() {
        filters = build.signalingCollection("db1.signal").createFilters();
        assertCollectionIncluded("db1.signal");
    }

    @Test
    public void shouldIncludeSignalingCollectionNotCoveredByIncludeList() {
        filters = build.includeCollections("db1.table").signalingCollection("db1.signal").createFilters();
        assertCollectionIncluded("db1.signal");
    }

    @Test
    public void shouldIncludeSignalingCollectionCoveredByLiteralInExcludeList() {
        filters = build.excludeCollections("db1.signal").signalingCollection("db1.signal").createFilters();
        assertCollectionIncluded("db1.signal");
    }

    @Test
    public void shouldIncludeSignalingCollectionCoveredByRegexInExcludeList() {
        filters = build.excludeCollections("db1.*").signalingCollection("db1.signal").createFilters();
        assertCollectionIncluded("db1.signal");
    }

    protected void assertCollectionIncluded(String fullyQualifiedCollectionName) {
        TableId id = TableId.parse(fullyQualifiedCollectionName);
        assertThat(id).isNotNull();
        assertThat(filters.dataCollectionFilter().isIncluded(id)).isTrue();
    }

    protected void assertCollectionExcluded(String fullyQualifiedCollectionName) {
        TableId id = TableId.parse(fullyQualifiedCollectionName);
        assertThat(id).isNotNull();
        assertThat(filters.dataCollectionFilter().isIncluded(id)).isFalse();
    }

    protected void assertCollectionExcludedFromSchemaSnapshot(String fullyQualifiedCollectionName) {
        TableId id = TableId.parse(fullyQualifiedCollectionName);
        assertThat(id).isNotNull();
        assertThat(filters.eligibleForSchemaDataCollectionFilter().isIncluded(id)).isFalse();
    }

    protected void assertCollectionIncludedInSchemaSnapshot(String fullyQualifiedCollectionName) {
        TableId id = TableId.parse(fullyQualifiedCollectionName);
        assertThat(id).isNotNull();
        assertThat(filters.eligibleForSchemaDataCollectionFilter().isIncluded(id)).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#2874")
    public void shouldKeepFilteringWhenTheExcludedCacheIsSmallerThanTheTablesAsked() {
        filters = build.includeCollections("db1\\.in.*").excludedCacheSize("2").createFilters();

        for (int round = 0; round < 3; round++) {
            for (int i = 0; i < 10; i++) {
                assertThat(filters.dataCollectionFilter().isIncluded(new TableId("db1", null, "out" + i))).isFalse();
            }
            assertThat(filters.dataCollectionFilter().isIncluded(new TableId("db1", null, "in1"))).isTrue();
        }
    }

    @Test
    @FixFor("debezium/dbz#2874")
    public void shouldRejectANonPositiveExcludedCacheSize() {
        assertThat(RelationalDatabaseConnectorConfig.TABLE_FILTER_EXCLUDED_CACHE_SIZE.defaultValueAsString()).isEqualTo("10000");
        assertThat(Configuration.create()
                .with(RelationalDatabaseConnectorConfig.TABLE_FILTER_EXCLUDED_CACHE_SIZE, 0)
                .build()
                .validate(Field.setOf(RelationalDatabaseConnectorConfig.TABLE_FILTER_EXCLUDED_CACHE_SIZE))
                .get(RelationalDatabaseConnectorConfig.TABLE_FILTER_EXCLUDED_CACHE_SIZE.name()).errorMessages()).isNotEmpty();
    }
}
