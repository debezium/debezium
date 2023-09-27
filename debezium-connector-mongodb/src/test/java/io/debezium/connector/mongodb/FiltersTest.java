/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import org.apache.kafka.connect.errors.ConnectException;
import org.bson.Document;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.doc.FixFor;

/**
 * @author Randall Hauch
 */
public class FiltersTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(FiltersTest.class);

    private Configurator build;
    private Filters filters;
    private Field.Set configFields;

    @Before
    public void beforeEach() {
        build = new Configurator();
        filters = null;
        configFields = Field.setOf(MongoDbConnectorConfig.FIELD_EXCLUDE_LIST, MongoDbConnectorConfig.FIELD_RENAMES);
    }

    @Test
    public void shouldIncludeDatabaseCoveredByLiteralInIncludeList() {
        filters = build.includeDatabases("db1").createFilters();
        assertThat(filters.databaseFilter().test("db1")).isTrue();

        filters = build.useLiteralFilters().createFilters();
        assertThat(filters.databaseFilter().test("db1")).isTrue();
    }

    @Test
    public void shouldIncludeDatabaseCoveredByMultipleLiteralsInIncludeList() {
        filters = build.includeDatabases("db1,db2").createFilters();
        assertThat(filters.databaseFilter().test("db1")).isTrue();
        assertThat(filters.databaseFilter().test("db2")).isTrue();

        filters = build.useLiteralFilters().createFilters();
        assertThat(filters.databaseFilter().test("db1")).isTrue();
        assertThat(filters.databaseFilter().test("db2")).isTrue();
    }

    @Test
    public void shouldIncludeDatabaseCoveredByMultipleLiteralsWithSpacesInIncludeListForLiteralFilters() {
        filters = build
                .useLiteralFilters()
                .includeDatabases(" db1 ,  db2 ")
                .createFilters();
        assertThat(filters.databaseFilter().test("db1")).isTrue();
        assertThat(filters.databaseFilter().test("db2")).isTrue();
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

        filters = build.useLiteralFilters().createFilters();
        assertThat(filters.databaseFilter().test("db1")).isFalse();
    }

    @Test
    public void shouldExcludeDatabaseCoveredByMultipleLiteralsInExcludeList() {
        filters = build.excludeDatabases("db1,db2").createFilters();
        assertThat(filters.databaseFilter().test("db1")).isFalse();
        assertThat(filters.databaseFilter().test("db2")).isFalse();

        filters = build.useLiteralFilters().createFilters();
        assertThat(filters.databaseFilter().test("db1")).isFalse();
        assertThat(filters.databaseFilter().test("db2")).isFalse();
    }

    @Test
    public void shouldExcludeDatabaseCoveredByMultipleLiteralsWithSpacesInExcludeListForLiteralFilters() {
        filters = build
                .useLiteralFilters()
                .excludeDatabases(" db1 ,  db2 ")
                .createFilters();
        assertThat(filters.databaseFilter().test("db1")).isFalse();
        assertThat(filters.databaseFilter().test("db2")).isFalse();
    }

    @Test
    public void shouldNotExcludeDatabaseNotCoveredByLiteralInExcludeList() {
        filters = build.excludeDatabases("db1").createFilters();
        assertThat(filters.databaseFilter().test("db2")).isTrue();

        filters = build.useLiteralFilters().createFilters();
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

        filters = build.useLiteralFilters().createFilters();
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

        filters = build.useLiteralFilters().createFilters();
        assertCollectionIncluded("db1.collectionA");
        assertCollectionIncluded("db1.collectionB");
        assertCollectionExcluded("db2.collectionA");
        assertCollectionExcluded("db2.collectionB");
    }

    @Test
    public void shouldIncludeCollectionCoveredByMultipleLiteralsWithSpacesInIncludeListAndNoExcludeListForLiteralFilters() {
        filters = build
                .useLiteralFilters()
                .includeCollections(" db1.collectionA ,  db1.collectionB ")
                .createFilters();
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

        filters = build.useLiteralFilters().createFilters();
        assertCollectionExcluded("db1.collectionA");
        assertCollectionIncluded("db1.collectionB");
        assertCollectionIncluded("db2.collectionA");
    }

    @Test
    public void shouldExcludeCollectionCoveredByMultipleLiteralsnExcludeList() {
        filters = build.excludeCollections("db1.collectionA,db1.collectionB").createFilters();
        assertCollectionExcluded("db1.collectionA");
        assertCollectionExcluded("db1.collectionB");
        assertCollectionIncluded("db1.collectionC");
        assertCollectionIncluded("db2.collectionA");

        filters = build.useLiteralFilters().createFilters();
        assertCollectionExcluded("db1.collectionA");
        assertCollectionExcluded("db1.collectionB");
        assertCollectionIncluded("db1.collectionC");
        assertCollectionIncluded("db2.collectionA");
    }

    @Test
    public void shouldExcludeCollectionCoveredByMultipleLiteralsWithSpacesInExcludeListForLiteralFilters() {
        filters = build
                .useLiteralFilters()
                .excludeCollections(" db1.collectionA ,  db1.collectionB ")
                .createFilters();
        assertCollectionExcluded("db1.collectionA");
        assertCollectionExcluded("db1.collectionB");
        assertCollectionIncluded("db1.collectionC");
        assertCollectionIncluded("db2.collectionA");
    }

    @Test
    public void shouldIncludeAllCollectionsFromDatabaseWithSignalingCollection() {
        filters = build.includeDatabases("db1")
                .signalingCollection("db1.singal")
                .createFilters();
        assertCollectionIncluded("db1.other");

        filters = build.useLiteralFilters().createFilters();
        assertCollectionIncluded("db1.other");
    }

    @Test
    public void shouldIncludeSignalingCollectionAndNoIncludeListAndNoExcludeList() {
        filters = build.signalingCollection("db1.signal").createFilters();
        assertCollectionIncluded("db1.signal");

        filters = build.useLiteralFilters().createFilters();
        assertCollectionIncluded("db1.signal");
    }

    @Test
    public void shouldIncludeSignalingCollectionNotCoveredByIncludeList() {
        filters = build.includeCollections("db1.table")
                .signalingCollection("db1.signal")
                .createFilters();
        assertCollectionIncluded("db1.signal");

        filters = build.useLiteralFilters().createFilters();
        assertCollectionIncluded("db1.signal");
    }

    @Test
    public void shouldIncludeSignalingCollectionCoveredByLiteralInExcludeList() {
        filters = build.excludeCollections("db1.signal")
                .signalingCollection("db1.signal")
                .createFilters();
        assertCollectionIncluded("db1.signal");

        filters = build.useLiteralFilters().createFilters();
        assertCollectionIncluded("db1.signal");
    }

    @Test
    public void shouldIncludeSignalingCollectionCoveredByRegexInExcludeList() {
        filters = build.excludeCollections("db1.*")
                .signalingCollection("db1.signal")
                .createFilters();
        assertCollectionIncluded("db1.signal");
    }

    @Test
    public void excludeFilterShouldRemoveMatchingField() {
        filters = build.excludeFields("db1.collectionA.key1").createFilters();
        validateConfigFields();
        CollectionId id = CollectionId.parse("rs1.", "db1.collectionA");
        assertEquals(
                Document.parse(" { \"key2\" : \"value2\" }"),
                filters.fieldFilterFor(id).apply(Document.parse(" { \"key1\" : \"value1\", \"key2\" : \"value2\" }")));
    }

    @Test
    public void excludeFilterShouldRemoveMatchingFieldWithLeadingWhiteSpaces() {
        filters = build.excludeFields(" *.collectionA.key1").createFilters();
        validateConfigFields();
        CollectionId id = CollectionId.parse("rs1.", " *.collectionA");
        assertEquals(
                Document.parse(" { \"key2\" : \"value2\" }"),
                filters.fieldFilterFor(id).apply(Document.parse(" { \"key1\" : \"value1\", \"key2\" : \"value2\" }")));
    }

    @Test
    @FixFor("DBZ-5818")
    public void excludeFilterShouldRemoveMatchingFieldWithLeadingMultipleAsterisks() {
        filters = build.excludeFields(" *.*.key1").createFilters();
        validateConfigFields();
        CollectionId id = CollectionId.parse("rs1.", " *.collectionA");
        assertEquals(
                Document.parse(" { \"key2\" : \"value2\" }"),
                filters.fieldFilterFor(id).apply(Document.parse(" { \"key1\" : \"value1\", \"key2\" : \"value2\" }")));
    }

    @Test
    public void excludeFilterShouldRemoveMatchingFieldWithTrailingWhiteSpaces() {
        filters = build.excludeFields("db.collectionA.key1 ,db.collectionA.key2 ").createFilters();
        validateConfigFields();
        CollectionId id = CollectionId.parse("rs1.", "db.collectionA");
        assertEquals(
                Document.parse(" { \"key3\" : \"value3\" }"),
                filters.fieldFilterFor(id).apply(Document.parse(" { \"key1\" : \"value1\", \"key2\" : \"value2\", \"key3\" : \"value3\" }")));
    }

    @Test
    public void renameFilterShouldRenameMatchingField() {
        filters = build.renameFields("db1.collectionA.key1:key2").createFilters();
        validateConfigFields();
        CollectionId id = CollectionId.parse("rs1.", "db1.collectionA");
        assertEquals(
                Document.parse(" { \"key2\" : \"value1\" }"),
                filters.fieldFilterFor(id).apply(Document.parse(" { \"key1\" : \"value1\" }")));
    }

    @Test
    public void renameFilterShouldRenameMatchingFieldWithLeadingWhiteSpaces() {
        filters = build.renameFields(" *.collectionA.key2:key3").createFilters();
        validateConfigFields();
        CollectionId id = CollectionId.parse("rs1.", " *.collectionA");
        assertEquals(
                Document.parse(" { \"key1\" : \"valueA\", \"key3\" : \"valueB\" }"),
                filters.fieldFilterFor(id).apply(Document.parse(" { \"key1\" : \"valueA\", \"key2\" : \"valueB\" }")));
    }

    @Test
    @FixFor("DBZ-5818")
    public void renameFilterShouldRenameMatchingFieldWithLeadingMultipleAsterisks() {
        filters = build.renameFields(" *.*.key2:key3").createFilters();
        validateConfigFields();
        CollectionId id = CollectionId.parse("rs1.", " *.collectionA");
        assertEquals(
                Document.parse(" { \"key1\" : \"valueA\", \"key3\" : \"valueB\" }"),
                filters.fieldFilterFor(id).apply(Document.parse(" { \"key1\" : \"valueA\", \"key2\" : \"valueB\" }")));
    }

    @Test
    public void renameFilterShouldRenameMatchingFieldWithTrailingWhiteSpaces() {
        filters = build.renameFields("db2.collectionA.key1:key2 ,db2.collectionA.key3:key4 ").createFilters();
        validateConfigFields();
        CollectionId id = CollectionId.parse("rs1.", "db2.collectionA");
        assertEquals(
                Document.parse(" { \"key2\" : \"valueA\", \"key4\" : \"valueB\" }"),
                filters.fieldFilterFor(id).apply(Document.parse(" { \"key1\" : \"valueA\", \"key3\" : \"valueB\" }")));
    }

    protected void assertCollectionIncluded(String fullyQualifiedCollectionName) {
        CollectionId id = CollectionId.parse("rs1.", fullyQualifiedCollectionName);
        assertThat(id).isNotNull();
        assertThat(filters.collectionFilter().test(id)).isTrue();
    }

    protected void assertCollectionExcluded(String fullyQualifiedCollectionName) {
        CollectionId id = CollectionId.parse("rs1.", fullyQualifiedCollectionName);
        assertThat(id).isNotNull();
        assertThat(filters.collectionFilter().test(id)).isFalse();
    }

    private void validateConfigFields() {
        Configuration config = build.config();
        if (!config.validateAndRecord(configFields, LOGGER::error)) {
            throw new ConnectException("Unable to validate config.");
        }
    }
}
