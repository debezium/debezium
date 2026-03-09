/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;
import java.util.function.Predicate;

import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;
import io.debezium.relational.RelationalDatabaseConnectorConfig;

/**
 * Unit tests for {@link PostgresConnectorTask}.
 */
public class PostgresConnectorTaskTest {

    /**
     * Build a minimal {@link PostgresConnectorConfig} that supplies enough fields for
     * {@link PostgresConnectorTask#buildTypeRegistrySchemaFilter(Predicate, Set)} to run without NPE.
     */
    private static Predicate<String> schemaFilterFor(String schemaIncludeList) {
        Configuration.Builder builder = Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "test_server")
                .with(PostgresConnectorConfig.DATABASE_NAME, "testdb");
        if (schemaIncludeList != null) {
            builder.with(RelationalDatabaseConnectorConfig.SCHEMA_INCLUDE_LIST, schemaIncludeList);
        }
        return new PostgresConnectorConfig(builder.build()).getTableFilters().schemaFilter();
    }

    // -------------------------------------------------------------------------
    // Tests for the predicate+candidateSchemas overload (no DB connection needed)
    // -------------------------------------------------------------------------

    @Test
    @FixFor("DBZ-9455")
    void allSchemasReturnedWhenNoIncludeListConfigured() {
        // With no schema.include.list the predicate accepts everything.
        Set<String> candidates = Set.of("public", "tenant_a", "tenant_b");
        Set<String> filter = PostgresConnectorTask.buildTypeRegistrySchemaFilter(
                schemaFilterFor(null), candidates);
        assertThat(filter).containsExactlyInAnyOrder("public", "tenant_a", "tenant_b");
    }

    @Test
    @FixFor("DBZ-9455")
    void allSchemasReturnedWhenSchemaIncludeListIsBlank() {
        Set<String> candidates = Set.of("public", "tenant_a");
        Set<String> filter = PostgresConnectorTask.buildTypeRegistrySchemaFilter(
                schemaFilterFor("   "), candidates);
        assertThat(filter).containsExactlyInAnyOrder("public", "tenant_a");
    }

    @Test
    @FixFor("DBZ-9455")
    void plainLiteralSchemaNameIsIncludedInFilter() {
        Set<String> candidates = Set.of("public", "noise");
        Set<String> filter = PostgresConnectorTask.buildTypeRegistrySchemaFilter(
                schemaFilterFor("public"), candidates);
        assertThat(filter).containsExactly("public");
    }

    @Test
    @FixFor("DBZ-9455")
    void multipleLiteralSchemaNamesAreIncludedInFilter() {
        Set<String> candidates = Set.of("public", "tenant_a", "tenant_b", "noise");
        Set<String> filter = PostgresConnectorTask.buildTypeRegistrySchemaFilter(
                schemaFilterFor("public,tenant_a,tenant_b"), candidates);
        assertThat(filter).containsExactlyInAnyOrder("public", "tenant_a", "tenant_b");
    }

    @Test
    @FixFor("DBZ-9455")
    void regexPatternMatchesSchemasFromCandidateSet() {
        // Regex patterns like "tenant.*" should now work — matched against actual DB schema names.
        Set<String> candidates = Set.of("public", "tenant_a", "tenant_b", "other");
        Set<String> filter = PostgresConnectorTask.buildTypeRegistrySchemaFilter(
                schemaFilterFor("public,tenant.*"), candidates);
        assertThat(filter).containsExactlyInAnyOrder("public", "tenant_a", "tenant_b");
    }

    @Test
    @FixFor("DBZ-9455")
    void noMatchingCandidatesResultsInEmptyFilter() {
        Set<String> candidates = Set.of("other1", "other2");
        Set<String> filter = PostgresConnectorTask.buildTypeRegistrySchemaFilter(
                schemaFilterFor("public"), candidates);
        assertThat(filter).isEmpty();
    }

    @Test
    @FixFor("DBZ-9455")
    void emptyCandidateSetResultsInEmptyFilter() {
        Set<String> filter = PostgresConnectorTask.buildTypeRegistrySchemaFilter(
                schemaFilterFor("public"), Set.of());
        assertThat(filter).isEmpty();
    }

    @Test
    @FixFor("DBZ-9455")
    void schemaExcludeListIsHonoured() {
        // schema.exclude.list should cause the named schema to be excluded.
        Configuration config = Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "test_server")
                .with(PostgresConnectorConfig.DATABASE_NAME, "testdb")
                .with(RelationalDatabaseConnectorConfig.SCHEMA_EXCLUDE_LIST, "noise")
                .build();
        Predicate<String> predicate = new PostgresConnectorConfig(config).getTableFilters().schemaFilter();

        Set<String> candidates = Set.of("public", "tenant_a", "noise");
        Set<String> filter = PostgresConnectorTask.buildTypeRegistrySchemaFilter(predicate, candidates);
        assertThat(filter).containsExactlyInAnyOrder("public", "tenant_a");
    }

    @Test
    @FixFor("DBZ-9455")
    void schemaNameWithHyphenAndDotIsAccepted() {
        Set<String> candidates = Set.of("my-schema", "my.schema", "other");
        Set<String> filter = PostgresConnectorTask.buildTypeRegistrySchemaFilter(
                schemaFilterFor("my-schema,my\\.schema"), candidates);
        assertThat(filter).containsExactlyInAnyOrder("my-schema", "my.schema");
    }
}
