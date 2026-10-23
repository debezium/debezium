/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.neo4j;

import java.util.List;

import io.debezium.transforms.neo4j.Neo4jDebeziumConverterConfig.Owner;
import io.debezium.transforms.neo4j.Neo4jDebeziumConverterConfig.RelationshipMode;

/**
 * Immutable optional override mapping for a single Neo4j relationship type, optionally qualified by endpoint
 * labels ({@code relationship.<TYPE>@<Start>-<End>.*}).
 * <p>
 * In {@link RelationshipMode#JOIN_TABLE} mode the relationship becomes a row in a join table whose two
 * foreign-key columns come from the start and end node keys ({@code startColumn}/{@code endColumn} override the
 * convention).
 * In {@link RelationshipMode#FOREIGN_KEY} mode the relationship becomes a foreign-key column update
 * on the {@code owner} endpoint's table ({@code fkColumn} overrides the convention).
 * <p>
 * Unlike {@link LabelMappingConfig}, this record does not self-validate: the mode-specific rules (that
 * {@code fk.column}/{@code owner} appear only in foreign-key mode and {@code start.column}/{@code end.column}
 * only in join-table mode) are enforced by {@code Neo4jDebeziumConfigParser.validateModeKeys}, which has the
 * per-entity key context needed to build a helpful {@code ConfigException}.
 *
 * @param type          the relationship type (e.g. {@code CONTAINS})
 * @param qualifier     the {@code @Start-End} endpoint qualifier, or {@code null} when unqualified
 * @param mode          join-table (default) or foreign-key
 * @param table         target table override, or {@code null} to derive it (from the type, or the owner label in FK mode)
 * @param startColumn   (join_table) start endpoint FK column override, or {@code null} to derive it
 * @param endColumn     (join_table) end endpoint FK column override, or {@code null} to derive it
 * @param properties    relationship properties to include as columns; empty means all
 * @param owner         (foreign_key) which endpoint owns the row to update
 * @param fkColumn      (foreign_key) foreign-key column override, or {@code null} to derive it
 */
public record RelationshipMappingConfig(String type, String qualifier, RelationshipMode mode, String table,
        String startColumn, String endColumn, List<String> properties, Owner owner, String fkColumn) {

    public boolean isForeignKey() {
        return mode == RelationshipMode.FOREIGN_KEY;
    }
}
