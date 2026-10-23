/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.neo4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.transforms.neo4j.Neo4jDebeziumConverterConfig.Owner;
import io.debezium.transforms.neo4j.Neo4jDebeziumConverterConfig.RelationshipMode;

class Neo4jDebeziumConfigParserTest {

    private static Neo4jDebeziumConverterConfig parse(Map<String, String> props) {
        return Neo4jDebeziumConverterConfig.from(Configuration.from(props), props);
    }

    @Test
    @DisplayName("empty configuration is valid and yields no per-entity mappings")
    void zeroConfig() {
        final var config = parse(Map.of());
        assertThat(config.labelMappings()).isEmpty();
        assertThat(config.relationshipMappings()).isEmpty();
        assertThat(config.tableNaming()).isEqualTo(Neo4jDebeziumConverterConfig.NamingStrategy.AS_IS);
    }

    @Test
    @DisplayName("label sub-keys are grouped by label")
    void labelGrouping() {
        final var config = parse(Map.of(
                "label.Customer.table", "customers",
                "label.Customer.key.properties", "id",
                "label.Customer.properties.exclude", "secret,internal"));

        final var mapping = config.labelMappings().get("Customer");
        assertThat(mapping).isNotNull();
        assertThat(mapping.table()).isEqualTo("customers");
        assertThat(mapping.keyProperties()).containsExactly("id");
        assertThat(mapping.propertiesExclude()).containsExactlyInAnyOrder("secret", "internal");
        assertThat(mapping.propertiesInclude()).isEmpty();
    }

    @Test
    @DisplayName("include and exclude on the same label are rejected")
    void includeExcludeMutuallyExclusive() {
        assertThatThrownBy(() -> parse(Map.of(
                "label.Customer.properties.include", "a",
                "label.Customer.properties.exclude", "b")))
                .isInstanceOf(ConfigException.class);
    }

    @Test
    @DisplayName("unknown label sub-key fails fast with the fully-qualified key")
    void unknownLabelKey() {
        assertThatThrownBy(() -> parse(Map.of("label.Customer.tabel", "customers")))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("label.Customer.tabel");
    }

    @Test
    @DisplayName("relationship type without qualifier parses in join-table mode")
    void relationshipUnqualified() {
        final var config = parse(Map.of(
                "relationship.CONTAINS.table", "order_items",
                "relationship.CONTAINS.properties", "quantity,price"));

        final var mapping = config.relationshipMappings().get("CONTAINS");
        assertThat(mapping.type()).isEqualTo("CONTAINS");
        assertThat(mapping.qualifier()).isNull();
        assertThat(mapping.mode()).isEqualTo(RelationshipMode.JOIN_TABLE);
        assertThat(mapping.table()).isEqualTo("order_items");
        assertThat(mapping.properties()).containsExactly("quantity", "price");
    }

    @Test
    @DisplayName("@Start-End qualifier is split into type and qualifier and wins over the unqualified mapping")
    void relationshipQualified() {
        final var config = parse(Map.of(
                "relationship.KNOWS.table", "generic_knows",
                "relationship.KNOWS@Person-Person.table", "friendships"));

        final var qualified = config.relationshipMappings().get("KNOWS@Person-Person");
        assertThat(qualified.type()).isEqualTo("KNOWS");
        assertThat(qualified.qualifier()).isEqualTo("Person-Person");
        assertThat(qualified.table()).isEqualTo("friendships");

        // The resolver prefers the qualified mapping for a Person-Person relationship.
        final var resolved = config.relationshipMappingFor("KNOWS", List.of("Person"), List.of("Person"));
        assertThat(resolved.table()).isEqualTo("friendships");
        // ... and falls back to the unqualified mapping for other endpoints.
        final var fallback = config.relationshipMappingFor("KNOWS", List.of("Person"), List.of("Company"));
        assertThat(fallback.table()).isEqualTo("generic_knows");
    }

    @Test
    @DisplayName("foreign-key relationship parses mode, owner and fk.column")
    void relationshipForeignKey() {
        final var config = parse(Map.of(
                "relationship.PLACED_BY.mode", "foreign_key",
                "relationship.PLACED_BY.owner", "end",
                "relationship.PLACED_BY.fk.column", "customer_ref"));

        final var mapping = config.relationshipMappings().get("PLACED_BY");
        assertThat(mapping.isForeignKey()).isTrue();
        assertThat(mapping.owner()).isEqualTo(Owner.END);
        assertThat(mapping.fkColumn()).isEqualTo("customer_ref");
    }

    @Test
    @DisplayName("fk.column in the default join-table mode is rejected")
    void fkColumnInJoinTableRejected() {
        assertThatThrownBy(() -> parse(Map.of("relationship.CONTAINS.fk.column", "x")))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("foreign_key");
    }

    @Test
    @DisplayName("start.column in foreign-key mode is rejected")
    void startColumnInForeignKeyRejected() {
        assertThatThrownBy(() -> parse(Map.of(
                "relationship.PLACED_BY.mode", "foreign_key",
                "relationship.PLACED_BY.start.column", "x")))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("join_table");
    }

    @Test
    @DisplayName("relationship.fk.naming is treated as a global key, not a relationship type named 'fk'")
    void fkNamingIsGlobal() {
        final var config = parse(Map.of("relationship.fk.naming", "label_key"));
        assertThat(config.relationshipMappings()).doesNotContainKey("fk");
        assertThat(config.fkNaming()).isEqualTo(Neo4jDebeziumConverterConfig.FkNaming.LABEL_KEY);
    }

    @Test
    @DisplayName("unknown relationship sub-key fails fast")
    void unknownRelationshipKey() {
        assertThatThrownBy(() -> parse(Map.of("relationship.CONTAINS.tabel", "x")))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("relationship.CONTAINS.tabel");
    }
}
