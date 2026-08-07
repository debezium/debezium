/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.neo4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.transforms.neo4j.Neo4jCudConverterConfig.NodeMode;
import io.debezium.transforms.neo4j.Neo4jCudConverterConfig.OutputMode;

class Neo4jCudConfigParserTest {

    @Nested
    @DisplayName("Node and output field parsing")
    class FieldParsing {

        @Test
        @DisplayName("Parses minimal valid config with node labels and id properties")
        void parsesMinimalConfig() {
            final var props = Map.of(
                    "node.labels", "Customer",
                    "node.id.properties", "id");

            final var config = Neo4jCudConfigParser.parse(Configuration.from(props), props);

            assertThat(config.nodeMode()).isEqualTo(NodeMode.NODE);
            assertThat(config.nodeLabels()).isEqualTo(List.of("Customer"));
            assertThat(config.nodeIdProperties()).isEqualTo(List.of("id"));
            assertThat(config.outputMode()).isEqualTo(OutputMode.ARRAY);
            assertThat(config.deleteDetach()).isTrue();
            assertThat(config.tombstonesEnabled()).isTrue();
        }

        @Test
        @DisplayName("Parses multiple comma-separated node labels")
        void parsesMultipleLabels() {
            final var props = Map.of(
                    "node.labels", "Person, Employee",
                    "node.id.properties", "id");

            final var config = Neo4jCudConfigParser.parse(Configuration.from(props), props);

            assertThat(config.nodeLabels()).isEqualTo(List.of("Person", "Employee"));
        }

        @Test
        @DisplayName("Parses composite id properties")
        void parsesCompositeIds() {
            final var props = Map.of(
                    "node.labels", "OrderItem",
                    "node.id.properties", "order_id, product_id");

            final var config = Neo4jCudConfigParser.parse(Configuration.from(props), props);

            assertThat(config.nodeIdProperties()).isEqualTo(List.of("order_id", "product_id"));
        }

        @Test
        @DisplayName("Parses output.mode=single")
        void parsesSingleOutputMode() {
            final var props = Map.of(
                    "node.labels", "Customer",
                    "node.id.properties", "id",
                    "output.mode", "single");

            final var config = Neo4jCudConfigParser.parse(Configuration.from(props), props);

            assertThat(config.outputMode()).isEqualTo(OutputMode.SINGLE);
        }

        @Test
        @DisplayName("Parses node.mode=relationship")
        void parsesRelationshipNodeMode() {
            final var props = new HashMap<String, String>();
            props.put("node.mode", "relationship");
            props.put("relationship.order_id.type", "CONTAINS");
            props.put("relationship.order_id.target.label", "Order");

            final var config = Neo4jCudConfigParser.parse(Configuration.from(props), props);

            assertThat(config.nodeMode()).isEqualTo(NodeMode.RELATIONSHIP);
        }

        @Test
        @DisplayName("Parses node.delete.detach=false")
        void parsesDetachFalse() {
            final var props = Map.of(
                    "node.labels", "Customer",
                    "node.id.properties", "id",
                    "node.delete.detach", "false");

            final var config = Neo4jCudConfigParser.parse(Configuration.from(props), props);

            assertThat(config.deleteDetach()).isFalse();
        }

        @Test
        @DisplayName("Parses tombstones.enabled=false")
        void parsesTombstonesFalse() {
            final var props = Map.of(
                    "node.labels", "Customer",
                    "node.id.properties", "id",
                    "tombstones.enabled", "false");

            final var config = Neo4jCudConfigParser.parse(Configuration.from(props), props);

            assertThat(config.tombstonesEnabled()).isFalse();
        }

        @Test
        @DisplayName("Parses include properties filter")
        void parsesIncludeFilter() {
            final var props = Map.of(
                    "node.labels", "Customer",
                    "node.id.properties", "id",
                    "node.properties.include", "first_name, email");

            final var config = Neo4jCudConfigParser.parse(Configuration.from(props), props);

            assertThat(config.propertiesInclude()).containsExactlyInAnyOrder("first_name", "email");
            assertThat(config.propertiesExclude()).isEmpty();
        }

        @Test
        @DisplayName("Parses exclude properties filter")
        void parsesExcludeFilter() {
            final var props = Map.of(
                    "node.labels", "Customer",
                    "node.id.properties", "id",
                    "node.properties.exclude", "internal_flag");

            final var config = Neo4jCudConfigParser.parse(Configuration.from(props), props);

            assertThat(config.propertiesExclude()).containsExactly("internal_flag");
            assertThat(config.propertiesInclude()).isEmpty();
        }

        @Test
        @DisplayName("Returns empty list for missing optional comma-separated fields")
        void emptyForMissingOptionalFields() {
            final var props = Map.of(
                    "node.labels", "Customer",
                    "node.id.properties", "id");

            final var config = Neo4jCudConfigParser.parse(Configuration.from(props), props);

            assertThat(config.propertiesInclude()).isEmpty();
            assertThat(config.propertiesExclude()).isEmpty();
            assertThat(config.relationships()).isEmpty();
        }
    }

    @Nested
    @DisplayName("Relationship parsing")
    class RelationshipParsing {

        @Test
        @DisplayName("Parses a single relationship mapping with all fields")
        void parsesSingleRelationship() {
            final var props = new HashMap<String, String>();
            props.put("node.labels", "Order");
            props.put("node.id.properties", "id");
            props.put("relationship.customer_id.type", "PLACED_BY");
            props.put("relationship.customer_id.direction", "outgoing");
            props.put("relationship.customer_id.target.label", "Customer");
            props.put("relationship.customer_id.target.id", "cust_id");
            props.put("relationship.customer_id.target.node.op", "merge");
            props.put("relationship.customer_id.properties", "weight");

            final var config = Neo4jCudConfigParser.parse(Configuration.from(props), props);
            final var rel = config.relationships().get(0);

            assertThat(rel.fkColumn()).isEqualTo("customer_id");
            assertThat(rel.type()).isEqualTo("PLACED_BY");
            assertThat(rel.direction()).isEqualTo(RelationshipMapping.Direction.OUTGOING);
            assertThat(rel.targetLabel()).isEqualTo("Customer");
            assertThat(rel.targetId()).isEqualTo("cust_id");
            assertThat(rel.targetNodeOp()).isEqualTo(CudEvent.Operation.MERGE);
            assertThat(rel.properties()).isEqualTo(List.of("weight"));
        }

        @Test
        @DisplayName("Applies defaults for optional relationship fields")
        void appliesRelationshipDefaults() {
            final var props = new HashMap<String, String>();
            props.put("node.labels", "Order");
            props.put("node.id.properties", "id");
            props.put("relationship.customer_id.type", "PLACED_BY");
            props.put("relationship.customer_id.target.label", "Customer");

            final var config = Neo4jCudConfigParser.parse(Configuration.from(props), props);
            final var rel = config.relationships().get(0);

            assertThat(rel.direction()).isEqualTo(RelationshipMapping.Direction.OUTGOING);
            assertThat(rel.targetId()).isEqualTo("customer_id");
            assertThat(rel.targetNodeOp()).isEqualTo(CudEvent.Operation.MATCH);
            assertThat(rel.properties()).isEmpty();
        }

        @Test
        @DisplayName("Parses incoming direction")
        void parsesIncomingDirection() {
            final var props = new HashMap<String, String>();
            props.put("node.labels", "Order");
            props.put("node.id.properties", "id");
            props.put("relationship.customer_id.type", "HAS_ORDER");
            props.put("relationship.customer_id.direction", "incoming");
            props.put("relationship.customer_id.target.label", "Customer");

            final var rel = Neo4jCudConfigParser.parse(Configuration.from(props), props).relationships().get(0);

            assertThat(rel.direction()).isEqualTo(RelationshipMapping.Direction.INCOMING);
        }

        @Test
        @DisplayName("Parses multiple relationship mappings preserving order")
        void parsesMultipleRelationships() {
            final var props = new HashMap<String, String>();
            props.put("node.mode", "relationship");
            props.put("relationship.order_id.type", "CONTAINS");
            props.put("relationship.order_id.target.label", "Order");
            props.put("relationship.product_id.type", "CONTAINS");
            props.put("relationship.product_id.target.label", "Product");

            final var config = Neo4jCudConfigParser.parse(Configuration.from(props), props);

            assertThat(config.relationships()).hasSize(2);
            assertThat(config.relationships().get(0).fkColumn()).isEqualTo("order_id");
            assertThat(config.relationships().get(1).fkColumn()).isEqualTo("product_id");
        }

        @Test
        @DisplayName("Parses relationship properties as comma-separated list")
        void parsesRelationshipProperties() {
            final var props = new HashMap<String, String>();
            props.put("node.mode", "relationship");
            props.put("relationship.order_id.type", "CONTAINS");
            props.put("relationship.order_id.target.label", "Order");
            props.put("relationship.order_id.properties", "quantity, price");
            props.put("relationship.product_id.type", "CONTAINS");
            props.put("relationship.product_id.target.label", "Product");

            final var rel = Neo4jCudConfigParser.parse(Configuration.from(props), props).relationships().get(0);

            assertThat(rel.properties()).isEqualTo(List.of("quantity", "price"));
        }

        @Test
        @DisplayName("Exposes FK column names as a set")
        void exposesFkColumns() {
            final var props = new HashMap<String, String>();
            props.put("node.mode", "relationship");
            props.put("relationship.order_id.type", "CONTAINS");
            props.put("relationship.order_id.target.label", "Order");
            props.put("relationship.product_id.type", "CONTAINS");
            props.put("relationship.product_id.target.label", "Product");

            final var config = Neo4jCudConfigParser.parse(Configuration.from(props), props);

            assertThat(config.fkColumns()).containsExactlyInAnyOrder("order_id", "product_id");
        }
    }

    @Nested
    @DisplayName("Validation errors")
    class ValidationErrors {

        @Test
        @DisplayName("Rejects node.mode=node without node.id.properties")
        void rejectsNodeModeWithoutIdProperties() {
            final var props = Map.of("node.labels", "Customer");

            assertThatThrownBy(() -> Neo4jCudConfigParser.parse(Configuration.from(props), props))
                    .isInstanceOf(ConfigException.class)
                    .hasMessageContaining("node.id.properties");
        }

        @Test
        @DisplayName("Rejects node.mode=relationship without any relationship config")
        void rejectsRelationshipModeWithoutRelConfig() {
            final var props = Map.of("node.mode", "relationship");

            assertThatThrownBy(() -> Neo4jCudConfigParser.parse(Configuration.from(props), props))
                    .isInstanceOf(ConfigException.class)
                    .hasMessageContaining("relationship");
        }

        @Test
        @DisplayName("Rejects both include and exclude properties set")
        void rejectsMutuallyExclusiveFilters() {
            final var props = Map.of(
                    "node.labels", "Customer",
                    "node.id.properties", "id",
                    "node.properties.include", "name",
                    "node.properties.exclude", "email");

            assertThatThrownBy(() -> Neo4jCudConfigParser.parse(Configuration.from(props), props))
                    .isInstanceOf(ConfigException.class);
        }

        @Test
        @DisplayName("Rejects relationship mapping without type")
        void rejectsRelationshipWithoutType() {
            final var props = new HashMap<String, String>();
            props.put("node.labels", "Order");
            props.put("node.id.properties", "id");
            props.put("relationship.customer_id.target.label", "Customer");

            assertThatThrownBy(() -> Neo4jCudConfigParser.parse(Configuration.from(props), props))
                    .isInstanceOf(ConfigException.class)
                    .hasMessageContaining("relationship.customer_id.type");
        }

        @Test
        @DisplayName("Rejects relationship mapping without target.label")
        void rejectsRelationshipWithoutTargetLabel() {
            final var props = new HashMap<String, String>();
            props.put("node.labels", "Order");
            props.put("node.id.properties", "id");
            props.put("relationship.customer_id.type", "PLACED_BY");

            assertThatThrownBy(() -> Neo4jCudConfigParser.parse(Configuration.from(props), props))
                    .isInstanceOf(ConfigException.class)
                    .hasMessageContaining("relationship.customer_id.target.label");
        }

        @Test
        @DisplayName("Rejects invalid target.node.op value at configure time")
        void rejectsInvalidTargetNodeOp() {
            final var props = new HashMap<String, String>();
            props.put("node.labels", "Order");
            props.put("node.id.properties", "id");
            props.put("relationship.customer_id.type", "PLACED_BY");
            props.put("relationship.customer_id.target.label", "Customer");
            props.put("relationship.customer_id.target.node.op", "create");

            assertThatThrownBy(() -> Neo4jCudConfigParser.parse(Configuration.from(props), props))
                    .isInstanceOf(ConfigException.class)
                    .hasMessageContaining("target.node.op")
                    .hasMessageContaining("match");
        }
    }

}
