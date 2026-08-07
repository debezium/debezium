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

    private static Neo4jCudConverterConfig parse(Map<String, String> props) {
        return Neo4jCudConfigParser.parse(Configuration.from(props), props);
    }

    private static TableMappingConfig mapping(Map<String, String> props, String table) {
        return parse(props).mappingFor(table);
    }

    @Nested
    @DisplayName("Node and output field parsing")
    class FieldParsing {

        @Test
        @DisplayName("Parses minimal valid config with node labels and id properties")
        void parsesMinimalConfig() {
            final var props = Map.of(
                    "table.customers.node.labels", "Customer",
                    "table.customers.node.id.properties", "id");

            final var config = parse(props);
            final var customers = config.mappingFor("customers");

            assertThat(customers.nodeMode()).isEqualTo(NodeMode.NODE);
            assertThat(customers.nodeLabels()).isEqualTo(List.of("Customer"));
            assertThat(customers.nodeIdProperties()).isEqualTo(List.of("id"));
            assertThat(customers.deleteDetach()).isTrue();
            assertThat(config.outputMode()).isEqualTo(OutputMode.ARRAY);
            assertThat(config.tombstonesEnabled()).isTrue();
        }

        @Test
        @DisplayName("Parses multiple comma-separated node labels")
        void parsesMultipleLabels() {
            final var props = Map.of(
                    "table.customers.node.labels", "Person, Employee",
                    "table.customers.node.id.properties", "id");

            assertThat(mapping(props, "customers").nodeLabels()).isEqualTo(List.of("Person", "Employee"));
        }

        @Test
        @DisplayName("Parses composite id properties")
        void parsesCompositeIds() {
            final var props = Map.of(
                    "table.order_items.node.labels", "OrderItem",
                    "table.order_items.node.id.properties", "order_id, product_id");

            assertThat(mapping(props, "order_items").nodeIdProperties()).isEqualTo(List.of("order_id", "product_id"));
        }

        @Test
        @DisplayName("Parses output.mode=single as a global setting")
        void parsesSingleOutputMode() {
            final var props = Map.of(
                    "table.customers.node.labels", "Customer",
                    "table.customers.node.id.properties", "id",
                    "output.mode", "single");

            assertThat(parse(props).outputMode()).isEqualTo(OutputMode.SINGLE);
        }

        @Test
        @DisplayName("Parses node.mode=relationship")
        void parsesRelationshipNodeMode() {
            final var props = new HashMap<String, String>();
            props.put("table.order_items.node.mode", "relationship");
            props.put("table.order_items.relationship.order_id.direction", "outgoing");
            props.put("table.order_items.relationship.order_id.type", "CONTAINS");
            props.put("table.order_items.relationship.order_id.target.label", "Order");
            props.put("table.order_items.relationship.product_id.direction", "incoming");
            props.put("table.order_items.relationship.product_id.type", "CONTAINS");
            props.put("table.order_items.relationship.product_id.target.label", "Product");

            assertThat(mapping(props, "order_items").nodeMode()).isEqualTo(NodeMode.RELATIONSHIP);
        }

        @Test
        @DisplayName("Parses node.delete.detach=false")
        void parsesDetachFalse() {
            final var props = Map.of(
                    "table.customers.node.labels", "Customer",
                    "table.customers.node.id.properties", "id",
                    "table.customers.node.delete.detach", "false");

            assertThat(mapping(props, "customers").deleteDetach()).isFalse();
        }

        @Test
        @DisplayName("Parses tombstones.enabled=false as a global setting")
        void parsesTombstonesFalse() {
            final var props = Map.of(
                    "table.customers.node.labels", "Customer",
                    "table.customers.node.id.properties", "id",
                    "tombstones.enabled", "false");

            assertThat(parse(props).tombstonesEnabled()).isFalse();
        }

        @Test
        @DisplayName("Parses include properties filter")
        void parsesIncludeFilter() {
            final var props = Map.of(
                    "table.customers.node.labels", "Customer",
                    "table.customers.node.id.properties", "id",
                    "table.customers.node.properties.include", "first_name, email");

            final var customers = mapping(props, "customers");
            assertThat(customers.propertiesInclude()).containsExactlyInAnyOrder("first_name", "email");
            assertThat(customers.propertiesExclude()).isEmpty();
        }

        @Test
        @DisplayName("Parses exclude properties filter")
        void parsesExcludeFilter() {
            final var props = Map.of(
                    "table.customers.node.labels", "Customer",
                    "table.customers.node.id.properties", "id",
                    "table.customers.node.properties.exclude", "internal_flag");

            final var customers = mapping(props, "customers");
            assertThat(customers.propertiesExclude()).containsExactly("internal_flag");
            assertThat(customers.propertiesInclude()).isEmpty();
        }

        @Test
        @DisplayName("Returns empty list for missing optional comma-separated fields")
        void emptyForMissingOptionalFields() {
            final var props = Map.of(
                    "table.customers.node.labels", "Customer",
                    "table.customers.node.id.properties", "id");

            final var customers = mapping(props, "customers");
            assertThat(customers.propertiesInclude()).isEmpty();
            assertThat(customers.propertiesExclude()).isEmpty();
            assertThat(customers.relationships()).isEmpty();
        }
    }

    @Nested
    @DisplayName("Multi-table parsing")
    class MultiTableParsing {

        @Test
        @DisplayName("Parses several tables into distinct mappings under one config")
        void parsesMultipleTables() {
            final var props = new HashMap<String, String>();
            props.put("table.customers.node.labels", "Customer");
            props.put("table.customers.node.id.properties", "id");
            props.put("table.orders.node.labels", "Order");
            props.put("table.orders.node.id.properties", "id");
            props.put("table.orders.relationship.customer_id.type", "PLACED_BY");
            props.put("table.orders.relationship.customer_id.target.label", "Customer");

            final var config = parse(props);

            assertThat(config.tableMappings()).containsOnlyKeys("customers", "orders");
            assertThat(config.mappingFor("customers").nodeLabels()).isEqualTo(List.of("Customer"));
            assertThat(config.mappingFor("orders").nodeLabels()).isEqualTo(List.of("Order"));
            assertThat(config.mappingFor("orders").relationships()).hasSize(1);
            assertThat(config.mappingFor("customers").relationships()).isEmpty();
        }

        @Test
        @DisplayName("Returns null for a table with no configured mapping")
        void unmappedTableReturnsNull() {
            final var props = Map.of(
                    "table.customers.node.labels", "Customer",
                    "table.customers.node.id.properties", "id");

            assertThat(parse(props).mappingFor("products")).isNull();
        }

        @Test
        @DisplayName("Relationships of one table are scoped to that table only")
        void relationshipsAreScopedPerTable() {
            final var props = new HashMap<String, String>();
            props.put("table.orders.node.labels", "Order");
            props.put("table.orders.node.id.properties", "id");
            props.put("table.orders.relationship.customer_id.type", "PLACED_BY");
            props.put("table.orders.relationship.customer_id.target.label", "Customer");
            props.put("table.customers.node.labels", "Customer");
            props.put("table.customers.node.id.properties", "id");

            final var config = parse(props);

            assertThat(config.mappingFor("orders").fkColumns()).containsExactly("customer_id");
            assertThat(config.mappingFor("customers").fkColumns()).isEmpty();
        }
    }

    @Nested
    @DisplayName("Relationship parsing")
    class RelationshipParsing {

        @Test
        @DisplayName("Parses a single relationship mapping with all fields")
        void parsesSingleRelationship() {
            final var props = new HashMap<String, String>();
            props.put("table.orders.node.labels", "Order");
            props.put("table.orders.node.id.properties", "id");
            props.put("table.orders.relationship.customer_id.type", "PLACED_BY");
            props.put("table.orders.relationship.customer_id.direction", "outgoing");
            props.put("table.orders.relationship.customer_id.target.label", "Customer");
            props.put("table.orders.relationship.customer_id.target.id", "cust_id");
            props.put("table.orders.relationship.customer_id.target.node.op", "merge");
            props.put("table.orders.relationship.customer_id.properties", "weight");

            final var rel = mapping(props, "orders").relationships().get(0);

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
            props.put("table.orders.node.labels", "Order");
            props.put("table.orders.node.id.properties", "id");
            props.put("table.orders.relationship.customer_id.type", "PLACED_BY");
            props.put("table.orders.relationship.customer_id.target.label", "Customer");

            final var rel = mapping(props, "orders").relationships().get(0);

            assertThat(rel.direction()).isEqualTo(RelationshipMapping.Direction.OUTGOING);
            assertThat(rel.targetId()).isEqualTo("customer_id");
            assertThat(rel.targetNodeOp()).isEqualTo(CudEvent.Operation.MATCH);
            assertThat(rel.properties()).isEmpty();
        }

        @Test
        @DisplayName("Parses incoming direction")
        void parsesIncomingDirection() {
            final var props = new HashMap<String, String>();
            props.put("table.orders.node.labels", "Order");
            props.put("table.orders.node.id.properties", "id");
            props.put("table.orders.relationship.customer_id.type", "HAS_ORDER");
            props.put("table.orders.relationship.customer_id.direction", "incoming");
            props.put("table.orders.relationship.customer_id.target.label", "Customer");

            final var rel = mapping(props, "orders").relationships().get(0);

            assertThat(rel.direction()).isEqualTo(RelationshipMapping.Direction.INCOMING);
        }

        @Test
        @DisplayName("Selects endpoints by direction, independent of config key order")
        void parsesMultipleRelationships() {
            // product_id is declared first, but direction (not order) fixes the endpoints.
            final var props = new HashMap<String, String>();
            props.put("table.order_items.node.mode", "relationship");
            props.put("table.order_items.relationship.product_id.direction", "incoming");
            props.put("table.order_items.relationship.product_id.type", "CONTAINS");
            props.put("table.order_items.relationship.product_id.target.label", "Product");
            props.put("table.order_items.relationship.order_id.direction", "outgoing");
            props.put("table.order_items.relationship.order_id.type", "CONTAINS");
            props.put("table.order_items.relationship.order_id.target.label", "Order");

            final var relationships = mapping(props, "order_items").relationships();

            assertThat(relationships).hasSize(2);
            final var source = relationships.stream()
                    .filter(r -> r.direction() == RelationshipMapping.Direction.OUTGOING).findFirst().orElseThrow();
            final var target = relationships.stream()
                    .filter(r -> r.direction() == RelationshipMapping.Direction.INCOMING).findFirst().orElseThrow();
            assertThat(source.fkColumn()).isEqualTo("order_id");
            assertThat(target.fkColumn()).isEqualTo("product_id");
        }

        @Test
        @DisplayName("Parses relationship properties as comma-separated list")
        void parsesRelationshipProperties() {
            final var props = new HashMap<String, String>();
            props.put("table.order_items.node.mode", "relationship");
            props.put("table.order_items.relationship.order_id.direction", "outgoing");
            props.put("table.order_items.relationship.order_id.type", "CONTAINS");
            props.put("table.order_items.relationship.order_id.target.label", "Order");
            props.put("table.order_items.relationship.order_id.properties", "quantity, price");
            props.put("table.order_items.relationship.product_id.direction", "incoming");
            props.put("table.order_items.relationship.product_id.type", "CONTAINS");
            props.put("table.order_items.relationship.product_id.target.label", "Product");

            final var rel = mapping(props, "order_items").relationships().stream()
                    .filter(r -> r.direction() == RelationshipMapping.Direction.OUTGOING).findFirst().orElseThrow();

            assertThat(rel.properties()).isEqualTo(List.of("quantity", "price"));
        }

        @Test
        @DisplayName("Exposes FK column names as a set")
        void exposesFkColumns() {
            final var props = new HashMap<String, String>();
            props.put("table.order_items.node.mode", "relationship");
            props.put("table.order_items.relationship.order_id.direction", "outgoing");
            props.put("table.order_items.relationship.order_id.type", "CONTAINS");
            props.put("table.order_items.relationship.order_id.target.label", "Order");
            props.put("table.order_items.relationship.product_id.direction", "incoming");
            props.put("table.order_items.relationship.product_id.type", "CONTAINS");
            props.put("table.order_items.relationship.product_id.target.label", "Product");

            assertThat(mapping(props, "order_items").fkColumns()).containsExactlyInAnyOrder("order_id", "product_id");
        }
    }

    @Nested
    @DisplayName("Validation errors")
    class ValidationErrors {

        @Test
        @DisplayName("Rejects a config with no per-table mappings")
        void rejectsEmptyConfig() {
            final var props = Map.of("output.mode", "single");

            assertThatThrownBy(() -> parse(props))
                    .isInstanceOf(ConfigException.class)
                    .hasMessageContaining("table.<table_name>");
        }

        @Test
        @DisplayName("Rejects node.mode=node without node.id.properties, naming the table")
        void rejectsNodeModeWithoutIdProperties() {
            final var props = Map.of("table.customers.node.labels", "Customer");

            assertThatThrownBy(() -> parse(props))
                    .isInstanceOf(ConfigException.class)
                    .hasMessageContaining("table.customers.node.id.properties");
        }

        @Test
        @DisplayName("Rejects node.mode=relationship without any relationship config")
        void rejectsRelationshipModeWithoutRelConfig() {
            final var props = Map.of("table.order_items.node.mode", "relationship");

            assertThatThrownBy(() -> parse(props))
                    .isInstanceOf(ConfigException.class)
                    .hasMessageContaining("relationship");
        }

        @Test
        @DisplayName("Rejects node.mode=relationship with a single mapping (needs two endpoints)")
        void rejectsRelationshipModeWithSingleMapping() {
            final var props = new HashMap<String, String>();
            props.put("table.order_items.node.mode", "relationship");
            props.put("table.order_items.relationship.order_id.direction", "outgoing");
            props.put("table.order_items.relationship.order_id.type", "CONTAINS");
            props.put("table.order_items.relationship.order_id.target.label", "Order");

            assertThatThrownBy(() -> parse(props))
                    .isInstanceOf(ConfigException.class)
                    .hasMessageContaining("exactly two");
        }

        @Test
        @DisplayName("Rejects node.mode=relationship with more than two mappings")
        void rejectsRelationshipModeWithThreeMappings() {
            final var props = new HashMap<String, String>();
            props.put("table.order_items.node.mode", "relationship");
            props.put("table.order_items.relationship.order_id.direction", "outgoing");
            props.put("table.order_items.relationship.order_id.type", "CONTAINS");
            props.put("table.order_items.relationship.order_id.target.label", "Order");
            props.put("table.order_items.relationship.product_id.direction", "incoming");
            props.put("table.order_items.relationship.product_id.type", "CONTAINS");
            props.put("table.order_items.relationship.product_id.target.label", "Product");
            props.put("table.order_items.relationship.customer_id.direction", "outgoing");
            props.put("table.order_items.relationship.customer_id.type", "CONTAINS");
            props.put("table.order_items.relationship.customer_id.target.label", "Customer");

            assertThatThrownBy(() -> parse(props))
                    .isInstanceOf(ConfigException.class)
                    .hasMessageContaining("exactly two");
        }

        @Test
        @DisplayName("Rejects node.mode=relationship when an endpoint omits direction")
        void rejectsRelationshipModeWithoutDirection() {
            final var props = new HashMap<String, String>();
            props.put("table.order_items.node.mode", "relationship");
            props.put("table.order_items.relationship.order_id.direction", "outgoing");
            props.put("table.order_items.relationship.order_id.type", "CONTAINS");
            props.put("table.order_items.relationship.order_id.target.label", "Order");
            props.put("table.order_items.relationship.product_id.type", "CONTAINS");
            props.put("table.order_items.relationship.product_id.target.label", "Product");

            assertThatThrownBy(() -> parse(props))
                    .isInstanceOf(ConfigException.class)
                    .hasMessageContaining("direction");
        }

        @Test
        @DisplayName("Rejects node.mode=relationship when both endpoints are outgoing")
        void rejectsRelationshipModeWithTwoOutgoing() {
            final var props = new HashMap<String, String>();
            props.put("table.order_items.node.mode", "relationship");
            props.put("table.order_items.relationship.order_id.direction", "outgoing");
            props.put("table.order_items.relationship.order_id.type", "CONTAINS");
            props.put("table.order_items.relationship.order_id.target.label", "Order");
            props.put("table.order_items.relationship.product_id.direction", "outgoing");
            props.put("table.order_items.relationship.product_id.type", "CONTAINS");
            props.put("table.order_items.relationship.product_id.target.label", "Product");

            assertThatThrownBy(() -> parse(props))
                    .isInstanceOf(ConfigException.class)
                    .hasMessageContaining("one outgoing");
        }

        @Test
        @DisplayName("Rejects node.mode=relationship when both endpoints are incoming")
        void rejectsRelationshipModeWithTwoIncoming() {
            final var props = new HashMap<String, String>();
            props.put("table.order_items.node.mode", "relationship");
            props.put("table.order_items.relationship.order_id.direction", "incoming");
            props.put("table.order_items.relationship.order_id.type", "CONTAINS");
            props.put("table.order_items.relationship.order_id.target.label", "Order");
            props.put("table.order_items.relationship.product_id.direction", "incoming");
            props.put("table.order_items.relationship.product_id.type", "CONTAINS");
            props.put("table.order_items.relationship.product_id.target.label", "Product");

            assertThatThrownBy(() -> parse(props))
                    .isInstanceOf(ConfigException.class)
                    .hasMessageContaining("one incoming");
        }

        @Test
        @DisplayName("Rejects an invalid direction value")
        void rejectsInvalidDirection() {
            final var props = new HashMap<String, String>();
            props.put("table.order_items.node.mode", "relationship");
            props.put("table.order_items.relationship.order_id.direction", "sideways");
            props.put("table.order_items.relationship.order_id.type", "CONTAINS");
            props.put("table.order_items.relationship.order_id.target.label", "Order");
            props.put("table.order_items.relationship.product_id.direction", "incoming");
            props.put("table.order_items.relationship.product_id.type", "CONTAINS");
            props.put("table.order_items.relationship.product_id.target.label", "Product");

            assertThatThrownBy(() -> parse(props))
                    .isInstanceOf(ConfigException.class)
                    .hasMessageContaining("outgoing");
        }

        @Test
        @DisplayName("Rejects both include and exclude properties set")
        void rejectsMutuallyExclusiveFilters() {
            final var props = Map.of(
                    "table.customers.node.labels", "Customer",
                    "table.customers.node.id.properties", "id",
                    "table.customers.node.properties.include", "name",
                    "table.customers.node.properties.exclude", "email");

            assertThatThrownBy(() -> parse(props))
                    .isInstanceOf(ConfigException.class);
        }

        @Test
        @DisplayName("Rejects relationship mapping without type")
        void rejectsRelationshipWithoutType() {
            final var props = new HashMap<String, String>();
            props.put("table.orders.node.labels", "Order");
            props.put("table.orders.node.id.properties", "id");
            props.put("table.orders.relationship.customer_id.target.label", "Customer");

            assertThatThrownBy(() -> parse(props))
                    .isInstanceOf(ConfigException.class)
                    .hasMessageContaining("relationship.customer_id.type");
        }

        @Test
        @DisplayName("Rejects relationship mapping without target.label")
        void rejectsRelationshipWithoutTargetLabel() {
            final var props = new HashMap<String, String>();
            props.put("table.orders.node.labels", "Order");
            props.put("table.orders.node.id.properties", "id");
            props.put("table.orders.relationship.customer_id.type", "PLACED_BY");

            assertThatThrownBy(() -> parse(props))
                    .isInstanceOf(ConfigException.class)
                    .hasMessageContaining("relationship.customer_id.target.label");
        }

        @Test
        @DisplayName("Rejects invalid target.node.op value at configure time")
        void rejectsInvalidTargetNodeOp() {
            final var props = new HashMap<String, String>();
            props.put("table.orders.node.labels", "Order");
            props.put("table.orders.node.id.properties", "id");
            props.put("table.orders.relationship.customer_id.type", "PLACED_BY");
            props.put("table.orders.relationship.customer_id.target.label", "Customer");
            props.put("table.orders.relationship.customer_id.target.node.op", "create");

            assertThatThrownBy(() -> parse(props))
                    .isInstanceOf(ConfigException.class)
                    .hasMessageContaining("target.node.op")
                    .hasMessageContaining("match");
        }

        @Test
        @DisplayName("Rejects node.mode=node without node.labels, naming the table")
        void rejectsNodeModeWithoutLabels() {
            final var props = Map.of("table.customers.node.id.properties", "id");

            assertThatThrownBy(() -> parse(props))
                    .isInstanceOf(ConfigException.class)
                    .hasMessageContaining("table.customers.node.labels");
        }

        @Test
        @DisplayName("Rejects a misspelled node sub-key instead of silently ignoring it")
        void rejectsUnknownNodeKey() {
            final var props = Map.of(
                    "table.customers.node.labels", "Customer",
                    "table.customers.node.id.properties", "id",
                    "table.customers.node.lable", "Customer");

            assertThatThrownBy(() -> parse(props))
                    .isInstanceOf(ConfigException.class)
                    .hasMessageContaining("table.customers.node.lable")
                    .hasMessageContaining("Unknown");
        }

        @Test
        @DisplayName("Rejects a misspelled relationship sub-key")
        void rejectsUnknownRelationshipKey() {
            final var props = new HashMap<String, String>();
            props.put("table.orders.node.labels", "Order");
            props.put("table.orders.node.id.properties", "id");
            props.put("table.orders.relationship.customer_id.type", "PLACED_BY");
            props.put("table.orders.relationship.customer_id.target.label", "Customer");
            props.put("table.orders.relationship.customer_id.target.labol", "Customer");

            assertThatThrownBy(() -> parse(props))
                    .isInstanceOf(ConfigException.class)
                    .hasMessageContaining("table.orders.relationship.customer_id.target.labol")
                    .hasMessageContaining("Unknown");
        }
    }

}
