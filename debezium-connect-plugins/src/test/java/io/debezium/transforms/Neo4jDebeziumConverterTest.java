/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import io.debezium.transforms.neo4j.Neo4jPropertyTypesFixture;

/**
 * Unit tests for {@link Neo4jDebeziumConverter} (graph-to-relational). Input Neo4j CDC event Structs are built by
 * hand with {@link CdcBuilder}; the emitted Debezium envelope, primary-key Struct and output topic are asserted.
 */
class Neo4jDebeziumConverterTest {

    private Neo4jDebeziumConverter<SourceRecord> newTransform(Map<String, String> props) {
        final var t = new Neo4jDebeziumConverter<SourceRecord>();
        t.configure(props);
        return t;
    }

    private static SourceRecord recordOf(Struct value) {
        return new SourceRecord(null, null, "neo4j.topic", value.schema(), value);
    }

    @Nested
    @DisplayName("Zero-config node mapping")
    class ZeroConfigNodes {

        @Test
        @DisplayName("create maps to op=c envelope with after + record key, routed to the label table")
        void createNode() {
            final var value = CdcBuilder.node("c", List.of("Customer"))
                    .keys("Customer", "id", 1004L)
                    .after("id", 1004L, "first_name", "John", "last_name", "Foo", "email", "john@foo.org")
                    .build();

            final var result = newTransform(Map.of()).apply(recordOf(value));

            assertThat(result.topic()).isEqualTo("Customer");
            final var payload = (Struct) result.value();
            assertThat(payload.getString("op")).isEqualTo("c");
            final var after = payload.getStruct("after");
            assertThat(after.get("id")).isEqualTo(1004L);
            assertThat(after.get("first_name")).isEqualTo("John");
            assertThat(after.get("email")).isEqualTo("john@foo.org");
            assertThat(payload.getStruct("before")).isNull();

            final var key = (Struct) result.key();
            assertThat(key.get("id")).isEqualTo(1004L);
            assertThat(key.schema().fields()).hasSize(1);

            final var source = payload.getStruct("source");
            assertThat(source.getString("connector")).isEqualTo("neo4j");
            assertThat(source.getString("table")).isEqualTo("Customer");
        }

        @Test
        @DisplayName("update maps to op=u with the after image")
        void updateNode() {
            final var value = CdcBuilder.node("u", List.of("Customer"))
                    .keys("Customer", "id", 1004L)
                    .before("id", 1004L, "email", "old@foo.org")
                    .after("id", 1004L, "email", "new@foo.org")
                    .build();

            final var payload = (Struct) newTransform(Map.of()).apply(recordOf(value)).value();
            assertThat(payload.getString("op")).isEqualTo("u");
            assertThat(payload.getStruct("after").get("email")).isEqualTo("new@foo.org");
        }

        @Test
        @DisplayName("delete maps to op=d with the before image and record key")
        void deleteNode() {
            final var value = CdcBuilder.node("d", List.of("Customer"))
                    .keys("Customer", "id", 1004L)
                    .before("id", 1004L, "first_name", "John")
                    .build();

            final var result = newTransform(Map.of()).apply(recordOf(value));
            final var payload = (Struct) result.value();
            assertThat(payload.getString("op")).isEqualTo("d");
            assertThat(payload.getStruct("before").get("first_name")).isEqualTo("John");
            assertThat(((Struct) result.key()).get("id")).isEqualTo(1004L);
        }
    }

    @Nested
    @DisplayName("Naming and label overrides")
    class Overrides {

        @Test
        @DisplayName("snake_case naming lowercases table and columns")
        void snakeCase() {
            final var value = CdcBuilder.node("c", List.of("OrderItem"))
                    .keys("OrderItem", "id", 1L)
                    .after("id", 1L, "unitPrice", 9.99d)
                    .build();

            final var result = newTransform(Map.of(
                    "table.naming", "snake_case",
                    "column.naming", "snake_case")).apply(recordOf(value));

            assertThat(result.topic()).isEqualTo("order_item");
            assertThat(((Struct) result.value()).getStruct("after").get("unit_price")).isEqualTo(9.99d);
        }

        @Test
        @DisplayName("label.<Label>.table override routes to the configured table")
        void tableOverride() {
            final var value = CdcBuilder.node("c", List.of("Customer"))
                    .keys("Customer", "id", 1L)
                    .after("id", 1L)
                    .build();

            final var result = newTransform(Map.of("label.Customer.table", "customers")).apply(recordOf(value));
            assertThat(result.topic()).isEqualTo("customers");
        }

        @Test
        @DisplayName("properties.exclude drops the column but keeps the key")
        void propertiesExclude() {
            final var value = CdcBuilder.node("c", List.of("Customer"))
                    .keys("Customer", "id", 1L)
                    .after("id", 1L, "secret", "x", "name", "Jane")
                    .build();

            final var after = ((Struct) newTransform(Map.of("label.Customer.properties.exclude", "secret"))
                    .apply(recordOf(value)).value()).getStruct("after");
            assertThat(after.schema().field("secret")).isNull();
            assertThat(after.get("name")).isEqualTo("Jane");
            assertThat(after.get("id")).isEqualTo(1L);
        }

        @Test
        @DisplayName("properties.include keeps only listed columns while retaining the key")
        void propertiesInclude() {
            final var value = CdcBuilder.node("c", List.of("Customer"))
                    .keys("Customer", "id", 1L)
                    .after("id", 1L, "name", "Jane", "secret", "x", "note", "hi")
                    .build();

            final var after = ((Struct) newTransform(Map.of("label.Customer.properties.include", "name"))
                    .apply(recordOf(value)).value()).getStruct("after");
            assertThat(after.get("name")).isEqualTo("Jane");
            assertThat(after.get("id")).isEqualTo(1L);
            assertThat(after.schema().field("secret")).isNull();
            assertThat(after.schema().field("note")).isNull();
        }
    }

    @Nested
    @DisplayName("Multi-label ownership")
    class MultiLabel {

        @Test
        @DisplayName("mapped label owns the row")
        void mappedLabelOwns() {
            final var value = CdcBuilder.node("c", List.of("Person", "Customer"))
                    .keys("Customer", "id", 1L)
                    .after("id", 1L)
                    .build();

            final var result = newTransform(Map.of("label.Customer.table", "customers")).apply(recordOf(value));
            assertThat(result.topic()).isEqualTo("customers");
        }

        @Test
        @DisplayName("no mapped label on a multi-label node drops the record (warn)")
        void noMappedLabel() {
            final var value = CdcBuilder.node("c", List.of("Person", "Customer"))
                    .keys("Customer", "id", 1L)
                    .after("id", 1L)
                    .build();

            assertThat(newTransform(Map.of()).apply(recordOf(value))).isNull();
        }

        @Test
        @DisplayName("more than one mapped label fails when field.missing.behavior=fail")
        void ambiguousMappedLabelFails() {
            final var value = CdcBuilder.node("c", List.of("Person", "Customer"))
                    .keys("Customer", "id", 1L)
                    .after("id", 1L)
                    .build();

            final var t = newTransform(Map.of(
                    "label.Person.table", "people",
                    "label.Customer.table", "customers",
                    "field.missing.behavior", "fail"));
            assertThatThrownBy(() -> t.apply(recordOf(value)))
                    .isInstanceOf(DataException.class)
                    .hasMessageContaining("more than one mapped label");
        }
    }

    @Nested
    @DisplayName("Relationship join-table mode")
    class JoinTable {

        @Test
        @DisplayName("relationship becomes a join-table row with composite key")
        void joinRow() {
            final var value = CdcBuilder.relationship("c", "CONTAINS")
                    .start("Order", "id", 5001L)
                    .end("Product", "id", 200L)
                    .after("quantity", 3L)
                    .build();

            final var result = newTransform(Map.of(
                    "column.naming", "snake_case",
                    "relationship.CONTAINS.table", "order_items")).apply(recordOf(value));

            assertThat(result.topic()).isEqualTo("order_items");
            final var after = ((Struct) result.value()).getStruct("after");
            assertThat(after.get("order_id")).isEqualTo(5001L);
            assertThat(after.get("product_id")).isEqualTo(200L);
            assertThat(after.get("quantity")).isEqualTo(3L);

            final var key = (Struct) result.key();
            assertThat(key.get("order_id")).isEqualTo(5001L);
            assertThat(key.get("product_id")).isEqualTo(200L);
        }

        @Test
        @DisplayName("delete emits op=d keyed by the two endpoints, without properties")
        void joinDelete() {
            final var value = CdcBuilder.relationship("d", "CONTAINS")
                    .start("Order", "id", 5001L)
                    .end("Product", "id", 200L)
                    .build();

            final var payload = (Struct) newTransform(Map.of("column.naming", "snake_case"))
                    .apply(recordOf(value)).value();
            assertThat(payload.getString("op")).isEqualTo("d");
            assertThat(payload.getStruct("before").schema().field("quantity")).isNull();
        }

        @Test
        @DisplayName("start.column and end.column override the join-table foreign-key column names")
        void startEndColumnOverride() {
            final var value = CdcBuilder.relationship("c", "CONTAINS")
                    .start("Order", "id", 5001L)
                    .end("Product", "id", 200L)
                    .after("quantity", 3L)
                    .build();

            final var result = newTransform(Map.of(
                    "relationship.CONTAINS.start.column", "order_ref",
                    "relationship.CONTAINS.end.column", "product_ref")).apply(recordOf(value));

            final var after = ((Struct) result.value()).getStruct("after");
            assertThat(after.get("order_ref")).isEqualTo(5001L);
            assertThat(after.get("product_ref")).isEqualTo(200L);
            assertThat(after.get("quantity")).isEqualTo(3L);

            final var key = (Struct) result.key();
            assertThat(key.get("order_ref")).isEqualTo(5001L);
            assertThat(key.get("product_ref")).isEqualTo(200L);
        }

        @Test
        @DisplayName("relationship properties selection keeps only listed properties")
        void relationshipPropertiesSelection() {
            final var value = CdcBuilder.relationship("c", "CONTAINS")
                    .start("Order", "id", 5001L)
                    .end("Product", "id", 200L)
                    .after("quantity", 3L, "discount", 5L, "note", "gift")
                    .build();

            final var after = ((Struct) newTransform(Map.of(
                    "relationship.CONTAINS.properties", "quantity")).apply(recordOf(value)).value()).getStruct("after");
            assertThat(after.get("quantity")).isEqualTo(3L);
            assertThat(after.schema().field("discount")).isNull();
            assertThat(after.schema().field("note")).isNull();
        }
    }

    @Nested
    @DisplayName("Relationship foreign-key mode")
    class ForeignKey {

        @Test
        @DisplayName("start-owned relationship becomes a partial update of the owner table")
        void startOwner() {
            final var value = CdcBuilder.relationship("c", "PLACED_BY")
                    .start("Order", "id", 5001L)
                    .end("Customer", "id", 1004L)
                    .build();

            final var result = newTransform(Map.of(
                    "column.naming", "snake_case",
                    "relationship.PLACED_BY.mode", "foreign_key",
                    "relationship.PLACED_BY.table", "orders")).apply(recordOf(value));

            assertThat(result.topic()).isEqualTo("orders");
            final var payload = (Struct) result.value();
            assertThat(payload.getString("op")).isEqualTo("u");
            final var after = payload.getStruct("after");
            assertThat(after.get("id")).isEqualTo(5001L);
            assertThat(after.get("customer_id")).isEqualTo(1004L);
            assertThat(((Struct) result.key()).get("id")).isEqualTo(5001L);
        }

        @Test
        @DisplayName("owner=end puts the foreign key on the end table")
        void endOwner() {
            final var value = CdcBuilder.relationship("c", "PLACED")
                    .start("Customer", "id", 1004L)
                    .end("Order", "id", 5001L)
                    .build();

            final var after = ((Struct) newTransform(Map.of(
                    "column.naming", "snake_case",
                    "relationship.PLACED.mode", "foreign_key",
                    "relationship.PLACED.owner", "end")).apply(recordOf(value)).value()).getStruct("after");
            assertThat(after.get("id")).isEqualTo(5001L);
            assertThat(after.get("customer_id")).isEqualTo(1004L);
        }

        @Test
        @DisplayName("delete sets the foreign-key column to null as an update")
        void fkDelete() {
            final var value = CdcBuilder.relationship("d", "PLACED_BY")
                    .start("Order", "id", 5001L)
                    .end("Customer", "id", 1004L)
                    .build();

            final var payload = (Struct) newTransform(Map.of(
                    "column.naming", "snake_case",
                    "relationship.PLACED_BY.mode", "foreign_key")).apply(recordOf(value)).value();
            assertThat(payload.getString("op")).isEqualTo("u");
            final var after = payload.getStruct("after");
            assertThat(after.get("id")).isEqualTo(5001L);
            assertThat(after.get("customer_id")).isNull();
        }

        @Test
        @DisplayName("fk.column overrides the derived foreign-key column name")
        void fkColumnOverride() {
            final var value = CdcBuilder.relationship("c", "PLACED_BY")
                    .start("Order", "id", 5001L)
                    .end("Customer", "id", 1004L)
                    .build();

            final var after = ((Struct) newTransform(Map.of(
                    "relationship.PLACED_BY.mode", "foreign_key",
                    "relationship.PLACED_BY.fk.column", "cust_ref")).apply(recordOf(value)).value()).getStruct("after");
            assertThat(after.get("cust_ref")).isEqualTo(1004L);
            assertThat(after.schema().field("Customer_id")).isNull();
        }
    }

    @Nested
    @DisplayName("Error and edge handling")
    class Errors {

        @Test
        @DisplayName("missing key (no constraint) drops the record")
        void missingKey() {
            final var value = CdcBuilder.node("c", List.of("Customer"))
                    .after("id", 1L)
                    .build();
            assertThat(newTransform(Map.of()).apply(recordOf(value))).isNull();
        }

        @Test
        @DisplayName("primary-key value change is rejected")
        void keyValueChange() {
            final var value = CdcBuilder.node("u", List.of("Customer"))
                    .keys("Customer", "id", 2L)
                    .before("id", 1L, "email", "a@b.c")
                    .after("id", 2L, "email", "a@b.c")
                    .build();

            final var t = newTransform(Map.of("field.missing.behavior", "fail"));
            assertThatThrownBy(() -> t.apply(recordOf(value)))
                    .isInstanceOf(DataException.class)
                    .hasMessageContaining("primary-key value");
        }

        @Test
        @DisplayName("field.missing.behavior=ignore drops a keyless record silently")
        void ignoreDropsSilently() {
            final var value = CdcBuilder.node("c", List.of("Customer"))
                    .after("id", 1L)
                    .build();
            assertThat(newTransform(Map.of("field.missing.behavior", "ignore")).apply(recordOf(value))).isNull();
        }

        @Test
        @DisplayName("an update with no after image is dropped")
        void missingAfterImageDropped() {
            final var value = CdcBuilder.node("u", List.of("Customer"))
                    .keys("Customer", "id", 1L)
                    .before("id", 1L, "email", "a@b.c")
                    .build();
            assertThat(newTransform(Map.of()).apply(recordOf(value))).isNull();
        }

        @Test
        @DisplayName("non-Neo4j records pass through unchanged")
        void passThrough() {
            final var value = new Struct(SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build()).put("x", "y");
            final var record = recordOf(value);
            assertThat(newTransform(Map.of()).apply(record)).isSameAs(record);
        }

        @Test
        @DisplayName("tombstones pass through by default and drop when disabled")
        void tombstones() {
            final var tombstone = new SourceRecord(null, null, "t", null, null);
            assertThat(newTransform(Map.of()).apply(tombstone)).isSameAs(tombstone);
            assertThat(newTransform(Map.of("tombstones.enabled", "false")).apply(tombstone)).isNull();
        }
    }

    @Nested
    @DisplayName("Type mapping")
    class Types {

        @Test
        @DisplayName("temporal, point and array properties map to relational columns")
        void neo4jTypes() {
            final var value = CdcBuilder.node("c", List.of("Person"))
                    .keys("Person", "id", 1L)
                    .after("id", 1L,
                            "born", Neo4jPropertyTypesFixture.date("1990-01-15"),
                            "lastSeen", Neo4jPropertyTypesFixture.zonedDateTime("2021-06-15T10:15:30+01:00"),
                            "location", Neo4jPropertyTypesFixture.point(4326, 56.78, 12.34),
                            "tags", List.of("a", "b", "c"))
                    .build();

            final var after = ((Struct) newTransform(Map.of()).apply(recordOf(value)).value()).getStruct("after");
            assertThat(after.schema().field("born").schema().name()).isEqualTo("io.debezium.time.Date");
            assertThat(after.schema().field("lastSeen").schema().name()).isEqualTo("io.debezium.time.ZonedTimestamp");
            assertThat(after.get("lastSeen")).isEqualTo("2021-06-15T10:15:30+01:00");
            assertThat(after.schema().field("location").schema().type()).isEqualTo(Schema.Type.STRING);
            assertThat((String) after.get("location")).contains("56.78").contains("12.34");
            assertThat(after.schema().field("tags").schema().type()).isEqualTo(Schema.Type.ARRAY);
            assertThat(after.get("tags")).isEqualTo(List.of("a", "b", "c"));
        }
    }
}
