/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.data.Envelope;

class Neo4jCudConverterTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final Schema SOURCE_SCHEMA = SchemaBuilder.struct()
            .field("lsn", Schema.INT32_SCHEMA)
            .field("ts_ms", Schema.OPTIONAL_INT64_SCHEMA)
            .field("ts_us", Schema.OPTIONAL_INT64_SCHEMA)
            .field("ts_ns", Schema.OPTIONAL_INT64_SCHEMA)
            .field("table", Schema.OPTIONAL_STRING_SCHEMA)
            .build();

    private static final Schema CUSTOMER_SCHEMA = SchemaBuilder.struct()
            .field("id", Schema.INT32_SCHEMA)
            .field("first_name", Schema.STRING_SCHEMA)
            .field("last_name", Schema.STRING_SCHEMA)
            .field("email", Schema.STRING_SCHEMA)
            .build();

    private static final Schema ORDER_SCHEMA = SchemaBuilder.struct()
            .field("id", Schema.INT32_SCHEMA)
            .field("customer_id", Schema.INT32_SCHEMA)
            .field("total", Schema.FLOAT64_SCHEMA)
            .field("status", Schema.STRING_SCHEMA)
            .build();

    private static final Schema ORDER_ITEM_SCHEMA = SchemaBuilder.struct()
            .field("order_id", Schema.INT32_SCHEMA)
            .field("product_id", Schema.INT32_SCHEMA)
            .field("quantity", Schema.INT32_SCHEMA)
            .build();

    @Nested
    @DisplayName("Configuration validation")
    class ConfigValidation {

        @Test
        @DisplayName("Rejects node mode without id properties")
        void rejectsNodeModeWithoutIdProperties() {
            final var transform = new Neo4jCudConverter<SourceRecord>();
            final var props = Map.<String, String> of(
                    "node.labels", "Customer");

            assertThatThrownBy(() -> transform.configure(props))
                    .isInstanceOf(ConfigException.class);
            transform.close();
        }

        @Test
        @DisplayName("Rejects relationship mode without relationship config")
        void rejectsRelationshipModeWithoutRelConfig() {
            final var transform = new Neo4jCudConverter<SourceRecord>();
            final var props = Map.<String, String> of(
                    "node.mode", "relationship");

            assertThatThrownBy(() -> transform.configure(props))
                    .isInstanceOf(ConfigException.class);
            transform.close();
        }

        @Test
        @DisplayName("Rejects both include and exclude properties")
        void rejectsMutuallyExclusiveFilters() {
            final var transform = new Neo4jCudConverter<SourceRecord>();
            final var props = Map.<String, String> of(
                    "node.labels", "Customer",
                    "node.id.properties", "id",
                    "node.properties.include", "name",
                    "node.properties.exclude", "email");

            assertThatThrownBy(() -> transform.configure(props))
                    .isInstanceOf(ConfigException.class);
            transform.close();
        }
    }

    @Nested
    @DisplayName("Node mode with array output")
    class NodeModeArrayOutput {

        @Test
        @DisplayName("Transforms CREATE into a single-element array with node merge")
        void createEvent() throws Exception {
            final var transform = configureCustomerTransform();
            final var record = createCustomerRecord("c", customerAfter());

            final var result = transform.apply(record);

            final var events = parseArray(result);
            assertThat(events).hasSize(1);

            final var node = events.get(0);
            assertThat(node.get("type")).isEqualTo("node");
            assertThat(node.get("op")).isEqualTo("merge");
            assertThat(node.get("labels")).isEqualTo(List.of("Customer"));
            assertThat(asMap(node, "ids")).containsEntry("id", 1004);
            assertThat(asMap(node, "properties")).containsEntry("first_name", "John");
            assertThat(asMap(node, "properties")).doesNotContainKey("id");

            transform.close();
        }

        @Test
        @DisplayName("Transforms READ (snapshot) into a merge event")
        void readEvent() throws Exception {
            final var transform = configureCustomerTransform();
            final var record = createCustomerRecord("r", customerAfter());

            final var result = transform.apply(record);

            final var events = parseArray(result);
            assertThat(events.get(0).get("op")).isEqualTo("merge");

            transform.close();
        }

        @Test
        @DisplayName("Transforms UPDATE into a merge event with updated properties")
        void updateEvent() throws Exception {
            final var transform = configureCustomerTransform();
            final var after = customerAfter();
            after.put("email", "new@mail.org");
            final var record = createCustomerRecord("u", after, customerAfter());

            final var result = transform.apply(record);

            final var events = parseArray(result);
            assertThat(events.get(0).get("op")).isEqualTo("merge");
            assertThat(asMap(events.get(0), "properties")).containsEntry("email", "new@mail.org");

            transform.close();
        }

        @Test
        @DisplayName("Transforms DELETE into a delete event reading from before")
        void deleteEvent() throws Exception {
            final var transform = configureCustomerTransform();
            final var record = createDeleteRecord(customerEnvelope(), customerAfter());

            final var result = transform.apply(record);

            final var events = parseArray(result);
            assertThat(events).hasSize(1);
            assertThat(events.get(0).get("op")).isEqualTo("delete");
            assertThat(events.get(0).get("detach")).isEqualTo(true);
            assertThat(events.get(0)).doesNotContainKey("properties");

            transform.close();
        }

        @Test
        @DisplayName("DELETE with detach=false omits detach flag")
        void deleteWithoutDetach() throws Exception {
            final var transform = configureTransform(Map.of(
                    "node.labels", "Customer",
                    "node.id.properties", "id",
                    "node.delete.detach", "false"));
            final var record = createDeleteRecord(customerEnvelope(), customerAfter());

            final var result = transform.apply(record);

            final var events = parseArray(result);
            assertThat(events.get(0).get("detach")).isEqualTo(false);

            transform.close();
        }

        @Test
        @DisplayName("Excludes columns listed in node.properties.exclude")
        void excludeProperties() throws Exception {
            final var transform = configureTransform(Map.of(
                    "node.labels", "Customer",
                    "node.id.properties", "id",
                    "node.properties.exclude", "email"));
            final var record = createCustomerRecord("c", customerAfter());

            final var result = transform.apply(record);

            final var props = asMap(parseArray(result).get(0), "properties");
            assertThat(props).doesNotContainKey("email");
            assertThat(props).containsKey("first_name");

            transform.close();
        }

        @Test
        @DisplayName("Includes only columns listed in node.properties.include")
        void includeProperties() throws Exception {
            final var transform = configureTransform(Map.of(
                    "node.labels", "Customer",
                    "node.id.properties", "id",
                    "node.properties.include", "email"));
            final var record = createCustomerRecord("c", customerAfter());

            final var result = transform.apply(record);

            final var props = asMap(parseArray(result).get(0), "properties");
            assertThat(props).containsOnlyKeys("email");

            transform.close();
        }
    }

    @Nested
    @DisplayName("Node mode with FK relationships")
    class NodeModeWithRelationships {

        @Test
        @DisplayName("CREATE produces node + relationship in array mode")
        void createWithFk() throws Exception {
            final var transform = configureOrderTransform();
            final var record = createOrderRecord();

            final var result = transform.apply(record);

            final var events = parseArray(result);
            assertThat(events).hasSize(2);

            final var nodeEvent = events.get(0);
            assertThat(nodeEvent.get("type")).isEqualTo("node");
            assertThat(asMap(nodeEvent, "properties")).doesNotContainKey("customer_id");

            final var relEvent = events.get(1);
            assertThat(relEvent.get("type")).isEqualTo("relationship");
            assertThat(relEvent.get("rel_type")).isEqualTo("PLACED_BY");
            assertThat(asMap(asMap(relEvent, "from"), "ids")).containsEntry("id", 5001);
            assertThat(asMap(asMap(relEvent, "to"), "ids")).containsEntry("id", 1004);
            assertThat(asMap(relEvent, "to").get("op")).isEqualTo("match");

            transform.close();
        }

        @Test
        @DisplayName("DELETE in node mode produces only a node delete, no relationship events")
        void deleteSkipsRelationships() throws Exception {
            final var transform = configureOrderTransform();
            final var record = createDeleteRecord(orderEnvelope(), orderAfter());

            final var result = transform.apply(record);

            final var events = parseArray(result);
            assertThat(events).hasSize(1);
            assertThat(events.get(0).get("type")).isEqualTo("node");
            assertThat(events.get(0).get("op")).isEqualTo("delete");

            transform.close();
        }

        @Test
        @DisplayName("Null FK value skips the relationship event")
        void nullFkSkipped() throws Exception {
            final var nullableOrderSchema = SchemaBuilder.struct()
                    .field("id", Schema.INT32_SCHEMA)
                    .field("customer_id", Schema.OPTIONAL_INT32_SCHEMA)
                    .field("total", Schema.FLOAT64_SCHEMA)
                    .field("status", Schema.STRING_SCHEMA)
                    .build();
            final var nullableAfter = new Struct(nullableOrderSchema);
            nullableAfter.put("id", 5001);
            nullableAfter.put("customer_id", null);
            nullableAfter.put("total", 99.95);
            nullableAfter.put("status", "pending");

            final var envelope = Envelope.defineSchema()
                    .withName("test.Envelope")
                    .withRecord(nullableOrderSchema)
                    .withSource(SOURCE_SCHEMA)
                    .build();
            final var source = createSource("orders");
            final var payload = envelope.create(nullableAfter, source, Instant.now());
            final var record = new SourceRecord(new HashMap<>(), new HashMap<>(), "test.orders",
                    envelope.schema(), payload);

            final var transform = configureOrderTransform();

            final var result = transform.apply(record);

            final var events = parseArray(result);
            assertThat(events).hasSize(1);
            assertThat(events.get(0).get("type")).isEqualTo("node");

            transform.close();
        }
    }

    @Nested
    @DisplayName("Single output mode")
    class SingleOutputMode {

        @Test
        @DisplayName("Produces a single JSON object, not an array")
        void singleNodeOutput() throws Exception {
            final var transform = configureTransform(Map.of(
                    "node.labels", "Customer",
                    "node.id.properties", "id",
                    "output.mode", "single"));
            final var record = createCustomerRecord("c", customerAfter());

            final var result = transform.apply(record);

            final var json = (String) result.value();
            final var parsed = OBJECT_MAPPER.readValue(json, Map.class);
            assertThat(parsed.get("type")).isEqualTo("node");
            assertThat(result.valueSchema()).isEqualTo(Schema.STRING_SCHEMA);

            transform.close();
        }

        @Test
        @DisplayName("FK columns excluded from properties even in single mode")
        void fkExcludedInSingleMode() throws Exception {
            final var props = new HashMap<String, String>();
            props.put("node.labels", "Order");
            props.put("node.id.properties", "id");
            props.put("output.mode", "single");
            props.put("relationship.customer_id.type", "PLACED_BY");
            props.put("relationship.customer_id.target.label", "Customer");
            props.put("relationship.customer_id.target.id", "id");

            final var transform = configureTransform(props);
            final var record = createOrderRecord();

            final var result = transform.apply(record);

            final var parsed = OBJECT_MAPPER.readValue((String) result.value(), Map.class);
            assertThat(asMap(parsed, "properties")).doesNotContainKey("customer_id");

            transform.close();
        }
    }

    @Nested
    @DisplayName("Relationship mode (join tables)")
    class RelationshipMode {

        @Test
        @DisplayName("Array mode: produces node merges for both endpoints + relationship")
        void joinTableArrayMode() throws Exception {
            final var transform = configureOrderItemTransform("array");
            final var record = createOrderItemRecord();

            final var result = transform.apply(record);

            final var events = parseArray(result);
            assertThat(events).hasSize(3);

            assertThat(events.get(0).get("type")).isEqualTo("node");
            assertThat(events.get(0).get("labels")).isEqualTo(List.of("Order"));

            assertThat(events.get(1).get("type")).isEqualTo("node");
            assertThat(events.get(1).get("labels")).isEqualTo(List.of("Product"));

            final var rel = events.get(2);
            assertThat(rel.get("type")).isEqualTo("relationship");
            assertThat(rel.get("rel_type")).isEqualTo("CONTAINS");
            assertThat(asMap(rel, "from").get("op")).isEqualTo("match");
            assertThat(asMap(rel, "to").get("op")).isEqualTo("match");
            assertThat(asMap(rel, "properties")).containsEntry("quantity", 3);

            transform.close();
        }

        @Test
        @DisplayName("Single mode: produces relationship with merge endpoints, no separate node events")
        void joinTableSingleMode() throws Exception {
            final var transform = configureOrderItemTransform("single");
            final var record = createOrderItemRecord();

            final var result = transform.apply(record);

            final var parsed = OBJECT_MAPPER.readValue((String) result.value(), Map.class);
            assertThat(parsed.get("type")).isEqualTo("relationship");
            assertThat(asMap(parsed, "from").get("op")).isEqualTo("merge");
            assertThat(asMap(parsed, "to").get("op")).isEqualTo("merge");

            transform.close();
        }

        @Test
        @DisplayName("DELETE on join table produces relationship delete")
        void joinTableDelete() throws Exception {
            final var transform = configureOrderItemTransform("array");
            final var before = orderItemAfter();
            final var envelope = Envelope.defineSchema()
                    .withName("test.Envelope")
                    .withRecord(ORDER_ITEM_SCHEMA)
                    .withSource(SOURCE_SCHEMA)
                    .build();
            final var source = createSource("order_items");
            final var payload = envelope.delete(before, source, Instant.now());
            final var record = new SourceRecord(new HashMap<>(), new HashMap<>(), "test.order_items",
                    envelope.schema(), payload);

            final var result = transform.apply(record);

            final var events = parseArray(result);
            assertThat(events).hasSize(1);
            assertThat(events.get(0).get("type")).isEqualTo("relationship");
            assertThat(events.get(0).get("op")).isEqualTo("delete");

            transform.close();
        }
    }

    @Nested
    @DisplayName("Passthrough and tombstone handling")
    class PassthroughAndTombstones {

        @Test
        @DisplayName("Tombstone with tombstones.enabled=true passes through")
        void tombstonePassthrough() {
            final var transform = configureCustomerTransform();
            final var record = new SourceRecord(new HashMap<>(), new HashMap<>(), "test", null, null);

            final var result = transform.apply(record);

            assertThat(result).isSameAs(record);

            transform.close();
        }

        @Test
        @DisplayName("Tombstone with tombstones.enabled=false is dropped")
        void tombstoneDropped() {
            final var transform = configureTransform(Map.of(
                    "node.labels", "Customer",
                    "node.id.properties", "id",
                    "tombstones.enabled", "false"));
            final var record = new SourceRecord(new HashMap<>(), new HashMap<>(), "test", null, null);

            final var result = transform.apply(record);

            assertThat(result).isNull();

            transform.close();
        }

        @Test
        @DisplayName("Non-envelope record passes through unchanged")
        void nonEnvelopePassthrough() {
            final var transform = configureCustomerTransform();
            final var schema = SchemaBuilder.struct().name("some.random.Schema")
                    .field("id", Schema.INT32_SCHEMA).build();
            final var value = new Struct(schema);
            value.put("id", 1);
            final var record = new SourceRecord(new HashMap<>(), new HashMap<>(), "test", schema, value);

            final var result = transform.apply(record);

            assertThat(result).isSameAs(record);

            transform.close();
        }

        @Test
        @DisplayName("Truncate operation passes through unchanged")
        void truncatePassthrough() {
            final var transform = configureCustomerTransform();
            final var envelope = customerEnvelope();
            final var source = createSource("customers");
            source.put("lsn", 1234);
            final var payload = envelope.truncate(source, Instant.now());
            final var record = new SourceRecord(new HashMap<>(), new HashMap<>(), "test",
                    envelope.schema(), payload);

            final var result = transform.apply(record);

            assertThat(result).isSameAs(record);

            transform.close();
        }

        @Test
        @DisplayName("Output record preserves original key and topic")
        void preservesKeyAndTopic() throws Exception {
            final var transform = configureCustomerTransform();
            final var keySchema = SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).build();
            final var key = new Struct(keySchema);
            key.put("id", 1004);

            final var envelope = customerEnvelope();
            final var after = customerAfter();
            final var source = createSource("customers");
            final var payload = envelope.create(after, source, Instant.now());
            final var record = new SourceRecord(new HashMap<>(), new HashMap<>(),
                    "dbserver1.public.customers", keySchema, key, envelope.schema(), payload);

            final var result = transform.apply(record);

            assertThat(result.topic()).isEqualTo("dbserver1.public.customers");
            assertThat(result.keySchema()).isEqualTo(keySchema);
            assertThat(((Struct) result.key()).getInt32("id")).isEqualTo(1004);

            transform.close();
        }
    }

    @Nested
    @DisplayName("E2E: Full test pipeline")
    class EcommercePipeline {

        @Test
        @DisplayName("Full pipeline: customer, order with FK, order_item join table")
        void fullEcommercePipeline() throws Exception {
            final var customerTransform = configureCustomerTransform();
            final var customerRecord = createCustomerRecord("c", customerAfter());

            final var customerResult = customerTransform.apply(customerRecord);

            final var customerEvents = parseArray(customerResult);
            assertThat(customerEvents).hasSize(1);
            assertThat(asMap(customerEvents.get(0), "ids")).containsEntry("id", 1004);
            assertThat(asMap(customerEvents.get(0), "properties"))
                    .containsEntry("first_name", "John")
                    .containsEntry("last_name", "Foo")
                    .containsEntry("email", "john@foo.org");

            final var orderTransform = configureOrderTransform();
            final var orderRecord = createOrderRecord();

            final var orderResult = orderTransform.apply(orderRecord);

            final var orderEvents = parseArray(orderResult);
            assertThat(orderEvents).hasSize(2);

            final var orderNode = orderEvents.get(0);
            assertThat(orderNode.get("labels")).isEqualTo(List.of("Order"));
            assertThat(asMap(orderNode, "ids")).containsEntry("id", 5001);
            assertThat(asMap(orderNode, "properties"))
                    .containsEntry("total", 99.95)
                    .containsEntry("status", "pending")
                    .doesNotContainKey("customer_id");

            final var placedByRel = orderEvents.get(1);
            assertThat(placedByRel.get("rel_type")).isEqualTo("PLACED_BY");
            assertThat(asMap(asMap(placedByRel, "to"), "ids")).containsEntry("id", 1004);

            final var orderItemTransform = configureOrderItemTransform("array");
            final var orderItemRecord = createOrderItemRecord();

            final var orderItemResult = orderItemTransform.apply(orderItemRecord);

            final var orderItemEvents = parseArray(orderItemResult);
            assertThat(orderItemEvents).hasSize(3);

            assertThat(orderItemEvents.get(0).get("type")).isEqualTo("node");
            assertThat(orderItemEvents.get(0).get("labels")).isEqualTo(List.of("Order"));
            assertThat(asMap(orderItemEvents.get(0), "properties")).isEmpty();

            assertThat(orderItemEvents.get(1).get("type")).isEqualTo("node");
            assertThat(orderItemEvents.get(1).get("labels")).isEqualTo(List.of("Product"));

            final var containsRel = orderItemEvents.get(2);
            assertThat(containsRel.get("rel_type")).isEqualTo("CONTAINS");
            assertThat(asMap(containsRel, "properties")).containsEntry("quantity", 3);

            customerTransform.close();
            orderTransform.close();
            orderItemTransform.close();
        }

        @Test
        @DisplayName("Full delete lifecycle: delete customer cascades with detach")
        void deleteLifecycle() throws Exception {
            final var transform = configureCustomerTransform();
            final var createRecord = createCustomerRecord("c", customerAfter());
            transform.apply(createRecord);

            final var deleteRecord = createDeleteRecord(customerEnvelope(), customerAfter());

            final var result = transform.apply(deleteRecord);

            final var events = parseArray(result);
            final var deleteNode = events.get(0);
            assertThat(deleteNode.get("op")).isEqualTo("delete");
            assertThat(deleteNode.get("labels")).isEqualTo(List.of("Customer"));
            assertThat(asMap(deleteNode, "ids")).containsEntry("id", 1004);
            assertThat(deleteNode.get("detach")).isEqualTo(true);

            transform.close();
        }
    }

    @Nested
    @DisplayName("Relationship direction")
    class RelationshipDirection {

        @Test
        @DisplayName("Incoming direction swaps from/to endpoints")
        void incomingDirection() throws Exception {
            final var props = new HashMap<String, String>();
            props.put("node.labels", "Order");
            props.put("node.id.properties", "id");
            props.put("relationship.customer_id.type", "HAS_ORDER");
            props.put("relationship.customer_id.direction", "incoming");
            props.put("relationship.customer_id.target.label", "Customer");
            props.put("relationship.customer_id.target.id", "id");

            final var transform = configureTransform(props);
            final var record = createOrderRecord();

            final var result = transform.apply(record);

            final var events = parseArray(result);
            final var rel = events.get(1);
            assertThat(asMap(rel, "from").get("labels")).isEqualTo(List.of("Customer"));
            assertThat(asMap(rel, "to").get("labels")).isEqualTo(List.of("Order"));

            transform.close();
        }
    }

    private Neo4jCudConverter<SourceRecord> configureTransform(Map<String, String> props) {
        final var transform = new Neo4jCudConverter<SourceRecord>();
        transform.configure(props);
        return transform;
    }

    private Neo4jCudConverter<SourceRecord> configureCustomerTransform() {
        return configureTransform(Map.of(
                "node.labels", "Customer",
                "node.id.properties", "id"));
    }

    private Neo4jCudConverter<SourceRecord> configureOrderTransform() {
        final var props = new HashMap<String, String>();
        props.put("node.labels", "Order");
        props.put("node.id.properties", "id");
        props.put("relationship.customer_id.type", "PLACED_BY");
        props.put("relationship.customer_id.target.label", "Customer");
        props.put("relationship.customer_id.target.id", "id");
        return configureTransform(props);
    }

    private Neo4jCudConverter<SourceRecord> configureOrderItemTransform(String outputMode) {
        final var props = new HashMap<String, String>();
        props.put("node.mode", "relationship");
        props.put("output.mode", outputMode);
        props.put("relationship.order_id.type", "CONTAINS");
        props.put("relationship.order_id.target.label", "Order");
        props.put("relationship.order_id.target.id", "id");
        props.put("relationship.product_id.type", "CONTAINS");
        props.put("relationship.product_id.target.label", "Product");
        props.put("relationship.product_id.target.id", "id");
        props.put("relationship.order_id.properties", "quantity");
        return configureTransform(props);
    }

    private Envelope customerEnvelope() {
        return Envelope.defineSchema()
                .withName("test.Envelope")
                .withRecord(CUSTOMER_SCHEMA)
                .withSource(SOURCE_SCHEMA)
                .build();
    }

    private Envelope orderEnvelope() {
        return Envelope.defineSchema()
                .withName("test.Envelope")
                .withRecord(ORDER_SCHEMA)
                .withSource(SOURCE_SCHEMA)
                .build();
    }

    private Struct customerAfter() {
        final var after = new Struct(CUSTOMER_SCHEMA);
        after.put("id", 1004);
        after.put("first_name", "John");
        after.put("last_name", "Foo");
        after.put("email", "john@foo.org");
        return after;
    }

    private Struct orderAfter() {
        final var after = new Struct(ORDER_SCHEMA);
        after.put("id", 5001);
        after.put("customer_id", 1004);
        after.put("total", 99.95);
        after.put("status", "pending");
        return after;
    }

    private Struct orderItemAfter() {
        final var after = new Struct(ORDER_ITEM_SCHEMA);
        after.put("order_id", 5001);
        after.put("product_id", 200);
        after.put("quantity", 3);
        return after;
    }

    private Struct createSource(String table) {
        final var source = new Struct(SOURCE_SCHEMA);
        source.put("lsn", 1234);
        source.put("ts_ms", 1711900800000L);
        source.put("ts_us", 1711900800000000L);
        source.put("ts_ns", 1711900800000000000L);
        source.put("table", table);
        return source;
    }

    private SourceRecord createCustomerRecord(String op, Struct after) {
        return createCustomerRecord(op, after, null);
    }

    private SourceRecord createCustomerRecord(String op, Struct after, Struct before) {
        final var envelope = customerEnvelope();
        final var source = createSource("customers");
        final Struct payload;
        if ("u".equals(op)) {
            payload = envelope.update(before, after, source, Instant.now());
        }
        else {
            payload = envelope.create(after, source, Instant.now());
        }
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "test.customers",
                envelope.schema(), payload);
    }

    private SourceRecord createOrderRecord() {
        final var envelope = orderEnvelope();
        final var source = createSource("orders");
        final var after = orderAfter();
        final var payload = envelope.create(after, source, Instant.now());
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "test.orders",
                envelope.schema(), payload);
    }

    private SourceRecord createOrderItemRecord() {
        final var envelope = Envelope.defineSchema()
                .withName("test.Envelope")
                .withRecord(ORDER_ITEM_SCHEMA)
                .withSource(SOURCE_SCHEMA)
                .build();
        final var source = createSource("order_items");
        final var after = orderItemAfter();
        final var payload = envelope.create(after, source, Instant.now());
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "test.order_items",
                envelope.schema(), payload);
    }

    private SourceRecord createDeleteRecord(Envelope envelope, Struct before) {
        final var source = createSource("customers");
        final var payload = envelope.delete(before, source, Instant.now());
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "test",
                envelope.schema(), payload);
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> parseArray(SourceRecord record) throws JsonProcessingException {
        final var json = (String) record.value();
        return OBJECT_MAPPER.readValue(json, List.class);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> asMap(Map<String, Object> parent, String key) {
        return (Map<String, Object>) parent.get(key);
    }

}
