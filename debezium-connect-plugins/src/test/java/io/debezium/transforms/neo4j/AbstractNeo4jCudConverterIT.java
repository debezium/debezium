/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.neo4j;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.config.Configuration;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.transforms.Neo4jCudConverter;

public abstract class AbstractNeo4jCudConverterIT<T extends SourceConnector> extends AbstractAsyncEngineConnectorTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    protected abstract Class<T> getConnectorClass();

    protected abstract JdbcConnection databaseConnection();

    protected abstract Configuration.Builder getConfigurationBuilder();

    protected abstract void createTables() throws Exception;

    protected abstract void waitForStreamingStarted() throws InterruptedException;

    protected abstract String topicName(String table);

    @BeforeEach
    void setUp() throws Exception {
        createTables();
    }

    @AfterEach
    void tearDown() throws Exception {
        stopConnector();
        assertNoRecordsToConsume();
    }

    @Test
    @DisplayName("INSERT on entity table produces a node merge CUD event from real CDC")
    void shouldTransformInsertToNodeMerge() throws Exception {
        startStreaming("customers");
        insertCustomer(1004, "John", "Foo", "john@foo.org");

        final var records = consumeRecordsByTopic(1);
        final var record = records.recordsForTopic(topicName("customers")).get(0);

        try (var smt = configuredTransform(customerConfig())) {
            final var result = smt.apply(record);

            final var events = parseArray(result);
            assertThat(events).hasSize(1);

            final var node = events.get(0);
            assertThat(node.get("type")).isEqualTo("node");
            assertThat(node.get("op")).isEqualTo("merge");
            assertThat(node.get("labels")).isEqualTo(List.of("Customer"));
            assertThat(asMap(node, "ids")).containsEntry("id", 1004);
            assertThat(asMap(node, "properties"))
                    .containsEntry("first_name", "John")
                    .containsEntry("last_name", "Foo")
                    .containsEntry("email", "john@foo.org")
                    .doesNotContainKey("id");
        }
    }

    @Test
    @DisplayName("UPDATE produces a node merge CUD event with updated properties from real CDC")
    void shouldTransformUpdateToNodeMerge() throws Exception {
        startStreaming("customers");
        insertCustomer(1004, "John", "Foo", "john@foo.org");
        consumeRecordsByTopic(1);

        databaseConnection().execute(
                "UPDATE customers SET email = 'new@mail.org' WHERE id = 1004");

        final var records = consumeRecordsByTopic(1);
        final var record = records.recordsForTopic(topicName("customers")).get(0);

        try (var smt = configuredTransform(customerConfig())) {
            final var result = smt.apply(record);

            final var events = parseArray(result);
            assertThat(events.get(0).get("op")).isEqualTo("merge");
            assertThat(asMap(events.get(0), "properties"))
                    .containsEntry("email", "new@mail.org");
        }
    }

    @Test
    @DisplayName("DELETE produces a node delete CUD event with detach from real CDC")
    void shouldTransformDeleteToNodeDelete() throws Exception {
        startStreaming("customers");
        insertCustomer(1004, "John", "Foo", "john@foo.org");
        consumeRecordsByTopic(1);

        databaseConnection().execute("DELETE FROM customers WHERE id = 1004");

        final var records = consumeRecordsByTopic(2);
        final var deleteRecord = records.recordsForTopic(topicName("customers")).get(0);

        try (var smt = configuredTransform(customerConfig())) {
            final var result = smt.apply(deleteRecord);

            final var events = parseArray(result);
            assertThat(events).hasSize(1);
            assertThat(events.get(0).get("op")).isEqualTo("delete");
            assertThat(events.get(0).get("detach")).isEqualTo(true);
            assertThat(asMap(events.get(0), "ids")).containsEntry("id", 1004);
        }
    }

    @Test
    @DisplayName("INSERT on entity with FK produces node + relationship CUD events from real CDC")
    void shouldTransformInsertWithFkToNodeAndRelationship() throws Exception {
        startStreaming("customers,orders");
        insertCustomer(1004, "John", "Foo", "john@foo.org");
        consumeRecordsByTopic(1);

        insertOrder(5001, 1004, 99.95, "pending");

        final var records = consumeRecordsByTopic(1);
        final var record = records.recordsForTopic(topicName("orders")).get(0);

        try (var smt = configuredTransform(orderConfig())) {
            final var result = smt.apply(record);

            final var events = parseArray(result);
            assertThat(events).hasSize(2);

            final var node = events.get(0);
            assertThat(node.get("type")).isEqualTo("node");
            assertThat(node.get("labels")).isEqualTo(List.of("Order"));
            assertThat(asMap(node, "properties"))
                    .containsEntry("status", "pending")
                    .doesNotContainKey("customer_id");

            final var rel = events.get(1);
            assertThat(rel.get("type")).isEqualTo("relationship");
            assertThat(rel.get("rel_type")).isEqualTo("PLACED_BY");
            assertThat(asMap(asMap(rel, "to"), "ids")).containsEntry("id", 1004);
        }
    }

    @Test
    @DisplayName("INSERT on join table produces node merges + relationship from real CDC")
    void shouldTransformJoinTableInsertToRelationship() throws Exception {
        startStreaming("customers,orders,products,order_items");
        insertCustomer(1004, "John", "Foo", "john@foo.org");
        insertProduct(200, "Widget", 19.99);
        insertOrder(5001, 1004, 99.95, "pending");
        consumeRecordsByTopic(3);

        insertOrderItem(5001, 200, 3);

        final var records = consumeRecordsByTopic(1);
        final var record = records.recordsForTopic(topicName("order_items")).get(0);

        try (var smt = configuredTransform(orderItemConfig())) {
            final var result = smt.apply(record);

            final var events = parseArray(result);
            assertThat(events).hasSize(3);

            assertThat(events.get(0).get("type")).isEqualTo("node");
            assertThat(events.get(0).get("labels")).isEqualTo(List.of("Order"));

            assertThat(events.get(1).get("type")).isEqualTo("node");
            assertThat(events.get(1).get("labels")).isEqualTo(List.of("Product"));

            final var rel = events.get(2);
            assertThat(rel.get("type")).isEqualTo("relationship");
            assertThat(rel.get("rel_type")).isEqualTo("CONTAINS");
            assertThat(asMap(rel, "properties")).containsEntry("quantity", 3);
        }
    }

    @Test
    @DisplayName("Full e-commerce pipeline: customer, order, order_item through real CDC")
    void shouldHandleFullEcommercePipeline() throws Exception {
        startStreaming("customers,orders,products,order_items");

        insertCustomer(1004, "John", "Foo", "john@foo.org");
        insertProduct(200, "Widget", 19.99);
        insertOrder(5001, 1004, 99.95, "pending");
        insertOrderItem(5001, 200, 3);

        final var records = consumeRecordsByTopic(4);

        try (var customerSmt = configuredTransform(customerConfig())) {
            final var customerRecord = records.recordsForTopic(topicName("customers")).get(0);
            final var customerEvents = parseArray(customerSmt.apply(customerRecord));
            assertThat(customerEvents).hasSize(1);
            assertThat(customerEvents.get(0).get("type")).isEqualTo("node");
            assertThat(asMap(customerEvents.get(0), "ids")).containsEntry("id", 1004);
        }

        try (var orderSmt = configuredTransform(orderConfig())) {
            final var orderRecord = records.recordsForTopic(topicName("orders")).get(0);
            final var orderEvents = parseArray(orderSmt.apply(orderRecord));
            assertThat(orderEvents).hasSize(2);
            assertThat(orderEvents.get(1).get("rel_type")).isEqualTo("PLACED_BY");
        }

        try (var orderItemSmt = configuredTransform(orderItemConfig())) {
            final var orderItemRecord = records.recordsForTopic(topicName("order_items")).get(0);
            final var orderItemEvents = parseArray(orderItemSmt.apply(orderItemRecord));
            assertThat(orderItemEvents).hasSize(3);
            assertThat(orderItemEvents.get(2).get("rel_type")).isEqualTo("CONTAINS");
            assertThat(asMap(orderItemEvents.get(2), "properties")).containsEntry("quantity", 3);
        }
    }

    // --- DML helpers (standard SQL, override if dialect requires) ---

    protected void insertCustomer(int id, String firstName, String lastName, String email) throws SQLException {
        databaseConnection().execute(String.format(
                "INSERT INTO customers (id, first_name, last_name, email) VALUES (%d, '%s', '%s', '%s')",
                id, firstName, lastName, email));
    }

    protected void insertOrder(int id, int customerId, double total, String status) throws SQLException {
        databaseConnection().execute(String.format(
                "INSERT INTO orders (id, customer_id, total, status) VALUES (%d, %d, %s, '%s')",
                id, customerId, total, status));
    }

    protected void insertProduct(int id, String name, double price) throws SQLException {
        databaseConnection().execute(String.format(
                "INSERT INTO products (id, name, price) VALUES (%d, '%s', %s)",
                id, name, price));
    }

    protected void insertOrderItem(int orderId, int productId, int quantity) throws SQLException {
        databaseConnection().execute(String.format(
                "INSERT INTO order_items (order_id, product_id, quantity) VALUES (%d, %d, %d)",
                orderId, productId, quantity));
    }

    // --- SMT configuration helpers ---

    private Map<String, String> customerConfig() {
        return Map.of(
                "node.labels", "Customer",
                "node.id.properties", "id");
    }

    private Map<String, String> orderConfig() {
        final var config = new HashMap<String, String>();
        config.put("node.labels", "Order");
        config.put("node.id.properties", "id");
        config.put("relationship.customer_id.type", "PLACED_BY");
        config.put("relationship.customer_id.target.label", "Customer");
        config.put("relationship.customer_id.target.id", "id");
        return config;
    }

    private Map<String, String> orderItemConfig() {
        final var config = new HashMap<String, String>();
        config.put("node.mode", "relationship");
        config.put("relationship.order_id.type", "CONTAINS");
        config.put("relationship.order_id.target.label", "Order");
        config.put("relationship.order_id.target.id", "id");
        config.put("relationship.product_id.type", "CONTAINS");
        config.put("relationship.product_id.target.label", "Product");
        config.put("relationship.product_id.target.id", "id");
        config.put("relationship.order_id.properties", "quantity");
        return config;
    }

    // --- Infrastructure helpers ---

    protected void startStreaming(String tables) throws Exception {
        final var tableList = buildTableIncludeList(tables);
        final var config = getConfigurationBuilder()
                .with("table.include.list", tableList)
                .build();
        start(getConnectorClass(), config);
        assertConnectorIsRunning();
        waitForStreamingStarted();
        assertNoRecordsToConsume();
    }

    protected String buildTableIncludeList(String commaSeparatedTables) {
        final var sb = new StringBuilder();
        for (final var table : commaSeparatedTables.split(",")) {
            if (sb.length() > 0) {
                sb.append(",");
            }
            sb.append(schemaPrefix()).append(".").append(table.trim());
        }
        return sb.toString();
    }

    protected String schemaPrefix() {
        return "public";
    }

    private Neo4jCudConverter<SourceRecord> configuredTransform(Map<String, String> config) {
        final var smt = new Neo4jCudConverter<SourceRecord>();
        smt.configure(config);
        return smt;
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
