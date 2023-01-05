/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.partitions;

import static io.debezium.data.Envelope.Operation.CREATE;
import static io.debezium.data.Envelope.Operation.DELETE;
import static io.debezium.data.Envelope.Operation.TRUNCATE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import io.debezium.data.Envelope;

public class ComputePartitionTest {

    public static final Schema VALUE_SCHEMA = SchemaBuilder.struct()
            .name("mysql-server-1.inventory.products.Value")
            .field("id", Schema.INT64_SCHEMA)
            .field("price", Schema.FLOAT32_SCHEMA)
            .field("product", Schema.STRING_SCHEMA)
            .build();

    private final ComputePartition<SourceRecord> computePartitionTransformation = new ComputePartition<>();

    @Test
    public void correctComputeKafkaPartitionBasedOnConfiguredFieldOnCreateAndUpdateMySqlEvents() {

        configureTransformation("inventory.products", "inventory.products:product", "inventory.products:2");

        final SourceRecord eventRecord = buildSourceRecord("mysql", "inventory", null, "products", productRow(1L, 1.0F, "APPLE"), CREATE);

        SourceRecord transformed = computePartitionTransformation.apply(eventRecord);

        assertThat(transformed.kafkaPartition()).isZero();
    }

    @Test
    public void correctComputeKafkaPartitionBasedOnConfiguredFieldOnCreateAndUpdatePostgreSQLEvents() {

        configureTransformation("inventory.products", "inventory.products:product", "inventory.products:2");

        final SourceRecord eventRecord = buildSourceRecord("postgres", "postgres", "inventory", "products", productRow(1L, 1.0F, "APPLE"), CREATE);

        SourceRecord transformed = computePartitionTransformation.apply(eventRecord);

        assertThat(transformed.kafkaPartition()).isZero();
    }

    @Test
    public void correctComputeKafkaPartitionBasedOnConfiguredFieldOnDeleteEvents() {

        configureTransformation("inventory.products", "inventory.products:product", "inventory.products:2");

        final SourceRecord eventRecord = buildSourceRecord("mysql", "inventory", null, "products", productRow(1L, 1.0F, "APPLE"), DELETE);

        SourceRecord transformed = computePartitionTransformation.apply(eventRecord);

        assertThat(transformed.kafkaPartition()).isZero();
    }

    @Test
    public void partitionNotComputedOnTruncateEvent() {

        configureTransformation("inventory.products", "inventory.products:product", "inventory.products:2");

        final SourceRecord eventRecord = buildSourceRecord("mysql", "inventory", null, "products", productRow(1L, 1.0F, "APPLE"), TRUNCATE);

        SourceRecord transformed = computePartitionTransformation.apply(eventRecord);

        assertThat(transformed.kafkaPartition()).isEqualTo(eventRecord.kafkaPartition());
    }

    @Test
    public void rowWithSameConfiguredFieldValueWillHaveTheSamePartition() {

        configureTransformation("inventory.products", "inventory.products:product", "inventory.products:2");

        final SourceRecord eventRecord1 = buildSourceRecord("mysql", "inventory", null, "products", productRow(1L, 1.0F, "APPLE"), CREATE);

        SourceRecord transformed1 = computePartitionTransformation.apply(eventRecord1);

        final SourceRecord eventRecord2 = buildSourceRecord("mysql", "inventory", null, "products", productRow(2L, 2.0F, "APPLE"), CREATE);

        SourceRecord transformed2 = computePartitionTransformation.apply(eventRecord2);

        assertThat(transformed1.kafkaPartition()).isZero();
        assertThat(transformed2.kafkaPartition()).isZero();
    }

    @Test
    public void rowWithDifferentConfiguredFieldValueWillHaveDifferentPartition() {

        configureTransformation("inventory.products", "inventory.products:product", "inventory.products:2");

        final SourceRecord eventRecord1 = buildSourceRecord("mysql", "inventory", null, "products", productRow(1L, 1.0F, "APPLE"), CREATE);

        SourceRecord transformed1 = computePartitionTransformation.apply(eventRecord1);

        final SourceRecord eventRecord2 = buildSourceRecord("mysql", "inventory", null, "products", productRow(3L, 0.95F, "BANANA"), CREATE);

        SourceRecord transformed2 = computePartitionTransformation.apply(eventRecord2);

        assertThat(transformed1.kafkaPartition()).isNotEqualTo(transformed2.kafkaPartition());
    }

    @Test
    public void notConsistentConfigurationSizeWillThrowConnectionException() {

        assertThatThrownBy(() -> configureTransformation("inventory.orders,inventory.products", "inventory.orders:purchaser,inventory.products:product", "purchaser:2"))
                .isInstanceOf(ComputePartitionException.class)
                .hasMessageContaining(
                        "Unable to validate config. partition.data-collections.partition.num.mappings and partition.data-collections.field.mappings has different number of table defined");
    }

    @Test
    public void notConsistentConfigurationWillThrowConnectionException() {

        assertThatThrownBy(
                () -> configureTransformation("inventory.orders,inventory.products", "inventory.orders:purchaser,inventory.products:product", "prod:2,purchaser:2"))
                        .isInstanceOf(ComputePartitionException.class)
                        .hasMessageContaining(
                                "Unable to validate config. partition.data-collections.partition.num.mappings and partition.data-collections.field.mappings has different tables defined");
    }

    @Test
    public void negativeHashCodeValueWillBeCorrectlyManaged() {

        configureTransformation("inventory.products", "inventory.products:product", "inventory.products:3");

        final SourceRecord eventRecord1 = buildSourceRecord("mysql", "inventory", null, "products", productRow(1L, 1.0F, "orange"), CREATE);

        SourceRecord transformed1 = computePartitionTransformation.apply(eventRecord1);

        assertThat(transformed1.kafkaPartition()).isEqualTo(1);
    }

    @Test
    public void zeroAsPartitionNumberWillThrowConnectionException() {
        assertThatThrownBy(
                () -> configureTransformation("inventory.orders,inventory.products", "inventory.orders:purchaser,inventory.products:products",
                        "inventory.products:0,inventory.orders:2"))
                                .isInstanceOf(ComputePartitionException.class)
                                .hasMessageContaining(
                                        "Unable to validate config. partition.data-collections.partition.num.mappings: partition number for 'inventory.products' must be positive");
    }

    @Test
    public void negativeAsPartitionNumberWillThrowConnectionException() {
        assertThatThrownBy(
                () -> configureTransformation("inventory.orders,inventory.products", "inventory.orders:purchaser,inventory.products:products",
                        "inventory.products:-3,inventory.orders:2"))
                                .isInstanceOf(ComputePartitionException.class)
                                .hasMessageContaining(
                                        "Unable to validate config. partition.data-collections.partition.num.mappings: partition number for 'inventory.products' must be positive");
    }

    private SourceRecord buildSourceRecord(String connector, String db, String schema, String tableName, Struct row, Envelope.Operation operation) {

        String dataCollectionFieldName = "mongodb".equals(connector) ? "collection" : "table";

        SchemaBuilder sourceSchemaBuilder = SchemaBuilder.struct()
                .name("source")
                .field("connector", Schema.STRING_SCHEMA)
                .field("db", Schema.STRING_SCHEMA)
                .field(dataCollectionFieldName, Schema.STRING_SCHEMA);

        if (schema != null) {
            sourceSchemaBuilder.field("schema", Schema.STRING_SCHEMA);
        }

        Schema sourceSchema = sourceSchemaBuilder.build();

        Envelope createEnvelope = Envelope.defineSchema()
                .withName("mysql-server-1.inventory.product.Envelope")
                .withRecord(VALUE_SCHEMA)
                .withSource(sourceSchema)
                .build();

        Struct source = new Struct(sourceSchema);
        source.put("connector", connector);
        source.put("db", db);
        source.put(dataCollectionFieldName, tableName);
        if (schema != null) {
            source.put("schema", schema);
        }

        Struct payload = createEnvelope.create(row, source, Instant.now());

        switch (operation) {
            case CREATE:
            case UPDATE:
            case READ:
                payload = createEnvelope.create(row, source, Instant.now());
                break;
            case DELETE:
                payload = createEnvelope.delete(row, source, Instant.now());
                break;
            case TRUNCATE:
                payload = createEnvelope.truncate(source, Instant.now());
                break;
        }

        return new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "prefix.inventory.products",
                createEnvelope.schema(), payload);
    }

    private void configureTransformation(String tables, String tableFieldMapping, String tablePartitionNumMapping) {
        computePartitionTransformation.configure(Map.of(
                "partition.data-collections.field.mappings", tableFieldMapping,
                "partition.data-collections.partition.num.mappings", tablePartitionNumMapping));
    }

    private Struct productRow(long id, float price, String name) {
        return new Struct(VALUE_SCHEMA)
                .put("id", id)
                .put("price", price)
                .put("product", name);
    }
}
