/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.transforms.partitions;

import static io.debezium.data.Envelope.Operation.CREATE;
import static io.debezium.data.Envelope.Operation.DELETE;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import io.debezium.data.Envelope;

public class ComputePartitionTest {

    public static final Schema VALUE_SCHEMA = SchemaBuilder.struct()
            .name("mysql-server-1.inventory.product.Value")
            .field("id", Schema.INT64_SCHEMA)
            .field("price", Schema.FLOAT32_SCHEMA)
            .field("product", Schema.STRING_SCHEMA)
            .build();
    public static final Schema SOURCE_SCHEMA = SchemaBuilder.struct()
            .name("source")
            .field("table", Schema.STRING_SCHEMA).build();

    private final ComputePartition<SourceRecord> computePartitionTransformation = new ComputePartition<>();

    @Test
    public void correctComputeKafkaPartitionBasedOnConfiguredFieldOnCreateAndUpdateEvents() {

        configureTransformation("products", "products:product", "products:2");

        final SourceRecord eventRecord = buildSourceRecord("products", productRow(1L, 1.0F, "APPLE"), CREATE);

        SourceRecord transformed = computePartitionTransformation.apply(eventRecord);

        assertThat(transformed.kafkaPartition()).isZero();
    }

    @Test
    public void correctComputeKafkaPartitionBasedOnConfiguredFieldOnDeleteEvents() {

        configureTransformation("products", "products:product", "products:2");

        final SourceRecord eventRecord = buildSourceRecord("products", productRow(1L, 1.0F, "APPLE"), DELETE);

        SourceRecord transformed = computePartitionTransformation.apply(eventRecord);

        assertThat(transformed.kafkaPartition()).isZero();
    }

    @Test
    public void rowWithSameConfiguredFieldValueWillHaveTheSamePartition() {

        configureTransformation("products", "products:product", "products:2");

        final SourceRecord eventRecord1 = buildSourceRecord("products", productRow(1L, 1.0F, "APPLE"), CREATE);

        SourceRecord transformed1 = computePartitionTransformation.apply(eventRecord1);

        final SourceRecord eventRecord2 = buildSourceRecord("products", productRow(2L, 2.0F, "APPLE"), CREATE);

        SourceRecord transformed2 = computePartitionTransformation.apply(eventRecord2);

        assertThat(transformed1.kafkaPartition()).isZero();
        assertThat(transformed2.kafkaPartition()).isZero();
    }

    @Test
    public void rowWithDifferentConfiguredFieldValueWillHaveDifferentPartition() {

        configureTransformation("products", "products:product", "products:2");

        final SourceRecord eventRecord1 = buildSourceRecord("products", productRow(1L, 1.0F, "APPLE"), CREATE);

        SourceRecord transformed1 = computePartitionTransformation.apply(eventRecord1);

        final SourceRecord eventRecord2 = buildSourceRecord("products", productRow(3L, 0.95F, "BANANA"), CREATE);

        SourceRecord transformed2 = computePartitionTransformation.apply(eventRecord2);

        assertThat(transformed1.kafkaPartition()).isNotEqualTo(transformed2.kafkaPartition());
    }

    @Test
    public void notConfiguredTableRecordsWillBeSkipped() {

        configureTransformation("orders", "orders:purchaser", "purchaser:2");

        final SourceRecord eventRecord1 = buildSourceRecord("products", productRow(1L, 1.0F, "APPLE"), CREATE);

        SourceRecord transformed1 = computePartitionTransformation.apply(eventRecord1);

        assertThat(transformed1.kafkaPartition()).isEqualTo(eventRecord1.kafkaPartition());
    }

    @Test
    public void notConfiguredTableFieldMappingRecordsWillBeSkipped() {

        configureTransformation("orders,products", "orders:purchaser", "purchaser:2");

        final SourceRecord eventRecord1 = buildSourceRecord("products", productRow(1L, 1.0F, "APPLE"), CREATE);

        SourceRecord transformed1 = computePartitionTransformation.apply(eventRecord1);

        assertThat(transformed1.kafkaPartition()).isEqualTo(eventRecord1.kafkaPartition());
    }

    @Test
    public void notConfiguredTablePartitionNumMappingRecordsWillBeSkipped() {

        configureTransformation("orders,products", "orders:purchaser,products:product", "purchaser:2");

        final SourceRecord eventRecord1 = buildSourceRecord("products", productRow(1L, 1.0F, "APPLE"), CREATE);

        SourceRecord transformed1 = computePartitionTransformation.apply(eventRecord1);

        assertThat(transformed1.kafkaPartition()).isEqualTo(eventRecord1.kafkaPartition());
    }

    @NotNull
    private SourceRecord buildSourceRecord(String tableName, Struct row, Envelope.Operation operation) {

        Envelope createEnvelope = Envelope.defineSchema()
                .withName("mysql-server-1.inventory.product.Envelope")
                .withRecord(VALUE_SCHEMA)
                .withSource(SOURCE_SCHEMA)
                .build();

        Struct source = new Struct(SOURCE_SCHEMA);
        source.put("table", tableName);

        Struct payload = createEnvelope.create(row, source, Instant.now());
        if (operation.equals(Envelope.Operation.DELETE)) {
            payload = createEnvelope.delete(row, source, Instant.now());
        }
        return new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "prefix.inventory.product",
                createEnvelope.schema(), payload);
    }

    private void configureTransformation(String tables, String tableFieldMapping, String tablePartitionNumMapping) {
        computePartitionTransformation.configure(Map.of(
                "partition.table.list", tables,
                "partition.table.field.mappings", tableFieldMapping,
                "partition.table.partition.num.mappings", tablePartitionNumMapping));
    }

    private Struct productRow(long id, float price, String name) {
        return new Struct(VALUE_SCHEMA)
                .put("id", id)
                .put("price", price)
                .put("product", name);
    }
}
