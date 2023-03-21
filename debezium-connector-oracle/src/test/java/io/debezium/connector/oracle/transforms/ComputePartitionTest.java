/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.transforms;

import static io.debezium.data.Envelope.Operation.CREATE;
import static io.debezium.data.Envelope.Operation.DELETE;
import static io.debezium.data.Envelope.Operation.TRUNCATE;
import static io.debezium.transforms.SmtTestFramework.VALUE_SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import io.debezium.transforms.SmtTestFramework;
import io.debezium.transforms.partitions.ComputePartition;
import io.debezium.transforms.partitions.ComputePartitionException;

public class ComputePartitionTest {

    private final ComputePartition<SourceRecord> computePartitionTransformation = new ComputePartition<>();

    @Test
    public void correctComputeKafkaPartitionBasedOnConfiguredFieldOnCreateAndUpdateEvents() {

        configureTransformation("inventory.products:product", "inventory.products:2");

        final SourceRecord eventRecord = SmtTestFramework.buildProductsSourceRecord("oracle", "oracle", "inventory", "products", productRow(1L, 1.0F, "APPLE"), CREATE);

        SourceRecord transformed = computePartitionTransformation.apply(eventRecord);

        assertThat(transformed.kafkaPartition()).isZero();
    }

    @Test
    public void correctComputeKafkaPartitionBasedOnConfiguredFieldOnDeleteEvents() {

        configureTransformation("inventory.products:product", "inventory.products:2");

        final SourceRecord eventRecord = SmtTestFramework.buildProductsSourceRecord("oracle", "oracle", "inventory", "products", productRow(1L, 1.0F, "APPLE"), DELETE);

        SourceRecord transformed = computePartitionTransformation.apply(eventRecord);

        assertThat(transformed.kafkaPartition()).isZero();
    }

    @Test
    public void partitionNotComputedOnTruncateEvent() {

        configureTransformation("inventory.products:product", "inventory.products:2");

        final SourceRecord eventRecord = SmtTestFramework.buildProductsSourceRecord("oracle", "oracle", "inventory", "products", productRow(1L, 1.0F, "APPLE"), TRUNCATE);

        SourceRecord transformed = computePartitionTransformation.apply(eventRecord);

        assertThat(transformed.kafkaPartition()).isEqualTo(eventRecord.kafkaPartition());
    }

    @Test
    public void rowWithSameConfiguredFieldValueWillHaveTheSamePartition() {

        configureTransformation("inventory.products:product", "inventory.products:2");

        final SourceRecord eventRecord1 = SmtTestFramework.buildProductsSourceRecord("oracle", "oracle", "inventory", "products", productRow(1L, 1.0F, "APPLE"), CREATE);

        SourceRecord transformed1 = computePartitionTransformation.apply(eventRecord1);

        final SourceRecord eventRecord2 = SmtTestFramework.buildProductsSourceRecord("oracle", "oracle", "inventory", "products", productRow(2L, 2.0F, "APPLE"), CREATE);

        SourceRecord transformed2 = computePartitionTransformation.apply(eventRecord2);

        assertThat(transformed1.kafkaPartition()).isZero();
        assertThat(transformed2.kafkaPartition()).isZero();
    }

    @Test
    public void rowWithDifferentConfiguredFieldValueWillHaveDifferentPartition() {

        configureTransformation("inventory.products:product", "inventory.products:2");

        final SourceRecord eventRecord1 = SmtTestFramework.buildProductsSourceRecord("oracle", "oracle", "inventory", "products", productRow(1L, 1.0F, "APPLE"), CREATE);

        SourceRecord transformed1 = computePartitionTransformation.apply(eventRecord1);

        final SourceRecord eventRecord2 = SmtTestFramework.buildProductsSourceRecord("oracle", "oracle", "inventory", "products", productRow(3L, 0.95F, "BANANA"),
                CREATE);

        SourceRecord transformed2 = computePartitionTransformation.apply(eventRecord2);

        assertThat(transformed1.kafkaPartition()).isNotEqualTo(transformed2.kafkaPartition());
    }

    @Test
    public void notConsistentConfigurationSizeWillThrowConnectionException() {

        assertThatThrownBy(() -> configureTransformation("inventory.orders:purchaser,inventory.products:product", "purchaser:2"))
                .isInstanceOf(ComputePartitionException.class)
                .hasMessageContaining(
                        "Unable to validate config. partition.data-collections.partition.num.mappings and partition.data-collections.field.mappings has different number of table defined");
    }

    @Test
    public void notConsistentConfigurationWillThrowConnectionException() {

        assertThatThrownBy(
                () -> configureTransformation("inventory.orders:purchaser,inventory.products:product", "prod:2,purchaser:2"))
                .isInstanceOf(ComputePartitionException.class)
                .hasMessageContaining(
                        "Unable to validate config. partition.data-collections.partition.num.mappings and partition.data-collections.field.mappings has different tables defined");
    }

    @Test
    public void negativeHashCodeValueWillBeCorrectlyManaged() {

        configureTransformation("inventory.products:product", "inventory.products:3");

        final SourceRecord eventRecord1 = SmtTestFramework.buildProductsSourceRecord("oracle", "oracle", "inventory", "products", productRow(1L, 1.0F, "orange"), CREATE);

        SourceRecord transformed1 = computePartitionTransformation.apply(eventRecord1);

        assertThat(transformed1.kafkaPartition()).isEqualTo(1);
    }

    @Test
    public void zeroAsPartitionNumberWillThrowConnectionException() {
        assertThatThrownBy(
                () -> configureTransformation("inventory.orders:purchaser,inventory.products:products",
                        "inventory.products:0,inventory.orders:2"))
                .isInstanceOf(ComputePartitionException.class)
                .hasMessageContaining(
                        "Unable to validate config. partition.data-collections.partition.num.mappings: partition number for 'inventory.products' must be positive");
    }

    @Test
    public void negativeAsPartitionNumberWillThrowConnectionException() {
        assertThatThrownBy(
                () -> configureTransformation("inventory.orders:purchaser,inventory.products:products",
                        "inventory.products:-3,inventory.orders:2"))
                .isInstanceOf(ComputePartitionException.class)
                .hasMessageContaining(
                        "Unable to validate config. partition.data-collections.partition.num.mappings: partition number for 'inventory.products' must be positive");
    }

    private void configureTransformation(String tableFieldMapping, String tablePartitionNumMapping) {
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
