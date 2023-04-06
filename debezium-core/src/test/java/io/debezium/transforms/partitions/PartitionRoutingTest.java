/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.partitions;

import static io.debezium.data.Envelope.Operation.CREATE;
import static io.debezium.data.Envelope.Operation.DELETE;
import static io.debezium.data.Envelope.Operation.TRUNCATE;
import static io.debezium.data.Envelope.Operation.UPDATE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import io.debezium.data.Envelope;

public class PartitionRoutingTest {

    public static final Schema VALUE_SCHEMA = SchemaBuilder.struct()
            .name("server1.inventory.products.Value")
            .field("id", Schema.INT64_SCHEMA)
            .field("price", Schema.FLOAT32_SCHEMA)
            .field("product", Schema.STRING_SCHEMA)
            .build();

    private final PartitionRouting<SourceRecord> partitionRoutingTransformation = new PartitionRouting<>();

    @Test
    public void whenNoPartitionPayloadFieldDeclaredAConfigExceptionIsThrew() {

        assertThatThrownBy(() -> partitionRoutingTransformation.configure(Map.of(
                "partition.topic.num", 2)))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(
                        "Invalid value null for configuration partition.payload.fields: The 'partition.payload.fields' value is invalid: A value is required");
    }

    @Test
    public void whenNoPartitionTopicNumFieldDeclaredAConfigExceptionIsThrew() {

        assertThatThrownBy(() -> partitionRoutingTransformation.configure(Map.of(
                "partition.payload.fields", 2)))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(
                        "Invalid value null for configuration partition.topic.num: The 'partition.topic.num' value is invalid: A value is required");
    }

    @Test
    public void whenPartitionPayloadFieldContainsEmptyElementAConfigExceptionIsThrew() {

        assertThatThrownBy(() -> partitionRoutingTransformation.configure(Map.of(
                "partition.payload.fields", ",source.table",
                "partition.topic.num", 2)))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(
                        "Invalid value ,source.table for configuration partition.payload.fields: The 'partition.payload.fields' value is invalid: Empty string element(s) not permitted");
    }

    @Test
    public void spaceBetweenNestedFiledSeparatedWillBeCorrectManaged() {

        partitionRoutingTransformation.configure(Map.of(
                "partition.payload.fields", "change . product",
                "partition.topic.num", 2));

        final SourceRecord eventRecord = buildSourceRecord(productRow(1L, 1.0F, "APPLE"), CREATE);

        SourceRecord transformed = partitionRoutingTransformation.apply(eventRecord);

        assertThat(transformed.kafkaPartition()).isZero();
    }

    @Test
    public void correctComputeKafkaPartitionBasedOnNewConfiguredFieldOnCreateAndUpdateEvents() {

        partitionRoutingTransformation.configure(Map.of(
                "partition.payload.fields", "after.product",
                "partition.topic.num", 2));

        final SourceRecord eventRecord = buildSourceRecord(productRow(1L, 1.0F, "APPLE"), CREATE);

        SourceRecord transformed = partitionRoutingTransformation.apply(eventRecord);

        assertThat(transformed.kafkaPartition()).isZero();
    }

    @Test
    public void correctComputeKafkaPartitionBasedOnSpecialChangeNestedFieldOnCreateEvent() {

        partitionRoutingTransformation.configure(Map.of(
                "partition.payload.fields", "change.product",
                "partition.topic.num", 2));

        final SourceRecord eventRecord = buildSourceRecord(productRow(1L, 1.0F, "APPLE"), CREATE);

        SourceRecord transformed = partitionRoutingTransformation.apply(eventRecord);

        assertThat(transformed.kafkaPartition()).isZero();
    }

    @Test
    public void whenASpecifiedFieldIsNotFoundOnPayloadItWillBeIgnored() {

        partitionRoutingTransformation.configure(Map.of(
                "partition.payload.fields", "after.not_existing",
                "partition.topic.num", 2));

        final SourceRecord eventRecord = buildSourceRecord(productRow(1L, 1.0F, "APPLE"), CREATE);

        SourceRecord transformed = partitionRoutingTransformation.apply(eventRecord);

        assertThat(eventRecord).isEqualTo(transformed);
    }

    @Test
    public void onlyFieldThatExistForCurrentEventWillBeUsedForPartitionComputation() {

        partitionRoutingTransformation.configure(Map.of(
                "partition.payload.fields", "after.not_existing,change.product",
                "partition.topic.num", 3));

        final SourceRecord eventRecord = buildSourceRecord(productRow(1L, 1.0F, "orange"), CREATE);

        SourceRecord transformed = partitionRoutingTransformation.apply(eventRecord);

        assertThat(transformed.kafkaPartition()).isOne();
    }

    @Test
    public void correctComputeKafkaPartitionBasedOnSpecialChangeNestedFieldOnCreateDelete() {

        partitionRoutingTransformation.configure(Map.of(
                "partition.payload.fields", "change.product",
                "partition.topic.num", 2));

        final SourceRecord eventRecord = buildSourceRecord(productRow(1L, 1.0F, "APPLE"), DELETE);

        SourceRecord transformed = partitionRoutingTransformation.apply(eventRecord);

        assertThat(transformed.kafkaPartition()).isZero();
    }

    @Test
    public void truncateOperationRecordWillBeSkipped() {

        partitionRoutingTransformation.configure(Map.of(
                "partition.payload.fields", "change.product",
                "partition.topic.num", 2));

        final SourceRecord eventRecord = buildSourceRecord(productRow(1L, 1.0F, "APPLE"), TRUNCATE);

        SourceRecord transformed = partitionRoutingTransformation.apply(eventRecord);

        assertThat(eventRecord).isEqualTo(transformed);
    }

    @Test
    public void correctComputeKafkaPartitionBasedOnNotNestedField() {

        partitionRoutingTransformation.configure(Map.of(
                "partition.payload.fields", "op",
                "partition.topic.num", 2));

        final SourceRecord createRecord1 = buildSourceRecord(productRow(1L, 1.0F, "APPLE"), CREATE);
        final SourceRecord createRecord2 = buildSourceRecord(productRow(1L, 1.0F, "ORANGE"), CREATE);
        final SourceRecord updateRecord = buildSourceRecord(productRow(1L, 1.0F, "ORANGE"), UPDATE);

        SourceRecord transformedCreateRecord1 = partitionRoutingTransformation.apply(createRecord1);
        SourceRecord transformedCreateRecord2 = partitionRoutingTransformation.apply(createRecord2);
        SourceRecord transformedUpdateRecord = partitionRoutingTransformation.apply(updateRecord);

        assertThat(transformedCreateRecord1.kafkaPartition()).isEqualTo(transformedCreateRecord2.kafkaPartition());
        assertThat(transformedUpdateRecord).isNotEqualTo(updateRecord);
    }

    private SourceRecord buildSourceRecord(Struct row, Envelope.Operation operation) {

        SchemaBuilder sourceSchemaBuilder = SchemaBuilder.struct()
                .name("source")
                .field("connector", Schema.STRING_SCHEMA)
                .field("db", Schema.STRING_SCHEMA)
                .field("table", Schema.STRING_SCHEMA);

        Schema sourceSchema = sourceSchemaBuilder.build();

        Envelope createEnvelope = Envelope.defineSchema()
                .withName("server1.inventory.product.Envelope")
                .withRecord(VALUE_SCHEMA)
                .withSource(sourceSchema)
                .build();

        Struct source = new Struct(sourceSchema);
        source.put("connector", "mysql");
        source.put("db", "inventory");
        source.put("table", "products");

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

    private Struct productRow(long id, float price, String name) {
        return new Struct(VALUE_SCHEMA)
                .put("id", id)
                .put("price", price)
                .put("product", name);
    }
}
