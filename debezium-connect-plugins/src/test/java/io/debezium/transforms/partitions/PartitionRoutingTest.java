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
import org.junit.jupiter.api.Test;

import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;

public class PartitionRoutingTest {

    public static final Schema VALUE_SCHEMA = SchemaBuilder.struct()
            .name("server1.inventory.products.Value")
            .field("id", Schema.INT64_SCHEMA)
            .field("price", Schema.FLOAT32_SCHEMA)
            .field("product", Schema.OPTIONAL_STRING_SCHEMA)
            .build();

    private static final Schema KEY_SCHEMA = SchemaBuilder.struct()
            .name("server1.inventory.products.Key")
            .field("tenant_id", Schema.INT64_SCHEMA)
            .field("id", Schema.INT64_SCHEMA)
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
    @FixFor("DBZ-6543")
    public void whenAnSpecifiedOptionalFieldIsNotFoundOnPayloadItWillBeIgnored() {

        partitionRoutingTransformation.configure(Map.of(
                "partition.payload.fields", "change.product",
                "partition.topic.num", 2));

        final SourceRecord eventRecord = buildSourceRecord(productRow(Map.of("id", 1L, "price", 1.0F)), CREATE);

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

    @Test
    public void byDefaultJavaHashIsUsed() {

        partitionRoutingTransformation.configure(Map.of(
                "partition.payload.fields", "change.id, change.product",
                "partition.topic.num", 100));

        final SourceRecord eventRecord = buildSourceRecord(productRow(1L, 1.0F, "orange"), CREATE);

        SourceRecord transformed = partitionRoutingTransformation.apply(eventRecord);

        assertThat(transformed.kafkaPartition()).isEqualTo(39);
    }

    @Test
    public void murmurHashWillBeUsed() {

        partitionRoutingTransformation.configure(Map.of(
                "partition.payload.fields", "change.id, change.product",
                "partition.topic.num", 100,
                "partition.hash.function", "murmur"));

        final SourceRecord eventRecord = buildSourceRecord(productRow(1L, 1.0F, "orange"), CREATE);

        SourceRecord transformed = partitionRoutingTransformation.apply(eventRecord);

        assertThat(transformed.kafkaPartition()).isEqualTo(65);
    }

    @Test
    @FixFor("debezium/dbz#2698")
    public void whenNestedFieldPathTraversesNullStructItWillBeIgnored() {

        partitionRoutingTransformation.configure(Map.of(
                "partition.payload.fields", "before.product",
                "partition.topic.num", 2));

        final SourceRecord eventRecord = buildSourceRecord(productRow(1L, 1.0F, "APPLE"), CREATE);

        SourceRecord transformed = partitionRoutingTransformation.apply(eventRecord);

        assertThat(eventRecord).isEqualTo(transformed);
    }

    @Test
    @FixFor("debezium/dbz#2698")
    public void whenNestedFieldPathTraversesNullStructWithOtherValidFieldItWillBeRouted() {

        partitionRoutingTransformation.configure(Map.of(
                "partition.payload.fields", "before.product,after.product",
                "partition.topic.num", 2));

        final SourceRecord eventRecord = buildSourceRecord(productRow(1L, 1.0F, "APPLE"), CREATE);

        SourceRecord transformed = partitionRoutingTransformation.apply(eventRecord);

        assertThat(transformed.kafkaPartition()).isZero();
    }

    @Test
    @FixFor("debezium/dbz#990")
    public void keyFieldRoutesCreateDeleteAndTombstoneToSamePartition() {

        partitionRoutingTransformation.configure(Map.of(
                "partition.payload.fields", "id",
                "partition.payload.fields.from.key", true,
                "partition.topic.num", 3));

        final SourceRecord create = buildSourceRecord(productKey(7L, 1L), productRow(1L, 1.0F, "APPLE"), CREATE);
        final SourceRecord delete = buildSourceRecord(productKey(7L, 1L), productRow(1L, 1.0F, "APPLE"), DELETE);

        final Integer partition = partitionRoutingTransformation.apply(create).kafkaPartition();

        assertThat(partition).isNotNull();
        assertThat(partitionRoutingTransformation.apply(delete).kafkaPartition()).isEqualTo(partition);
        assertThat(partitionRoutingTransformation.apply(tombstoneOf(delete)).kafkaPartition()).isEqualTo(partition);
    }

    @Test
    @FixFor("debezium/dbz#990")
    public void keyFieldOnPartOfCompositeKeyRoutesAllRowsOfTenantTogether() {

        partitionRoutingTransformation.configure(Map.of(
                "partition.payload.fields", "tenant_id",
                "partition.payload.fields.from.key", true,
                "partition.topic.num", 3));

        final SourceRecord delete1 = buildSourceRecord(productKey(7L, 1L), productRow(1L, 1.0F, "APPLE"), DELETE);
        final SourceRecord delete2 = buildSourceRecord(productKey(7L, 2L), productRow(2L, 1.0F, "ORANGE"), DELETE);

        final Integer partition = partitionRoutingTransformation.apply(delete1).kafkaPartition();

        assertThat(partition).isNotNull();
        assertThat(partitionRoutingTransformation.apply(tombstoneOf(delete1)).kafkaPartition()).isEqualTo(partition);
        assertThat(partitionRoutingTransformation.apply(delete2).kafkaPartition()).isEqualTo(partition);
        assertThat(partitionRoutingTransformation.apply(tombstoneOf(delete2)).kafkaPartition()).isEqualTo(partition);
    }

    @Test
    @FixFor("debezium/dbz#990")
    public void deleteIsRoutedButItsTombstoneIsPassedThroughWhenRoutingByValueFields() {

        partitionRoutingTransformation.configure(Map.of(
                "partition.payload.fields", "change.product",
                "partition.topic.num", 3));

        final SourceRecord delete = buildSourceRecord(productKey(7L, 1L), productRow(1L, 1.0F, "APPLE"), DELETE);
        final SourceRecord tombstone = tombstoneOf(delete);

        assertThat(partitionRoutingTransformation.apply(delete).kafkaPartition()).isNotNull();
        assertThat(partitionRoutingTransformation.apply(tombstone)).isEqualTo(tombstone);
    }

    private static Struct productKey(long tenantId, long id) {
        return new Struct(KEY_SCHEMA).put("tenant_id", tenantId).put("id", id);
    }

    private static SourceRecord tombstoneOf(SourceRecord delete) {
        return delete.newRecord(delete.topic(), delete.kafkaPartition(), delete.keySchema(), delete.key(), null, null, delete.timestamp());
    }

    private SourceRecord buildSourceRecord(Struct row, Envelope.Operation operation) {
        return buildSourceRecord(null, row, operation);
    }

    private SourceRecord buildSourceRecord(Struct key, Struct row, Envelope.Operation operation) {

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
                null,
                key == null ? null : key.schema(), key,
                createEnvelope.schema(), payload);
    }

    private Struct productRow(long id, float price, String name) {

        return new Struct(VALUE_SCHEMA)
                .put("id", id)
                .put("price", price)
                .put("product", name);
    }

    private Struct productRow(Map<String, Object> rowValues) {

        Struct struct = new Struct(VALUE_SCHEMA);
        rowValues.forEach(struct::put);

        return struct;
    }
}
