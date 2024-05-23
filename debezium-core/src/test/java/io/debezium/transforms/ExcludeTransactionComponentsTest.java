/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import static io.debezium.schema.SchemaFactory.TRANSACTION_BLOCK_SCHEMA_NAME;
import static io.debezium.schema.SchemaFactory.TRANSACTION_BLOCK_SCHEMA_VERSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import io.debezium.data.Envelope;
import io.debezium.pipeline.txmetadata.TransactionStructMaker;
import io.debezium.schema.SchemaFactory;
import io.debezium.schema.SchemaNameAdjuster;

public class ExcludeTransactionComponentsTest {

    @Test
    public void shouldExcludeTransactionTopicMessageWhenEnabled() {
        Schema keySchema = SchemaFactory.get().transactionKeySchema(SchemaNameAdjuster.NO_OP);
        Struct keyStruct = new Struct(keySchema);
        Schema valueSchema = SchemaFactory.get().transactionValueSchema(SchemaNameAdjuster.NO_OP);
        Struct valueStruct = new Struct(valueSchema);
        SourceRecord sourceRecord = new SourceRecord(
                null,
                null,
                "topic",
                0,
                keySchema,
                keyStruct,
                valueSchema,
                valueStruct,
                null);
        ExcludeTransactionComponents<SourceRecord> exclude = new ExcludeTransactionComponents();
        Map<String, ?> config = Map.of(ExcludeTransactionComponents.EXCLUDED_TRANSACTION_METADATA_COMPONENTS.name(), "transaction_topic");
        exclude.configure(config);
        SourceRecord resultRecord = exclude.apply(sourceRecord);
        assertThat(resultRecord).isNull();
    }

    @Test
    public void shouldExcludeTxIdWhenEnabled() {
        Schema recordSchema = SchemaBuilder.struct()
                .field("id", Schema.STRING_SCHEMA)
                .build();
        Schema transactionSchema = SchemaFactory.get().transactionBlockSchema();
        Schema sourceSchema = SchemaBuilder.struct()
                .field("db", Schema.STRING_SCHEMA).build();
        Envelope envelope = SchemaFactory.get().datatypeEnvelopeSchema()
                .withRecord(recordSchema)
                .withSource(sourceSchema)
                .withTransaction(transactionSchema)
                .build();
        Schema keySchema = SchemaBuilder.struct().field("key", SchemaBuilder.STRING_SCHEMA).build();
        Struct keyStruct = new Struct(keySchema).put("key", "k1");
        Schema valueSchema = envelope.schema();
        Struct valueStruct = new Struct(valueSchema)
                .put("before", new Struct(recordSchema).put("id", "foo"))
                .put("after", new Struct(recordSchema).put("id", "foo"))
                .put("op", "c")
                .put("source", new Struct(sourceSchema).put("db", "bar"))
                .put("transaction", new Struct(transactionSchema)
                        .put("id", "foo")
                        .put("data_collection_order", 1L)
                        .put("total_order", 2L));
        SourceRecord sourceRecord = new SourceRecord(
                null,
                null,
                "topic",
                0,
                keySchema,
                keyStruct,
                valueSchema,
                valueStruct,
                null);
        ExcludeTransactionComponents<SourceRecord> exclude = new ExcludeTransactionComponents();
        Map<String, ?> config = Map.of(ExcludeTransactionComponents.EXCLUDED_TRANSACTION_METADATA_COMPONENTS.name(), "id");
        exclude.configure(config);
        SourceRecord record = exclude.apply(sourceRecord);

        Schema expectedTransactionSchema = SchemaBuilder.struct().optional()
                .name(TRANSACTION_BLOCK_SCHEMA_NAME)
                .version(TRANSACTION_BLOCK_SCHEMA_VERSION)
                .field(TransactionStructMaker.DEBEZIUM_TRANSACTION_TOTAL_ORDER_KEY, SchemaBuilder.int64().build())
                .field(TransactionStructMaker.DEBEZIUM_TRANSACTION_DATA_COLLECTION_ORDER_KEY, SchemaBuilder.int64().build())
                .build();
        Envelope expectedEnvelope = SchemaFactory.get().datatypeEnvelopeSchema()
                .withRecord(recordSchema)
                .withSource(sourceSchema)
                .withTransaction(expectedTransactionSchema)
                .build();
        Schema expectedValueSchema = expectedEnvelope.schema();
        Struct expectedValueStruct = new Struct(expectedValueSchema)
                .put("before", new Struct(recordSchema).put("id", "foo"))
                .put("after", new Struct(recordSchema).put("id", "foo"))
                .put("op", "c")
                .put("source", new Struct(sourceSchema).put("db", "bar"))
                .put("transaction", new Struct(expectedTransactionSchema)
                        .put("data_collection_order", 1L)
                        .put("total_order", 2L));

        Struct actualStruct = (Struct) record.value();

        assertThat(actualStruct).isEqualTo(expectedValueStruct);
    }

    @Test
    public void shouldExcludeTotalOrderWhenEnabled() {
        Schema recordSchema = SchemaBuilder.struct()
                .field("id", Schema.STRING_SCHEMA)
                .build();
        Schema transactionSchema = SchemaFactory.get().transactionBlockSchema();
        Schema sourceSchema = SchemaBuilder.struct()
                .field("db", Schema.STRING_SCHEMA).build();
        Envelope envelope = SchemaFactory.get().datatypeEnvelopeSchema()
                .withRecord(recordSchema)
                .withSource(sourceSchema)
                .withTransaction(transactionSchema)
                .build();
        Schema keySchema = SchemaBuilder.struct().field("key", SchemaBuilder.STRING_SCHEMA).build();
        Struct keyStruct = new Struct(keySchema).put("key", "k1");
        Schema valueSchema = envelope.schema();
        Struct valueStruct = new Struct(valueSchema)
                .put("before", new Struct(recordSchema).put("id", "foo"))
                .put("after", new Struct(recordSchema).put("id", "foo"))
                .put("op", "c")
                .put("source", new Struct(sourceSchema).put("db", "bar"))
                .put("transaction", new Struct(transactionSchema)
                        .put("id", "foo")
                        .put("data_collection_order", 1L)
                        .put("total_order", 2L));
        SourceRecord sourceRecord = new SourceRecord(
                null,
                null,
                "topic",
                0,
                keySchema,
                keyStruct,
                valueSchema,
                valueStruct,
                null);
        ExcludeTransactionComponents<SourceRecord> exclude = new ExcludeTransactionComponents();
        Map<String, ?> config = Map.of(ExcludeTransactionComponents.EXCLUDED_TRANSACTION_METADATA_COMPONENTS.name(), "total_order");
        exclude.configure(config);
        SourceRecord record = exclude.apply(sourceRecord);

        Schema expectedTransactionSchema = SchemaBuilder.struct().optional()
                .name(TRANSACTION_BLOCK_SCHEMA_NAME)
                .version(TRANSACTION_BLOCK_SCHEMA_VERSION)
                .field(TransactionStructMaker.DEBEZIUM_TRANSACTION_ID_KEY, SchemaBuilder.string().build())
                .field(TransactionStructMaker.DEBEZIUM_TRANSACTION_DATA_COLLECTION_ORDER_KEY, SchemaBuilder.int64().build())
                .build();
        Envelope expectedEnvelope = SchemaFactory.get().datatypeEnvelopeSchema()
                .withRecord(recordSchema)
                .withSource(sourceSchema)
                .withTransaction(expectedTransactionSchema)
                .build();
        Schema expectedValueSchema = expectedEnvelope.schema();
        Struct expectedValueStruct = new Struct(expectedValueSchema)
                .put("before", new Struct(recordSchema).put("id", "foo"))
                .put("after", new Struct(recordSchema).put("id", "foo"))
                .put("op", "c")
                .put("source", new Struct(sourceSchema).put("db", "bar"))
                .put("transaction", new Struct(expectedTransactionSchema)
                        .put("id", "foo")
                        .put("data_collection_order", 1L));

        Struct actualStruct = (Struct) record.value();

        assertThat(actualStruct).isEqualTo(expectedValueStruct);
    }

    @Test
    public void shouldExcludeDataCollectionOrderWhenEnabled() {
        Schema recordSchema = SchemaBuilder.struct()
                .field("id", Schema.STRING_SCHEMA)
                .build();
        Schema transactionSchema = SchemaFactory.get().transactionBlockSchema();
        Schema sourceSchema = SchemaBuilder.struct()
                .field("db", Schema.STRING_SCHEMA).build();
        Envelope envelope = SchemaFactory.get().datatypeEnvelopeSchema()
                .withRecord(recordSchema)
                .withSource(sourceSchema)
                .withTransaction(transactionSchema)
                .build();
        Schema keySchema = SchemaBuilder.struct().field("key", SchemaBuilder.STRING_SCHEMA).build();
        Struct keyStruct = new Struct(keySchema).put("key", "k1");
        Schema valueSchema = envelope.schema();
        Struct valueStruct = new Struct(valueSchema)
                .put("before", new Struct(recordSchema).put("id", "foo"))
                .put("after", new Struct(recordSchema).put("id", "foo"))
                .put("op", "c")
                .put("source", new Struct(sourceSchema).put("db", "bar"))
                .put("transaction", new Struct(transactionSchema)
                        .put("id", "foo")
                        .put("data_collection_order", 1L)
                        .put("total_order", 2L));
        SourceRecord sourceRecord = new SourceRecord(
                null,
                null,
                "topic",
                0,
                keySchema,
                keyStruct,
                valueSchema,
                valueStruct,
                null);
        ExcludeTransactionComponents<SourceRecord> exclude = new ExcludeTransactionComponents();
        Map<String, ?> config = Map.of(ExcludeTransactionComponents.EXCLUDED_TRANSACTION_METADATA_COMPONENTS.name(), "data_collection_order");
        exclude.configure(config);
        SourceRecord record = exclude.apply(sourceRecord);

        Schema expectedTransactionSchema = SchemaBuilder.struct().optional()
                .name(TRANSACTION_BLOCK_SCHEMA_NAME)
                .version(TRANSACTION_BLOCK_SCHEMA_VERSION)
                .field(TransactionStructMaker.DEBEZIUM_TRANSACTION_ID_KEY, SchemaBuilder.string().build())
                .field(TransactionStructMaker.DEBEZIUM_TRANSACTION_TOTAL_ORDER_KEY, SchemaBuilder.int64().build())
                .build();
        Envelope expectedEnvelope = SchemaFactory.get().datatypeEnvelopeSchema()
                .withRecord(recordSchema)
                .withSource(sourceSchema)
                .withTransaction(expectedTransactionSchema)
                .build();
        Schema expectedValueSchema = expectedEnvelope.schema();
        Struct expectedValueStruct = new Struct(expectedValueSchema)
                .put("before", new Struct(recordSchema).put("id", "foo"))
                .put("after", new Struct(recordSchema).put("id", "foo"))
                .put("op", "c")
                .put("source", new Struct(sourceSchema).put("db", "bar"))
                .put("transaction", new Struct(expectedTransactionSchema)
                        .put("id", "foo")
                        .put("total_order", 2L));

        Struct actualStruct = (Struct) record.value();

        assertThat(actualStruct).isEqualTo(expectedValueStruct);
    }

    @Test
    public void shouldValidateConfig() {
        ExcludeTransactionComponents<SourceRecord> exclude = new ExcludeTransactionComponents();
        Map<String, ?> config = Map.of(ExcludeTransactionComponents.EXCLUDED_TRANSACTION_METADATA_COMPONENTS.name(), "invalid_option");
        assertThatThrownBy(() -> {
            exclude.configure(config);
        }).isInstanceOf(ConfigException.class);
    }

}
