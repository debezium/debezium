/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.data;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;

import io.debezium.pipeline.txmetadata.TransactionMonitor;

/**
 * @author Randall Hauch
 *
 */
public class EnvelopeTest {

    @Test
    public void shouldBuildWithSimpleOptionalTypesForBeforeAndAfter() {
        Envelope env = Envelope.defineSchema()
                .withName("someName")
                .withRecord(Schema.OPTIONAL_STRING_SCHEMA)
                .withSource(Schema.OPTIONAL_INT64_SCHEMA)
                .build();
        assertThat(env.schema()).isNotNull();
        assertThat(env.schema().name()).isEqualTo("someName");
        assertThat(env.schema().doc()).isNull();
        assertThat(env.schema().version()).isEqualTo(2);
        assertOptionalField(env, Envelope.FieldName.AFTER, Schema.OPTIONAL_STRING_SCHEMA);
        assertOptionalField(env, Envelope.FieldName.BEFORE, Schema.OPTIONAL_STRING_SCHEMA);
        assertOptionalField(env, Envelope.FieldName.SOURCE, Schema.OPTIONAL_INT64_SCHEMA);
        assertRequiredField(env, Envelope.FieldName.OPERATION, Schema.STRING_SCHEMA);
        assertOptionalField(env, Envelope.FieldName.TRANSACTION, TransactionMonitor.TRANSACTION_BLOCK_SCHEMA);
    }

    @Test
    public void shouldBuildWithCustomTransactionSchema() {
        Envelope env = Envelope.defineSchema()
                .withName("someName")
                .withRecord(Schema.OPTIONAL_STRING_SCHEMA)
                .withSource(Schema.OPTIONAL_INT64_SCHEMA)
                .withTransaction(SchemaBuilder.STRING_SCHEMA)
                .build();
        assertThat(env.schema()).isNotNull();
        assertThat(env.schema().name()).isEqualTo("someName");
        assertThat(env.schema().doc()).isNull();
        assertThat(env.schema().version()).isEqualTo(2);
        assertOptionalField(env, Envelope.FieldName.AFTER, Schema.OPTIONAL_STRING_SCHEMA);
        assertOptionalField(env, Envelope.FieldName.BEFORE, Schema.OPTIONAL_STRING_SCHEMA);
        assertOptionalField(env, Envelope.FieldName.SOURCE, Schema.OPTIONAL_INT64_SCHEMA);
        assertRequiredField(env, Envelope.FieldName.OPERATION, Schema.STRING_SCHEMA);
        assertRequiredField(env, Envelope.FieldName.TRANSACTION, SchemaBuilder.STRING_SCHEMA);
    }

    @Test
    public void shouldUseDefaultEnvelopeSchemaFactory() {
        // Verify that the new Envelope.defineSchema(factory) overload with the DefaultEnvelopeSchemaFactory
        // produces a schema identical to the existing no-args Envelope.defineSchema() path.
        EnvelopeSchemaFactory defaultFactory = new DefaultEnvelopeSchemaFactory();

        Envelope envViaFactory = Envelope.defineSchema(defaultFactory)
                .withName("factoryTest")
                .withRecord(Schema.OPTIONAL_STRING_SCHEMA)
                .withSource(Schema.OPTIONAL_INT64_SCHEMA)
                .build();

        Envelope envDefault = Envelope.defineSchema()
                .withName("factoryTest")
                .withRecord(Schema.OPTIONAL_STRING_SCHEMA)
                .withSource(Schema.OPTIONAL_INT64_SCHEMA)
                .build();

        // Both envelopes must have exactly the same schema structure.
        assertThat(envViaFactory.schema().fields().size()).isEqualTo(envDefault.schema().fields().size());
        assertThat(envViaFactory.schema().version()).isEqualTo(Envelope.SCHEMA_VERSION);
        assertOptionalField(envViaFactory, Envelope.FieldName.BEFORE, Schema.OPTIONAL_STRING_SCHEMA);
        assertOptionalField(envViaFactory, Envelope.FieldName.AFTER, Schema.OPTIONAL_STRING_SCHEMA);
        assertOptionalField(envViaFactory, Envelope.FieldName.SOURCE, Schema.OPTIONAL_INT64_SCHEMA);
        assertRequiredField(envViaFactory, Envelope.FieldName.OPERATION, Schema.STRING_SCHEMA);
    }

    @Test
    public void shouldUseCustomEnvelopeSchemaFactory() {
        // Verify that a custom EnvelopeSchemaFactory is actually called, and its builder is used.
        // The custom factory injects a doc string into the envelope schema.
        final String customDoc = "Custom envelope for Iceberg sink";

        EnvelopeSchemaFactory customFactory = () -> Envelope.defineSchema().withDoc(customDoc);

        Envelope env = Envelope.defineSchema(customFactory)
                .withName("customFactoryTest")
                .withRecord(Schema.OPTIONAL_STRING_SCHEMA)
                .withSource(Schema.OPTIONAL_INT64_SCHEMA)
                .build();

        // The custom doc should be present on the built schema.
        assertThat(env.schema().doc()).isEqualTo(customDoc);
        assertThat(env.schema().name()).isEqualTo("customFactoryTest");
        assertThat(env.schema().version()).isEqualTo(Envelope.SCHEMA_VERSION);
        assertOptionalField(env, Envelope.FieldName.BEFORE, Schema.OPTIONAL_STRING_SCHEMA);
        assertRequiredField(env, Envelope.FieldName.OPERATION, Schema.STRING_SCHEMA);
    }

    @Test
    public void envelopeOperationLookupByCode() {
        assertThat(Envelope.Operation.forCode(null)).isNull();
        assertThat(Envelope.Operation.forCode("")).isNull();
        assertThat(Envelope.Operation.forCode("bogus")).isNull();
        for (Envelope.Operation operation : Envelope.Operation.values()) {
            assertThat(Envelope.Operation.forCode(operation.code().toLowerCase())).isEqualTo(operation);
            assertThat(Envelope.Operation.forCode(operation.code().toUpperCase())).isEqualTo(operation);
            assertThat(Envelope.Operation.forCode(operation.name())).isNull();
        }
    }

    protected void assertRequiredField(Envelope env, String fieldName, Schema expectedSchema) {
        assertField(env.schema().field(fieldName), fieldName, expectedSchema, false);
    }

    protected void assertOptionalField(Envelope env, String fieldName, Schema expectedSchema) {
        assertField(env.schema().field(fieldName), fieldName, expectedSchema, true);
    }

    protected void assertField(Field field, String fieldName, Schema expectedSchema, boolean optional) {
        assertThat(field.name()).isEqualTo(fieldName);
        Schema schema = field.schema();
        assertThat(schema.name()).isEqualTo(expectedSchema.name());
        assertThat(schema.doc()).isEqualTo(expectedSchema.doc());
        assertThat(schema.parameters()).isEqualTo(expectedSchema.parameters());
        assertThat(schema.version()).isEqualTo(expectedSchema.version());
        assertThat(schema.isOptional()).isEqualTo(optional);
        switch (expectedSchema.type()) {
            case STRUCT:
                for (Field f : expectedSchema.fields()) {
                    assertField(schema.field(f.name()), f.name(), f.schema(), f.schema().isOptional());
                }
                break;
            default:
        }
    }

}
