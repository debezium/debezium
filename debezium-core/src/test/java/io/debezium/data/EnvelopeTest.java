/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.data;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.junit.Test;

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
        assertThat(env.schema().version()).isEqualTo(1);
        assertOptionalField(env, Envelope.FieldName.AFTER, Schema.OPTIONAL_STRING_SCHEMA);
        assertOptionalField(env, Envelope.FieldName.BEFORE, Schema.OPTIONAL_STRING_SCHEMA);
        assertOptionalField(env, Envelope.FieldName.SOURCE, Schema.OPTIONAL_INT64_SCHEMA);
        assertRequiredField(env, Envelope.FieldName.OPERATION, Schema.STRING_SCHEMA);
        assertOptionalField(env, Envelope.FieldName.TRANSACTION, TransactionMonitor.TRANSACTION_BLOCK_SCHEMA);
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
