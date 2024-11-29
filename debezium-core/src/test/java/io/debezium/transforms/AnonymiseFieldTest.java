/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.transforms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.util.Requirements;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.data.VerifyRecord;

/**
 * @author Ismail Simsek
 */
public class AnonymiseFieldTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(AnonymiseFieldTest.class);

    public static final Schema RECORD_SCHEMA = SchemaBuilder.struct().optional()
            .field("id", Schema.INT64_SCHEMA)
            .field("price", SchemaBuilder.float32().doc("price-field-doc").defaultValue(1234.5f).build())
            .field("product", Schema.STRING_SCHEMA)
            .field("int64_field", SchemaBuilder.int64().optional().build())
            .field("float32_field", SchemaBuilder.float32().optional().build())
            .field("bool_field", SchemaBuilder.bool().optional().build())
            .build();

    protected final Schema SOURCE_SCHEMA = SchemaBuilder.struct().optional()
            .field("table", Schema.STRING_SCHEMA)
            .field("lsn", Schema.INT32_SCHEMA)
            .field("ts_ms", Schema.OPTIONAL_INT32_SCHEMA)
            .build();

    private final AnonymiseField.Value<SourceRecord> anonymiser = new AnonymiseField.Value<>();

    @Test
    public void shouldCopySchemaAndMD5HashValue() throws NoSuchAlgorithmException {
        anonymiser.configure(Map.of("fields", "id,price,product"));
        final Struct before = new Struct(RECORD_SCHEMA);
        before
                .put("id", 101L)
                .put("price", 20.0F)
                .put("product", "a product");
        SourceRecord sourceRecord = new SourceRecord(new HashMap<>(), new HashMap<>(), "topic", RECORD_SCHEMA.schema(), before);
        VerifyRecord.isValid(sourceRecord);
        SourceRecord transformedRecord = anonymiser.apply(sourceRecord);
        VerifyRecord.isValid(transformedRecord);

        Struct payloadStruct = Requirements.requireStruct(transformedRecord.value(), "");
        assertThat(payloadStruct.get("product")).isEqualTo(AnonymiseField.md5("a product"));
        assertThat(payloadStruct.get("id")).isEqualTo(AnonymiseField.md5("101"));
        assertThat(payloadStruct.get("price")).isEqualTo(AnonymiseField.md5("20.0"));
        assertThat(transformedRecord.valueSchema().field("product").schema().type()).isEqualTo(Schema.Type.STRING);
        assertThat(transformedRecord.valueSchema().field("id").schema().type()).isEqualTo(Schema.Type.STRING);
        assertThat(transformedRecord.valueSchema().field("price").schema().type()).isEqualTo(Schema.Type.STRING);
    }

    @Test
    public void shouldCopySchemaAndSHA256HashValue() throws NoSuchAlgorithmException {
        anonymiser.configure(Map.of("fields", "id,price,product", "hashing", "SHA256"));
        final Struct before = new Struct(RECORD_SCHEMA);
        before
                .put("id", 101L)
                .put("price", 20.0F)
                .put("product", "a product");
        SourceRecord sourceRecord = new SourceRecord(new HashMap<>(), new HashMap<>(), "topic", RECORD_SCHEMA.schema(), before);
        VerifyRecord.isValid(sourceRecord);
        SourceRecord transformedRecord = anonymiser.apply(sourceRecord);
        VerifyRecord.isValid(transformedRecord);

        Struct payloadStruct = Requirements.requireStruct(transformedRecord.value(), "");
        assertThat(payloadStruct.get("product")).isEqualTo(AnonymiseField.sha256("a product"));
        assertThat(payloadStruct.get("id")).isEqualTo(AnonymiseField.sha256("101"));
        assertThat(payloadStruct.get("price")).isEqualTo(AnonymiseField.sha256("20.0"));
        assertThat(transformedRecord.valueSchema().field("product").schema().type()).isEqualTo(Schema.Type.STRING);
        assertThat(transformedRecord.valueSchema().field("id").schema().type()).isEqualTo(Schema.Type.STRING);
        assertThat(transformedRecord.valueSchema().field("price").schema().type()).isEqualTo(Schema.Type.STRING);
    }

    @Test
    public void shouldAnonymiseSchemalessFields() {
        anonymiser.configure(Map.of("fields", "filed01,filed_string", "hashing", "SHA256"));
        SourceRecord record = new SourceRecord(null, null, "test", 0,
                null, null, null, Collections.singletonMap("filed01", 42L), 123L);
        SourceRecord transformedRecord = anonymiser.apply(record);
        assertEquals("test", transformedRecord.topic());
        assertEquals("73475cb40a568e8da8a045ced110137e159f890ac4da883b6b17dc651b3a8049", ((Map<?, ?>) transformedRecord.value()).get("filed01"));

        // test lowercase uppercase values should result same hash
        record = new SourceRecord(null, null, "test", 0,
                null, null, null, Collections.singletonMap("filed_string", "lowercase-value"), 123L);
        transformedRecord = anonymiser.apply(record);
        assertEquals("ce33c03beacb5b94f3d877e350fd128a5fd2c04b0a3b1e18466c36bd1737dcc0", ((Map<?, ?>) transformedRecord.value()).get("filed_string"));
        record = new SourceRecord(null, null, "test", 0,
                null, null, null, Collections.singletonMap("filed_string", "LOWERCASE-VALUE"), 123L);
        transformedRecord = anonymiser.apply(record);
        assertEquals("ce33c03beacb5b94f3d877e350fd128a5fd2c04b0a3b1e18466c36bd1737dcc0", ((Map<?, ?>) transformedRecord.value()).get("filed_string"));
    }

    @Test
    public void shouldChangeFieldSchemaToString() throws NoSuchAlgorithmException {
        anonymiser.configure(Map.of("fields", "id,price,product"));
        Schema transformedSchema = anonymiser.makeUpdatedSchema(RECORD_SCHEMA);
        assertThat(transformedSchema.field("id").schema().type()).isEqualTo(Schema.Type.STRING);
        assertThat(transformedSchema.field("price").schema().type()).isEqualTo(Schema.Type.STRING);
        assertThat(transformedSchema.field("price").schema().doc()).isEqualTo("price-field-doc");
        // default value should be anonymised
        assertThat(transformedSchema.field("price").schema().defaultValue()).isEqualTo(AnonymiseField.md5("1234.5"));
        assertThat(transformedSchema.field("product").schema().type()).isEqualTo(Schema.Type.STRING);
        // should stay same
        assertThat(transformedSchema.field("int64_field").schema().type()).isEqualTo(Schema.Type.INT64);
        assertThat(transformedSchema.field("float32_field").schema().type()).isEqualTo(Schema.Type.FLOAT32);
        assertThat(transformedSchema.field("bool_field").schema().type()).isEqualTo(Schema.Type.BOOLEAN);
    }

}
