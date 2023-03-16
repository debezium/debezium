/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.transforms;

import io.debezium.data.Envelope;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.util.Requirements;
import org.junit.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class HeaderToValueTest {

    public static final Schema VALUE_SCHEMA = SchemaBuilder.struct()
            .name("mysql-server-1.inventory.products.Value")
            .field("id", Schema.INT64_SCHEMA)
            .field("price", Schema.FLOAT32_SCHEMA)
            .field("product", Schema.STRING_SCHEMA)
            .build();
    private final HeaderToValue<SourceRecord> headerToValue = new HeaderToValue<>();

    @Test
    public void whenOperationIsNotMoveOrCopyAConfigExceptionIsThrew() {

        assertThatThrownBy(() ->
        headerToValue.configure(Map.of(
                "headers", "h1",
                "fields", "f1",
                "operation", "invalidOp")))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(
                        "Invalid value invalidOp for configuration operation: The 'operation' value is invalid: Value must be one of move, copy");;

    }

    @Test
    public void whenNoFieldsDeclaredAConfigExceptionIsThrew() {

        assertThatThrownBy(() ->
                headerToValue.configure(Map.of(
                        "headers", "h1",
                        "operation", "copy")))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(
                        "Invalid value null for configuration fields: The 'fields' value is invalid: A value is required");;

    }

    @Test
    public void whenNoHeadersDeclaredAConfigExceptionIsThrew() {

        assertThatThrownBy(() ->
                headerToValue.configure(Map.of(
                        "fields", "f1",
                        "operation", "copy")))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(
                        "Invalid value null for configuration headers: The 'headers' value is invalid: A value is required");;

    }

    @Test
    public void whenHeadersAndFieldsHaveDifferentSizeAConfigExceptionIsThrew() {

        assertThatThrownBy(() ->
                headerToValue.configure(Map.of(
                        "headers", "h1,h2",
                        "fields", "f1",
                        "operation", "copy")))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(
                        "'fields' config must have the same number of elements as 'headers' config.");;

    }

    @Test
    public void whenARecordThatContainsADefinedHeaderItWillBeCopiedInTheDefinedField() {

        headerToValue.configure(Map.of(
                "headers", "h1,h2",
                "fields", "f1, f2",
                "operation", "copy"));

        Struct row = new Struct(VALUE_SCHEMA)
                .put("id", 101L)
                .put("price", 20.0F)
                .put("product", "a product");

        Envelope createEnvelope = Envelope.defineSchema()
                .withName("mysql-server-1.inventory.product.Envelope")
                .withRecord(VALUE_SCHEMA)
                .withSource(Schema.STRING_SCHEMA)
                .build();

        Struct payload = createEnvelope.create(row, null , Instant.now());
        SourceRecord sourceRecord = new SourceRecord(new HashMap<>(), new HashMap<>(), "topic", createEnvelope.schema(), payload);
        sourceRecord.headers().add("h1", "this is a value from h1 header", Schema.STRING_SCHEMA);
        sourceRecord.headers().add("h2", "this is a value from h2 header", Schema.STRING_SCHEMA);

        SourceRecord transformedRecord = headerToValue.apply(sourceRecord);

        Struct payloadStruct = Requirements.requireStruct(transformedRecord.value(), "");
        assertThat(payloadStruct.get("f1")).isEqualTo("this is a value from h1 header");
        assertThat(payloadStruct.get("f2")).isEqualTo("this is a value from h2 header");

    }

    @Test
    public void whenARecordThatContainsADefinedStructHeaderItWillBeCopiedInTheDefinedField() {

        Schema headerSchema = SchemaBuilder.array(SchemaBuilder.OPTIONAL_STRING_SCHEMA).optional().name("h1").build();
        headerToValue.configure(Map.of(
                "headers", "h1",
                "fields", "f1",
                "operation", "copy"));

        Struct row = new Struct(VALUE_SCHEMA)
                .put("id", 101L)
                .put("price", 20.0F)
                .put("product", "a product");

        Envelope createEnvelope = Envelope.defineSchema()
                .withName("mysql-server-1.inventory.product.Envelope")
                .withRecord(VALUE_SCHEMA)
                .withSource(Schema.STRING_SCHEMA)
                .build();
        Struct payload = createEnvelope.create(row, null , Instant.now());
        SourceRecord sourceRecord = new SourceRecord(new HashMap<>(), new HashMap<>(), "topic", createEnvelope.schema(), payload);
        sourceRecord.headers().add("h1", List.of("v1", "v2"), headerSchema);

        SourceRecord transformedRecord = headerToValue.apply(sourceRecord);

        Struct payloadStruct = Requirements.requireStruct(transformedRecord.value(), "");
        assertThat(payloadStruct.getArray("f1")).contains("v1", "v2");

    }
}