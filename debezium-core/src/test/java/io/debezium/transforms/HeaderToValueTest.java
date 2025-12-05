/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.transforms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.util.Requirements;
import org.junit.Test;

import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;

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

        assertThatThrownBy(() -> headerToValue.configure(Map.of(
                "headers", "h1",
                "fields", "f1",
                "operation", "invalidOp")))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(
                        "Invalid value invalidOp for configuration operation: The 'operation' value is invalid: Value must be one of move, copy");

    }

    @Test
    public void whenNoFieldsDeclaredAConfigExceptionIsThrew() {

        assertThatThrownBy(() -> headerToValue.configure(Map.of(
                "headers", "h1",
                "operation", "copy")))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(
                        "Invalid value null for configuration fields: The 'fields' value is invalid: A value is required");

    }

    @Test
    public void whenNoHeadersDeclaredAConfigExceptionIsThrew() {

        assertThatThrownBy(() -> headerToValue.configure(Map.of(
                "fields", "f1",
                "operation", "copy")))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(
                        "Invalid value null for configuration headers: The 'headers' value is invalid: A value is required");

    }

    @Test
    public void whenHeadersAndFieldsHaveDifferentSizeAConfigExceptionIsThrew() {

        assertThatThrownBy(() -> headerToValue.configure(Map.of(
                "headers", "h1,h2",
                "fields", "f1",
                "operation", "copy")))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(
                        "'fields' config must have the same number of elements as 'headers' config.");

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

        Struct payload = createEnvelope.create(row, null, Instant.now());
        SourceRecord sourceRecord = new SourceRecord(new HashMap<>(), new HashMap<>(), "topic", createEnvelope.schema(), payload);
        sourceRecord.headers().add("h1", "this is a value from h1 header", Schema.STRING_SCHEMA);
        sourceRecord.headers().add("h2", "this is a value from h2 header", Schema.STRING_SCHEMA);

        SourceRecord transformedRecord = headerToValue.apply(sourceRecord);

        Struct payloadStruct = Requirements.requireStruct(transformedRecord.value(), "");
        assertThat(payloadStruct.get("f1")).isEqualTo("this is a value from h1 header");
        assertThat(payloadStruct.get("f2")).isEqualTo("this is a value from h2 header");

    }

    @Test
    public void whenFieldsOrHeadersContainsAnEmptyValueAConfigExceptionIsThrew() {

        assertThatThrownBy(() -> headerToValue.configure(Map.of(
                "headers", "h1,h2",
                "fields", ",f2",
                "operation", "copy")))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(
                        "Invalid value ,f2 for configuration fields: The 'fields' value is invalid: Empty string element(s) not permitted");
    }

    @Test
    public void whenFieldsOrHeadersContainsASpaceAConfigExceptionIsThrew() {

        assertThatThrownBy(() -> headerToValue.configure(Map.of(
                "headers", "header one",
                "fields", "f1",
                "operation", "copy")))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(
                        "Invalid value header one for configuration headers: The 'headers' value is invalid: Element(s) containing space not permitted");
    }

    @Test
    public void whenNestedFieldContainsASpaceInNestedAddressAConfigExceptionIsThrew() {

        assertThatThrownBy(() -> headerToValue.configure(Map.of(
                "headers", "headerOne",
                "fields", "after.field one",
                "operation", "copy")))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(
                        "Invalid value after.field one for configuration fields: The 'fields' value is invalid: Element(s) containing space not permitted");
    }

    @Test
    public void whenNestedFieldIsSeparatedWithADotAndASpaceAConfigExceptionIsThrew() {

        assertThatThrownBy(() -> headerToValue.configure(Map.of(
                "headers", "headerOne",
                "fields", "after. fieldOne",
                "operation", "copy")))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(
                        "Invalid value after. fieldOne for configuration fields: The 'fields' value is invalid: Element(s) containing space not permitted");
    }

    @Test
    public void leadingAndTrailingSpacesInFieldsAndHeadersConfigurationAreCorrectlyTrimmed() {

        Schema headerSchema = SchemaBuilder.array(SchemaBuilder.OPTIONAL_STRING_SCHEMA).optional().name("h1").build();
        headerToValue.configure(Map.of(
                "headers", " h1 , h2",
                "fields", " f1, f2 ",
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
        Struct payload = createEnvelope.create(row, null, Instant.now());
        SourceRecord sourceRecord = new SourceRecord(new HashMap<>(), new HashMap<>(), "topic", createEnvelope.schema(), payload);
        sourceRecord.headers().add("h1", List.of("v1", "v2"), headerSchema);
        sourceRecord.headers().add("h2", List.of("v1", "v2"), headerSchema);

        SourceRecord transformedRecord = headerToValue.apply(sourceRecord);

        Struct payloadStruct = Requirements.requireStruct(transformedRecord.value(), "");
        assertThat(payloadStruct.getArray("f1")).contains("v1", "v2");
        assertThat(payloadStruct.getArray("f2")).contains("v1", "v2");

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
        Struct payload = createEnvelope.create(row, null, Instant.now());
        SourceRecord sourceRecord = new SourceRecord(new HashMap<>(), new HashMap<>(), "topic", createEnvelope.schema(), payload);
        sourceRecord.headers().add("h1", List.of("v1", "v2"), headerSchema);

        SourceRecord transformedRecord = headerToValue.apply(sourceRecord);

        Struct payloadStruct = Requirements.requireStruct(transformedRecord.value(), "");
        assertThat(payloadStruct.getArray("f1")).contains("v1", "v2");

    }

    @Test
    public void whenARecordThatContainsADefinedHeaderAndOperationIsMoveItWillBeCopiedInTheDefinedFieldAndRemovedFromHeaders() {

        Schema headerSchema = SchemaBuilder.array(SchemaBuilder.OPTIONAL_STRING_SCHEMA).optional().name("h1").build();
        headerToValue.configure(Map.of(
                "headers", "h1,h2,h3",
                "fields", "f1,f2,f3",
                "operation", "move"));

        Struct row = new Struct(VALUE_SCHEMA)
                .put("id", 101L)
                .put("price", 20.0F)
                .put("product", "a product");

        Envelope createEnvelope = Envelope.defineSchema()
                .withName("mysql-server-1.inventory.product.Envelope")
                .withRecord(VALUE_SCHEMA)
                .withSource(Schema.STRING_SCHEMA)
                .build();

        Struct payload = createEnvelope.create(row, null, Instant.now());
        SourceRecord sourceRecord = new SourceRecord(new HashMap<>(), new HashMap<>(), "topic", createEnvelope.schema(), payload);
        sourceRecord.headers().add("h1", List.of("v1", "v2"), headerSchema);
        sourceRecord.headers().add("h2", List.of("v1", "v2"), headerSchema);
        sourceRecord.headers().add("h3", List.of("v1", "v2"), headerSchema);
        sourceRecord.headers().add("h4", List.of("v1", "v2"), headerSchema);

        SourceRecord transformedRecord = headerToValue.apply(sourceRecord);

        Struct payloadStruct = Requirements.requireStruct(transformedRecord.value(), "");
        assertThat(payloadStruct.getArray("f1")).contains("v1", "v2");
        assertThat(payloadStruct.getArray("f2")).contains("v1", "v2");
        assertThat(payloadStruct.getArray("f3")).contains("v1", "v2");
        assertThat(StreamSupport.stream(transformedRecord.headers().spliterator(), false)
                .map(Header::key).collect(Collectors.toList())).containsExactly("h4");

    }

    @Test
    public void supportNestedField() {

        headerToValue.configure(Map.of(
                "headers", "h1,h2,h3",
                "fields", "f1,after.f2,source.f3",
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

        Struct payload = createEnvelope.create(row, null, Instant.now());
        SourceRecord sourceRecord = new SourceRecord(new HashMap<>(), new HashMap<>(), "topic", createEnvelope.schema(), payload);
        sourceRecord.headers().add("h1", "this is a value from h1 header", Schema.STRING_SCHEMA);
        sourceRecord.headers().add("h2", "this is a value from h2 header", Schema.STRING_SCHEMA);

        SourceRecord transformedRecord = headerToValue.apply(sourceRecord);

        Struct payloadStruct = Requirements.requireStruct(transformedRecord.value(), "");
        assertThat(payloadStruct.get("f1")).isEqualTo("this is a value from h1 header");

        Struct after = Requirements.requireStruct(payloadStruct.get("after"), "");
        assertThat(after.get("f2")).isEqualTo("this is a value from h2 header");

    }

    @Test
    public void notExistingHeader() {

        headerToValue.configure(Map.of(
                "headers", "h1,",
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

        Struct payload = createEnvelope.create(row, null, Instant.now());
        SourceRecord sourceRecord = new SourceRecord(new HashMap<>(), new HashMap<>(), "topic", createEnvelope.schema(), payload);
        sourceRecord.headers().add("h2", "this is a value from h2 header", Schema.STRING_SCHEMA);

        SourceRecord transformedRecord = headerToValue.apply(sourceRecord);

        Struct payloadStruct = Requirements.requireStruct(transformedRecord.value(), "");
        assertThat(payloadStruct).isEqualTo(payload);

    }

    @Test
    public void notExistingNestedField() {

        headerToValue.configure(Map.of(
                "headers", "h1,",
                "fields", "parent.f1",
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

        Struct payload = createEnvelope.create(row, null, Instant.now());
        SourceRecord sourceRecord = new SourceRecord(new HashMap<>(), new HashMap<>(), "topic", createEnvelope.schema(), payload);
        sourceRecord.headers().add("h2", "this is a value from h2 header", Schema.STRING_SCHEMA);

        SourceRecord transformedRecord = headerToValue.apply(sourceRecord);

        Struct payloadStruct = Requirements.requireStruct(transformedRecord.value(), "");
        assertThat(payloadStruct).isEqualTo(payload);

    }

    @Test
    @FixFor("DBZ-6411")
    public void whenARecordNotContainsHeadersShouldBeSkipped() {

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

        Struct payload = createEnvelope.read(row, null, Instant.now());
        SourceRecord readRecord = new SourceRecord(new HashMap<>(), new HashMap<>(), "topic", createEnvelope.schema(), payload);

        SourceRecord transformedRecord = headerToValue.apply(readRecord);

        assertThat(transformedRecord).isEqualTo(readRecord);

        payload = createEnvelope.create(row, null, Instant.now());
        SourceRecord sourceRecord = new SourceRecord(new HashMap<>(), new HashMap<>(), "topic", createEnvelope.schema(), payload);
        sourceRecord.headers().add("h1", List.of("v1", "v2"), headerSchema);

        transformedRecord = headerToValue.apply(sourceRecord);

        Struct payloadStruct = Requirements.requireStruct(transformedRecord.value(), "");
        assertThat(payloadStruct.getArray("f1")).contains("v1", "v2");

    }

    @Test
    @FixFor("DBZ-6588")
    public void whenATombstoneRecordItShouldBeSkipped() {

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

        SourceRecord readRecord = new SourceRecord(new HashMap<>(), new HashMap<>(), "topic", createEnvelope.schema(), null);

        SourceRecord transformedRecord = headerToValue.apply(readRecord);

        assertThat(transformedRecord).isEqualTo(readRecord);
    }
}
