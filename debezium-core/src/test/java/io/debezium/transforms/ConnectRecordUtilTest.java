/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

import io.debezium.data.Envelope;

public class ConnectRecordUtilTest {

    public static final Schema NESTED_SCHEMA = SchemaBuilder.struct()
            .name("mysql.inventory.products.Value.Nested")
            .field("product", Schema.STRING_SCHEMA)
            .build();

    public static final Schema VALUE_SCHEMA = SchemaBuilder.struct()
            .name("mysql.inventory.products.Value")
            .field("id", Schema.INT64_SCHEMA)
            .field("price", Schema.FLOAT32_SCHEMA)
            .field("product", Schema.STRING_SCHEMA)
            .field("nested", NESTED_SCHEMA)
            .build();

    public static final Struct NESTED_ROW = new Struct(NESTED_SCHEMA)
            .put("product", "another product");

    public static final Struct ROW = new Struct(VALUE_SCHEMA)
            .put("id", 101L)
            .put("price", 20.0F)
            .put("product", "a product")
            .put("nested", NESTED_ROW);

    public static final Envelope ENVELOPE = Envelope.defineSchema()
            .withName("mysql.inventory.products.Envelope")
            .withRecord(VALUE_SCHEMA)
            .withSource(Schema.STRING_SCHEMA)
            .build();

    public static final Struct PAYLOAD = ENVELOPE.create(ROW, null, Instant.now());

    @Test
    public void testNewSchemaAddOneField() {
        Schema newSchema = ConnectRecordUtil.makeNewSchema(VALUE_SCHEMA, List.of(new ConnectRecordUtil.NewEntry("newField", Schema.STRING_SCHEMA, "just test")));

        assertThat(newSchema.field("newField")).isNotNull();
        assertThat(newSchema.field("newField").schema()).isEqualTo(Schema.STRING_SCHEMA);
    }

    @Test
    public void testNewSchemaAddMultipleFields() {
        List<ConnectRecordUtil.NewEntry> newFields = List.of(
                new ConnectRecordUtil.NewEntry("newString", Schema.STRING_SCHEMA, "just test"),
                new ConnectRecordUtil.NewEntry("after.newInt", Schema.INT32_SCHEMA, 10),
                new ConnectRecordUtil.NewEntry("after.nested.newFloat", Schema.FLOAT32_SCHEMA, 30.0F));
        Schema newSchema = ConnectRecordUtil.makeNewSchema(PAYLOAD.schema(), newFields);

        assertThat(newSchema.field("newString").schema()).isEqualTo(Schema.STRING_SCHEMA);
        assertThat(newSchema.field("after").schema().field("newInt").schema()).isEqualTo(Schema.INT32_SCHEMA);
        assertThat(newSchema.field("after")
                .schema().field("nested")
                .schema().field("newFloat").schema()).isEqualTo(Schema.FLOAT32_SCHEMA);
    }

    @Test
    public void testNewValueAddOneField() {
        List<ConnectRecordUtil.NewEntry> newFields = List.of(new ConnectRecordUtil.NewEntry("after.newField", Schema.STRING_SCHEMA, "just test"));
        Schema newSchema = ConnectRecordUtil.makeNewSchema(PAYLOAD.schema(), newFields);
        Struct newValue = ConnectRecordUtil.makeUpdatedValue(PAYLOAD, newFields, newSchema);

        assertThat(newValue.getStruct("after").getString("newField")).isEqualTo("just test");
        assertThat(newValue.getStruct("after").getInt64("id")).isEqualTo(101L);
        assertThat(newValue.getStruct("after").getFloat32("price")).isEqualTo(20.0F);
        assertThat(newValue.getStruct("after").getString("product")).isEqualTo("a product");
    }

    @Test
    public void testAddMultipleFields() {
        List<ConnectRecordUtil.NewEntry> newFields = List.of(
                new ConnectRecordUtil.NewEntry("newString", Schema.STRING_SCHEMA, "just test"),
                new ConnectRecordUtil.NewEntry("after.newInt", Schema.INT32_SCHEMA, 10),
                new ConnectRecordUtil.NewEntry("after.nested.newFloat", Schema.FLOAT32_SCHEMA, 30.0F));
        Schema newSchema = ConnectRecordUtil.makeNewSchema(PAYLOAD.schema(), newFields);
        Struct newValue = ConnectRecordUtil.makeUpdatedValue(PAYLOAD, newFields, newSchema);

        assertThat(newValue.getString("newString")).isEqualTo("just test");
        assertThat(newValue.getStruct("after").getInt32("newInt")).isEqualTo(10);
        assertThat(newValue.getStruct("after").getInt64("id")).isEqualTo(101L);
        assertThat(newValue.getStruct("after").getFloat32("price")).isEqualTo(20.0F);
        assertThat(newValue.getStruct("after").getString("product")).isEqualTo("a product");
        assertThat(newValue.getStruct("after").getStruct("nested").getFloat32("newFloat")).isEqualTo(30.0F);
        assertThat(newValue.getStruct("after").getStruct("nested").getString("product")).isEqualTo("another product");
    }

    @Test
    public void testAddTopLevelField() {
        List<ConnectRecordUtil.NewEntry> newFields = List.of(new ConnectRecordUtil.NewEntry("newField", Schema.STRING_SCHEMA, "just test"));
        Schema newSchema = ConnectRecordUtil.makeNewSchema(PAYLOAD.schema(), newFields);
        Struct newValue = ConnectRecordUtil.makeUpdatedValue(PAYLOAD, newFields, newSchema);

        assertThat(newValue.getString("newField")).isEqualTo("just test");
        assertThat(newValue.getStruct("after").getInt64("id")).isEqualTo(101L);
        assertThat(newValue.getStruct("after").getFloat32("price")).isEqualTo(20.0F);
        assertThat(newValue.getStruct("after").getString("product")).isEqualTo("a product");
    }

    @Test
    public void testAddNestedField() {
        List<ConnectRecordUtil.NewEntry> newFields = List.of(new ConnectRecordUtil.NewEntry("after.nested.newField", Schema.STRING_SCHEMA, "just test"));
        Schema newSchema = ConnectRecordUtil.makeNewSchema(PAYLOAD.schema(), newFields);
        Struct newValue = ConnectRecordUtil.makeUpdatedValue(PAYLOAD, newFields, newSchema);

        assertThat(newValue.getStruct("after").getStruct("nested").getString("newField")).isEqualTo("just test");
    }

    @Test
    public void testAddNestedFieldWithSimilarName() {
        List<ConnectRecordUtil.NewEntry> newFields = List.of(new ConnectRecordUtil.NewEntry("after.nested.product2", Schema.STRING_SCHEMA, "just test"));
        Schema newSchema = ConnectRecordUtil.makeNewSchema(PAYLOAD.schema(), newFields);
        Struct newValue = ConnectRecordUtil.makeUpdatedValue(PAYLOAD, newFields, newSchema);

        assertThat(newValue.getStruct("after").getStruct("nested").getString("product2")).isEqualTo("just test");
    }

    @Test
    public void testAddNestedStruct() {
        List<ConnectRecordUtil.NewEntry> newFields = List.of(new ConnectRecordUtil.NewEntry("after.nested2", NESTED_SCHEMA, NESTED_ROW));
        Schema newSchema = ConnectRecordUtil.makeNewSchema(PAYLOAD.schema(), newFields);
        Struct newValue = ConnectRecordUtil.makeUpdatedValue(PAYLOAD, newFields, newSchema);

        assertThat(newValue.getStruct("after").getStruct("nested2").getString("product")).isEqualTo("another product");
    }
}
