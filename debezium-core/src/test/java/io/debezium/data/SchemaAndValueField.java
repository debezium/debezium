/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.function.Supplier;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

public class SchemaAndValueField {
    private final Schema schema;
    private final Object value;
    private final String fieldName;
    private Supplier<Boolean> assertValueOnlyIf = null;

    public SchemaAndValueField(String fieldName, Schema schema, Object value) {
        this.schema = schema;
        this.value = value;
        this.fieldName = fieldName;
    }

    public SchemaAndValueField assertValueOnlyIf(final Supplier<Boolean> predicate) {
        assertValueOnlyIf = predicate;
        return this;
    }

    public Schema schema() {
        return schema;
    }

    public String fieldName() {
        return fieldName;
    }

    public Object value() {
        return value;
    }

    public void assertFor(Struct content) {
        assertSchema(content);
        assertValue(content);
    }

    private void assertValue(Struct content) {
        if (assertValueOnlyIf != null && !assertValueOnlyIf.get()) {
            return;
        }

        if (value == null) {
            assertThat(content.get(fieldName)).as(fieldName + " is present in the actual content").isNull();
            return;
        }
        Object actualValue = content.get(fieldName);
        assertThat(actualValue).as(fieldName + " is not present in the actual content").isNotNull();

        // assert the value type; for List all implementation types (e.g. immutable ones) are acceptable
        if (actualValue instanceof List) {
            assertThat(value).as("Incorrect value type for " + fieldName).isInstanceOf(List.class);
            final List<?> actualValueList = (List<?>) actualValue;
            final List<?> valueList = (List<?>) value;
            assertThat(actualValueList).as("List size don't match for " + fieldName).hasSize(valueList.size());
            if (!valueList.isEmpty() && valueList.iterator().next() instanceof Struct) {
                for (int i = 0; i < valueList.size(); i++) {
                    assertStruct((Struct) valueList.get(i), (Struct) actualValueList.get(i));
                }
                return;
            }
        }
        else {
            assertThat(actualValue.getClass()).as("Incorrect value type for " + fieldName).isEqualTo(value.getClass());
        }

        if (actualValue instanceof byte[]) {
            assertThat((byte[]) actualValue).as("Values don't match for " + fieldName).isEqualTo((byte[]) value);
        }
        else if (actualValue instanceof Struct) {
            assertStruct((Struct) value, (Struct) actualValue);
        }
        else {
            assertThat(actualValue).as("Values don't match for " + fieldName).isEqualTo(value);
        }
    }

    private void assertStruct(final Struct expectedStruct, final Struct actualStruct) {
        expectedStruct.schema().fields().stream().forEach(field -> {
            final Object expectedValue = expectedStruct.get(field);
            if (expectedValue == null) {
                assertThat(actualStruct.get(field.name())).as(fieldName + " is present in the actual content").isNull();
                return;
            }
            final Object actualValue = actualStruct.get(field.name());
            assertThat(actualValue).as("No value found for " + fieldName).isNotNull();
            assertThat(actualValue.getClass()).as("Incorrect value type for " + fieldName).isEqualTo(expectedValue.getClass());
            if (actualValue instanceof byte[]) {
                assertThat(expectedValue).as("Array is not expected for " + fieldName).isInstanceOf(byte[].class);
                assertThat((byte[]) actualValue).as("Values don't match for " + fieldName).isEqualTo((byte[]) expectedValue);
            }
            else if (actualValue instanceof Struct) {
                assertStruct((Struct) expectedValue, (Struct) actualValue);
            }
            else {
                assertThat(actualValue).as("Values don't match for " + fieldName).isEqualTo(expectedValue);
            }
        });
    }

    private void assertSchema(Struct content) {
        if (schema == null) {
            return;
        }
        Schema schema = content.schema();
        Field field = schema.field(fieldName);
        assertThat(field).as(fieldName + " not found in schema " + schema).isNotNull();

        VerifyRecord.assertConnectSchemasAreEqual(field.name(), field.schema(), this.schema);
    }
}
