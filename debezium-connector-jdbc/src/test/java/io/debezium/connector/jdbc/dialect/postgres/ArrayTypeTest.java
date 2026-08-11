/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.data.VariableScaleDecimal;
import io.debezium.doc.FixFor;

/**
 * Unit tests for the PostgreSQL {@link ArrayType} handler.
 */
@Tag("UnitTests")
class ArrayTypeTest {

    @Test
    @DisplayName("Should strip the precision/scale modifier for createArrayOf element type")
    void testStripsPrecisionAndScale() {
        // dbz#2100: numeric(10,2)[] previously passed "decimal(10,2)" to Connection#createArrayOf,
        // which the driver rejects with "Unable to find server array type for provided name decimal(10,2)".
        assertThat(ArrayType.baseElementTypeName("decimal(10,2)")).isEqualTo("decimal");
        assertThat(ArrayType.baseElementTypeName("numeric(10,2)")).isEqualTo("numeric");
        assertThat(ArrayType.baseElementTypeName("varchar(255)")).isEqualTo("varchar");
    }

    @Test
    @DisplayName("Should strip array brackets from the element type")
    void testStripsArrayBrackets() {
        assertThat(ArrayType.baseElementTypeName("numeric(10,2)[]")).isEqualTo("numeric");
        assertThat(ArrayType.baseElementTypeName("text[]")).isEqualTo("text");
        assertThat(ArrayType.baseElementTypeName("int[][]")).isEqualTo("int");
    }

    @Test
    @DisplayName("Should leave a bare base type name unchanged")
    void testLeavesBaseTypeUnchanged() {
        assertThat(ArrayType.baseElementTypeName("numeric")).isEqualTo("numeric");
        assertThat(ArrayType.baseElementTypeName("text")).isEqualTo("text");
        assertThat(ArrayType.baseElementTypeName("uuid")).isEqualTo("uuid");
    }

    @Test
    @DisplayName("Should lower-case the base type name")
    void testLowerCasesBaseType() {
        assertThat(ArrayType.baseElementTypeName("NUMERIC(10,2)")).isEqualTo("numeric");
        assertThat(ArrayType.baseElementTypeName("VARCHAR(255)[]")).isEqualTo("varchar");
    }

    @Test
    @DisplayName("Should resolve native array element types from the array source column type")
    void testResolvesNativeElementType() {
        // dbz#2100 case 11: the source emits inet[]/cidr[]/macaddr[]/range[]/jsonb[] with a generic
        // STRING (or Json) element schema, so the element type is recovered from the "_"-prefixed
        // array type propagated on the array field, e.g. "_INET" -> "inet".
        assertThat(ArrayType.nativeElementTypeName("_INET")).isEqualTo("inet");
        assertThat(ArrayType.nativeElementTypeName("_CIDR")).isEqualTo("cidr");
        assertThat(ArrayType.nativeElementTypeName("_MACADDR")).isEqualTo("macaddr");
        assertThat(ArrayType.nativeElementTypeName("_MACADDR8")).isEqualTo("macaddr8");
        assertThat(ArrayType.nativeElementTypeName("_TSRANGE")).isEqualTo("tsrange");
        assertThat(ArrayType.nativeElementTypeName("_TSTZRANGE")).isEqualTo("tstzrange");
        assertThat(ArrayType.nativeElementTypeName("_DATERANGE")).isEqualTo("daterange");
        assertThat(ArrayType.nativeElementTypeName("_INT4RANGE")).isEqualTo("int4range");
        assertThat(ArrayType.nativeElementTypeName("_INT8RANGE")).isEqualTo("int8range");
        assertThat(ArrayType.nativeElementTypeName("_NUMRANGE")).isEqualTo("numrange");
        assertThat(ArrayType.nativeElementTypeName("_JSONB")).isEqualTo("jsonb");
    }

    @Test
    @DisplayName("Should not override numeric or other precision-bearing arrays")
    void testDoesNotOverrideNumericArray() {
        // numeric[] must keep resolving through the element schema so numeric(10,2) precision survives.
        assertThat(ArrayType.nativeElementTypeName("_NUMERIC")).isNull();
        assertThat(ArrayType.nativeElementTypeName("_TEXT")).isNull();
        assertThat(ArrayType.nativeElementTypeName("_INT4")).isNull();
        assertThat(ArrayType.nativeElementTypeName("_UUID")).isNull();
    }

    @Test
    @FixFor("debezium/dbz#2398")
    @DisplayName("Should unwrap VariableScaleDecimal elements to their decimal value")
    void testUnwrapsVariableScaleDecimalElements() {
        // Connection#createArrayOf cannot encode a Struct; the driver renders it through toString().
        final Schema elementSchema = VariableScaleDecimal.optionalSchema();
        final Schema arraySchema = SchemaBuilder.array(elementSchema).optional().build();
        final List<Object> elements = Arrays.asList(
                VariableScaleDecimal.fromLogical(elementSchema, new BigDecimal("1.25")),
                null);

        assertThat(ArrayType.unwrapElements(arraySchema, elements))
                .isEqualTo(Arrays.asList(new BigDecimal("1.25"), null));
    }

    @Test
    @FixFor("debezium/dbz#2398")
    @DisplayName("Should pass elements of other types through untouched")
    void testPassesOtherElementsThrough() {
        final Schema arraySchema = SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build();
        final List<Object> elements = List.of("a", "b");

        assertThat(ArrayType.unwrapElements(arraySchema, elements)).isSameAs(elements);
    }

    @Test
    @DisplayName("Should return null when no array source column type is available")
    void testNullWhenNoArraySourceType() {
        // Column type propagation disabled, or a non-array (no leading underscore) type.
        assertThat(ArrayType.nativeElementTypeName(null)).isNull();
        assertThat(ArrayType.nativeElementTypeName("INET")).isNull();
        assertThat(ArrayType.nativeElementTypeName("")).isNull();
    }
}
