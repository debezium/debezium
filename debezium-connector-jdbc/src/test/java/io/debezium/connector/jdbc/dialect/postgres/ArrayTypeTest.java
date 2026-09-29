/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import io.debezium.connector.jdbc.type.connect.ConnectDecimalType;
import io.debezium.connector.jdbc.type.connect.ConnectStringType;
import io.debezium.connector.jdbc.type.debezium.VariableScaleDecimalType;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.doc.FixFor;

/**
 * Unit tests for the PostgreSQL {@link ArrayType} handler.
 */
@Tag("UnitTests")
class ArrayTypeTest {

    private static ArrayType configuredArrayType() {
        final var dialect = Mockito.mock(io.debezium.connector.jdbc.dialect.DatabaseDialect.class);
        final var arrayType = new ArrayType();
        final var integerType = new io.debezium.connector.jdbc.type.connect.ConnectInt32Type();
        Mockito.when(dialect.getSchemaType(ArgumentMatchers.argThat(value -> value.type() == Schema.Type.ARRAY))).thenReturn(arrayType);
        Mockito.when(dialect.getSchemaType(Schema.OPTIONAL_INT32_SCHEMA)).thenReturn(integerType);
        Mockito.when(dialect.getJdbcTypeName(Types.INTEGER)).thenReturn("integer");
        integerType.configure(null, dialect);
        arrayType.configure(null, dialect);
        return arrayType;
    }

    @Test
    @FixFor("debezium/dbz#2572")
    @DisplayName("Should convert two-dimensional Connect arrays for JDBC binding")
    void testConvertsTwoDimensionalArray() {
        final var arrayType = configuredArrayType();
        final Schema schema = SchemaBuilder.array(SchemaBuilder.array(Schema.OPTIONAL_INT32_SCHEMA).build()).build();

        assertThat(arrayType.bind(1, schema, List.of(List.of(1, 2), List.of(3, 4))))
                .singleElement().satisfies(binding -> {
                    assertThat(binding.getElementTypeName()).isEqualTo("integer");
                    assertThat(binding.getValue()).isEqualTo(new Object[]{ new Object[]{ 1, 2 }, new Object[]{ 3, 4 } });
                });
    }

    @Test
    @FixFor("debezium/dbz#2572")
    @DisplayName("Should convert three-dimensional Connect arrays for JDBC binding")
    void testConvertsThreeDimensionalArray() {
        final var arrayType = configuredArrayType();
        final Schema schema = SchemaBuilder.array(SchemaBuilder.array(SchemaBuilder.array(Schema.OPTIONAL_INT32_SCHEMA).build()).build()).build();

        assertThat(arrayType.bind(1, schema, List.of(List.of(List.of(1), List.of(2)), List.of(List.of(3), List.of(4)))))
                .singleElement().satisfies(binding -> assertThat(binding.getValue())
                        .isEqualTo(new Object[]{ new Object[]{ new Object[]{ 1 }, new Object[]{ 2 } }, new Object[]{ new Object[]{ 3 }, new Object[]{ 4 } } }));
    }

    @Test
    @FixFor("debezium/dbz#2572")
    @DisplayName("Should reject ragged nested arrays")
    void testRejectsRaggedNestedArray() {
        final var arrayType = configuredArrayType();
        final Schema schema = SchemaBuilder.array(SchemaBuilder.array(Schema.OPTIONAL_INT32_SCHEMA).build()).build();

        assertThatThrownBy(() -> arrayType.bind(1, schema, List.of(List.of(1), List.of(2, 3))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ragged")
                .hasMessageContaining("rectangular");
    }

    @Test
    @FixFor("debezium/dbz#2572")
    @DisplayName("Should preserve null scalar elements in nested arrays")
    void testPreservesNullScalarElements() {
        final var arrayType = configuredArrayType();
        final Schema schema = SchemaBuilder.array(SchemaBuilder.array(Schema.OPTIONAL_INT32_SCHEMA).build()).build();
        final List<Object> value = List.of(Arrays.asList(1, null), Arrays.asList(3, 4));

        assertThat(arrayType.bind(1, schema, value))
                .singleElement().satisfies(binding -> assertThat(binding.getValue())
                        .isEqualTo(new Object[]{ new Object[]{ 1, null }, new Object[]{ 3, 4 } }));
    }

    @Test
    @FixFor("debezium/dbz#2572")
    @DisplayName("Should reject null inner arrays")
    void testRejectsNullInnerArray() {
        final var arrayType = configuredArrayType();
        final Schema schema = SchemaBuilder.array(SchemaBuilder.array(Schema.OPTIONAL_INT32_SCHEMA).build()).build();
        final List<Object> value = Arrays.asList(List.of(1), null);

        assertThatThrownBy(() -> arrayType.bind(1, schema, value))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("null inner array");
    }

    static Stream<Schema> bytesSchemas() {
        return Stream.of(Schema.OPTIONAL_BYTES_SCHEMA,
                SchemaBuilder.bytes().name("custom.Binary").optional().build());
    }

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
        final var decimal = new BigDecimal("12345678901234567890.123456789");
        final List<Object> elements = Arrays.asList(
                VariableScaleDecimal.fromLogical(elementSchema, decimal),
                null);

        assertThat(VariableScaleDecimalType.INSTANCE.convertArray(elementSchema, elements))
                .containsExactly(decimal, null);
    }

    @Test
    @FixFor("debezium/dbz#2398")
    @DisplayName("Should pass elements of other types through untouched")
    void testPassesOtherElementsThrough() {
        final List<Object> elements = Arrays.asList("a", null, "b");

        assertThat(ConnectStringType.INSTANCE.convertArray(Schema.OPTIONAL_STRING_SCHEMA, elements))
                .containsExactly("a", null, "b");
    }

    @ParameterizedTest
    @MethodSource("bytesSchemas")
    @FixFor("debezium/dbz#2571")
    @DisplayName("Should convert named and unnamed BYTES elements to a typed byte[][]")
    void testConvertsBytesElementsToTypedArray(Schema schema) {
        // Connection#createArrayOf rejects a byte[] element inside a generic Object[].
        final List<Object> elements = Arrays.asList(
                new byte[]{ 1, 2, 3 },
                ByteBuffer.wrap(new byte[]{ 4, 5, 6 }),
                null);

        assertThat(BytesType.INSTANCE.convertArray(schema, elements))
                .isInstanceOf(byte[][].class)
                .isEqualTo(new byte[][]{ { 1, 2, 3 }, { 4, 5, 6 }, null });
    }

    @ParameterizedTest
    @MethodSource("bytesSchemas")
    @FixFor("debezium/dbz#2571")
    @DisplayName("Should reject a BYTES element that is neither byte[] nor ByteBuffer")
    void testRejectsUnconvertibleBytesElement(Schema schema) {
        // A silent SQL NULL here would turn an upstream converter bug into invisible data loss.
        final List<Object> elements = List.of("not-binary");

        assertThatThrownBy(() -> BytesType.INSTANCE.convertArray(schema, elements))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported BYTES array element type")
                .hasMessageContaining(String.class.getName());
    }

    @ParameterizedTest
    @MethodSource("bytesSchemas")
    @FixFor("debezium/dbz#2571")
    @DisplayName("Should convert an empty BYTES array to an empty byte[][]")
    void testConvertsEmptyBytesArray(Schema schema) {
        assertThat(BytesType.INSTANCE.convertArray(schema, List.of()))
                .isInstanceOf(byte[][].class)
                .isEmpty();
    }

    @ParameterizedTest
    @MethodSource("bytesSchemas")
    @FixFor("debezium/dbz#2571")
    @DisplayName("Should retain the binary array type when every element is null")
    void testConvertsBytesArrayWithOnlyNulls(Schema schema) {
        assertThat(BytesType.INSTANCE.convertArray(schema, Arrays.asList(null, null)))
                .isInstanceOf(byte[][].class)
                .containsExactly(null, null);
    }

    @Test
    @FixFor("debezium/dbz#2571")
    @DisplayName("Should pass logical BYTES elements through untouched")
    void testPassesLogicalBytesElementsThrough() {
        // Decimal is BYTES-based but its elements arrive as already converted BigDecimal values.
        final var decimal = new BigDecimal("1.25");
        final List<Object> elements = List.of(decimal);

        assertThat(ConnectDecimalType.INSTANCE.convertArray(Decimal.schema(2), elements))
                .containsExactly(decimal);
    }

    @Test
    @DisplayName("Should convert an empty decimal array")
    void testConvertsEmptyDecimalArray() {
        assertThat(VariableScaleDecimalType.INSTANCE.convertArray(VariableScaleDecimal.optionalSchema(), List.of()))
                .isEmpty();
    }

    @Test
    @DisplayName("Should preserve scalar VariableScaleDecimal binding")
    void testPreservesScalarDecimalBinding() {
        final Schema schema = VariableScaleDecimal.optionalSchema();
        final var decimal = new BigDecimal("12345678901234567890.123456789");
        final var value = VariableScaleDecimal.fromLogical(schema, decimal);

        assertThat(VariableScaleDecimalType.INSTANCE.bind(3, schema, value))
                .singleElement().satisfies(binding -> {
                    assertThat(binding.getIndex()).isEqualTo(3);
                    assertThat(binding.getValue()).isEqualTo(decimal);
                });
        assertThat(VariableScaleDecimalType.INSTANCE.bind(3, schema, null))
                .singleElement().satisfies(binding -> assertThat(binding.getValue()).isNull());
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
