/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.data.Bits;
import io.debezium.doc.FixFor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;

/**
 * Unit tests for the MySQL {@link BitType} handler.
 */
@Tag("UnitTests")
class BitTypeTest {

    private static final BigInteger UNSIGNED_64BIT_MAX_VALUE = new BigInteger("18446744073709551615");

    @Test
    @FixFor("debezium/dbz#2102")
    @DisplayName("Should bind one-bit boundary values")
    void shouldBindOneBitBoundaryValues() {
        assertBindValue(Bits.schema(1), new byte[]{ 0x00 }, BigInteger.ZERO);
        assertBindValue(Bits.schema(1), new byte[]{ 0x01 }, BigInteger.ONE);
    }

    @Test
    @FixFor("debezium/dbz#2102")
    @DisplayName("Should bind multi-byte bit values using Debezium Bits byte order")
    void shouldBindMultiByteBitValues() {
        assertBindValue(Bits.schema(9), new byte[]{ 0x55, 0x01 }, new BigInteger("341"));
        assertBindValue(Bits.schema(16), new byte[]{ 0x02, 0x01 }, new BigInteger("258"));
    }

    @Test
    @FixFor("debezium/dbz#2102")
    @DisplayName("Should bind ByteBuffer bit values from Avro converter")
    void shouldBindByteBufferBitValues() {
        assertBindValue(Bits.schema(9), ByteBuffer.wrap(new byte[]{ 0x55, 0x01 }), new BigInteger("341"));
    }

    @Test
    @FixFor("debezium/dbz#2102")
    @DisplayName("Should bind values with the high bit set as unsigned values")
    void shouldBindHighBitValuesAsUnsignedValues() {
        assertBindValue(Bits.schema(8), new byte[]{ (byte) 0x80 }, new BigInteger("128"));
        assertBindValue(Bits.schema(8), new byte[]{ (byte) 0xff }, new BigInteger("255"));
    }

    @Test
    @FixFor("debezium/dbz#2102")
    @DisplayName("Should bind the maximum MySQL BIT width")
    void shouldBindMaximumBitWidth() {
        assertBindValue(Bits.schema(64), new byte[]{ 0x00 }, BigInteger.ZERO);
        assertBindValue(Bits.schema(64),
                new byte[]{ (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff },
                UNSIGNED_64BIT_MAX_VALUE);
    }

    @Test
    @FixFor("debezium/dbz#2102")
    @DisplayName("Should format default values using MySQL bit literals")
    void shouldFormatDefaultValuesAsBitLiterals() {
        assertThat(BitType.INSTANCE.getDefaultValueBinding(Bits.schema(9), new byte[]{ 0x55, 0x01 })).isEqualTo("b'101010101'");
        assertThat(BitType.INSTANCE.getDefaultValueBinding(Bits.schema(8), new byte[]{ (byte) 0x80 })).isEqualTo("b'10000000'");
        assertThat(BitType.INSTANCE.getDefaultValueBinding(Bits.schema(64),
                new byte[]{ (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff }))
                .isEqualTo("b'1111111111111111111111111111111111111111111111111111111111111111'");
    }

    private void assertBindValue(Schema schema, Object value, BigInteger expected) {
        final List<ValueBindDescriptor> descriptors = BitType.INSTANCE.bind(1, schema, value);

        assertThat(descriptors).hasSize(1);
        assertThat(descriptors.get(0).getIndex()).isEqualTo(1);
        assertThat(descriptors.get(0).getValue()).isEqualTo(expected);
        assertThat(descriptors.get(0).getTargetSqlType()).isNull();
    }
}
