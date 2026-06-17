/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.postgres;

import static org.assertj.core.api.Assertions.assertThat;

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
 * Unit tests for the PostgreSQL {@link BitType} handler.
 */
@Tag("UnitTests")
class BitTypeTest {

    @Test
    @FixFor("debezium/dbz#2099")
    @DisplayName("Should register for Debezium Bits and PostgreSQL bit type names")
    void testRegistrationKeys() {
        assertThat(BitType.INSTANCE.getRegistrationKeys()).containsExactly(Bits.LOGICAL_NAME, "BIT", "VARBIT");
    }

    @Test
    @FixFor("debezium/dbz#2099")
    @DisplayName("Should bind multi-byte bit values using Debezium Bits byte order")
    void testBindMultiByteBitValues() {
        assertBindValue(Bits.schema(16), new byte[]{ 0x02, 0x01 }, "0000000100000010");
        assertBindValue(Bits.schema(24), new byte[]{ 0x01, 0x02, 0x03 }, "000000110000001000000001");
    }

    @Test
    @FixFor("debezium/dbz#2099")
    @DisplayName("Should bind ByteBuffer bit values from Avro converter")
    void testBindByteBufferBitValues() {
        assertBindValue(Bits.schema(16), ByteBuffer.wrap(new byte[]{ 0x02, 0x01 }), "0000000100000010");
        assertBindValue(Bits.schema(24), ByteBuffer.wrap(new byte[]{ 0x01, 0x02, 0x03 }), "000000110000001000000001");
    }

    @Test
    @FixFor("debezium/dbz#2099")
    @DisplayName("Should bind values with the high bit set as unsigned bit strings")
    void testBindHighBitValues() {
        assertBindValue(Bits.schema(8), new byte[]{ (byte) 0x80 }, "10000000");
        assertBindValue(Bits.schema(8), new byte[]{ (byte) 0xff }, "11111111");
        assertBindValue(Bits.schema(8), new byte[]{ (byte) 0x81 }, "10000001");
    }

    @Test
    @FixFor("debezium/dbz#2099")
    @DisplayName("Should preserve leading zeroes for bounded bit values")
    void testBindBoundedValuesWithLeadingZeroes() {
        assertBindValue(Bits.schema(2), new byte[]{}, "00");
        assertBindValue(Bits.schema(7), new byte[]{ 0x40 }, "1000000");
    }

    @Test
    @FixFor("debezium/dbz#2099")
    @DisplayName("Should bind unbounded varbit values using the payload bit length")
    void testBindUnboundedVarbitValue() {
        assertBindValue(Bits.schema(Integer.MAX_VALUE), new byte[]{ 0x05 }, "101");
    }

    private void assertBindValue(Schema schema, Object value, String expected) {
        final List<ValueBindDescriptor> descriptors = BitType.INSTANCE.bind(1, schema, value);

        assertThat(descriptors).hasSize(1);
        assertThat(descriptors.get(0).getIndex()).isEqualTo(1);
        assertThat(descriptors.get(0).getValue()).isEqualTo(expected);
        assertThat(descriptors.get(0).getTargetSqlType()).isNull();
    }
}
