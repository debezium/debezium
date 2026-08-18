/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.serde;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.data.VariableScaleDecimal;

/**
 * Unit tests for {@link ConnectValueSerde}, exercising exact-runtime-type round-trips for every
 * supported value type, the envelope timestamp, and the failure contract for unsupported types and
 * undecodable bytes.
 *
 * @author Chris Cranford
 */
public class ConnectValueSerdeTest {

    private final ConnectValueSerde serde = new ConnectValueSerde();

    private Object roundTrip(Object value) {
        return serde.deserialize(serde.serialize(value, 42L)).value();
    }

    @Test
    public void timestampIsPreserved() {
        assertThat(serde.deserialize(serde.serialize("x", 12345L)).timestampMs()).isEqualTo(12345L);
    }

    @Test
    public void nullRoundTrips() {
        assertThat(roundTrip(null)).isNull();
    }

    @Test
    public void primitivesRoundTripWithExactRuntimeTypes() {
        assertThat(roundTrip("hello")).isEqualTo("hello");
        assertThat(roundTrip(Boolean.TRUE)).isEqualTo(Boolean.TRUE);
        assertThat(roundTrip((byte) 7)).isInstanceOf(Byte.class).isEqualTo((byte) 7);
        assertThat(roundTrip((short) 7)).isInstanceOf(Short.class).isEqualTo((short) 7);
        assertThat(roundTrip(7)).isInstanceOf(Integer.class).isEqualTo(7);
        assertThat(roundTrip(7L)).isInstanceOf(Long.class).isEqualTo(7L);
        assertThat(roundTrip(7.5f)).isInstanceOf(Float.class).isEqualTo(7.5f);
        assertThat(roundTrip(7.5d)).isInstanceOf(Double.class).isEqualTo(7.5d);
    }

    @Test
    public void integerDoesNotWidenToLong() {
        // Struct.put validates the exact runtime class, so an Integer must never come back as a Long.
        assertThat(roundTrip(7).getClass()).isEqualTo(Integer.class);
        assertThat(roundTrip(7L).getClass()).isEqualTo(Long.class);
    }

    @Test
    public void byteArrayAndByteBufferKeepTheirDistinctTypes() {
        final Object array = roundTrip(new byte[]{ 1, 2, 3 });
        assertThat(array).isInstanceOf(byte[].class);
        assertThat((byte[]) array).containsExactly(1, 2, 3);

        final Object buffer = roundTrip(ByteBuffer.wrap(new byte[]{ 4, 5, 6 }));
        assertThat(buffer).isInstanceOf(ByteBuffer.class);
        assertThat(((ByteBuffer) buffer).array()).containsExactly(4, 5, 6);
    }

    @Test
    public void byteBufferSerializationDoesNotConsumeTheBuffer() {
        final ByteBuffer buffer = ByteBuffer.wrap(new byte[]{ 1, 2, 3 });
        serde.serialize(buffer, 0L);
        assertThat(buffer.remaining()).isEqualTo(3);
    }

    @Test
    public void stringsLargerThanSixtyFourKilobytesRoundTrip() {
        // writeUTF would fail here; LOB values routinely exceed 64KB.
        final String large = "x".repeat(100_000);
        assertThat(roundTrip(large)).isEqualTo(large);
    }

    @Test
    public void bigDecimalPreservesScaleAndValue() {
        final BigDecimal decimal = new BigDecimal("12345.6789");
        final Object result = roundTrip(decimal);
        assertThat(result).isInstanceOf(BigDecimal.class);
        assertThat(result).isEqualTo(decimal);
        assertThat(((BigDecimal) result).scale()).isEqualTo(4);
    }

    @Test
    public void bigIntegerRoundTrips() {
        final BigInteger bigInteger = new BigInteger("123456789012345678901234567890");
        assertThat(roundTrip(bigInteger)).isEqualTo(bigInteger);
    }

    @Test
    public void dateRoundTrips() {
        final Date date = new Date(1_700_000_000_000L);
        final Object result = roundTrip(date);
        assertThat(result.getClass()).isEqualTo(Date.class);
        assertThat(result).isEqualTo(date);
    }

    @Test
    public void sqlDateSubclassesAreRejectedRatherThanChangingType() {
        // java.sql.Timestamp would silently round-trip as java.util.Date; it is rejected instead so the
        // caller can skip storing the value.
        assertThatThrownBy(() -> serde.serialize(new java.sql.Timestamp(0L), 0L))
                .isInstanceOf(DebeziumException.class);
    }

    @Test
    public void variableScaleDecimalStructRoundTripsWithSchema() {
        final Struct struct = VariableScaleDecimal.fromLogical(VariableScaleDecimal.schema(), new BigDecimal("42.4242"));
        final Object result = roundTrip(struct);
        assertThat(result).isInstanceOf(Struct.class);
        final Struct resultStruct = (Struct) result;
        assertThat(resultStruct.schema().name()).isEqualTo(VariableScaleDecimal.LOGICAL_NAME);
        assertThat(VariableScaleDecimal.toLogical(resultStruct).getDecimalValue())
                .isEqualTo(VariableScaleDecimal.toLogical(struct).getDecimalValue());
    }

    @Test
    public void nestedListsAndMapsRoundTrip() {
        final List<Object> list = List.of(1, "two", List.of(3L));
        assertThat(roundTrip(list)).isEqualTo(list);

        final Map<Object, Object> map = Map.of("a", 1, "b", Map.of("c", 2L));
        assertThat(roundTrip(map)).isEqualTo(map);
    }

    @Test
    public void unsupportedTypeThrows() {
        assertThatThrownBy(() -> serde.serialize(new Object(), 0L))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("Unsupported");
    }

    @Test
    public void unknownFormatVersionThrows() {
        final byte[] bytes = serde.serialize("x", 0L);
        bytes[0] = 99;
        assertThatThrownBy(() -> serde.deserialize(bytes))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("version");
    }

    @Test
    public void truncatedBytesThrow() {
        final byte[] bytes = serde.serialize("hello world", 0L);
        final byte[] truncated = new byte[bytes.length - 5];
        System.arraycopy(bytes, 0, truncated, 0, truncated.length);
        assertThatThrownBy(() -> serde.deserialize(truncated)).isInstanceOf(DebeziumException.class);
    }
}