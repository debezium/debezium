/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.junit.jupiter.api.Test;

import io.debezium.data.Uuid;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.doc.FixFor;
import io.debezium.time.MicroTimestamp;

/**
 * Unit tests for {@link UnchangedToastedPlaceholder#getArrayValue(Schema)}. The element schemas stand in for
 * the ones the connector derives under the different handling modes.
 */
public class UnchangedToastedPlaceholderTest {

    // 0x00 and a negative byte, both possible with a hex encoded placeholder
    private final UnchangedToastedPlaceholder placeholder = new UnchangedToastedPlaceholder(new PostgresConnectorConfig(
            TestHelper.defaultConfig().with(PostgresConnectorConfig.UNAVAILABLE_VALUE_PLACEHOLDER, "hex:5f00ff").build()));

    @Test
    @FixFor("debezium/dbz#2418")
    public void shouldExpressNumericPlaceholderPerByte() {
        assertThat(arrayValue(Schema.OPTIONAL_INT16_SCHEMA)).containsExactly((short) 95, (short) 0, (short) -1);
        assertThat(arrayValue(Schema.OPTIONAL_INT32_SCHEMA)).containsExactly(95, 0, -1);
        assertThat(arrayValue(MicroTimestamp.builder().optional().build())).containsExactly(95L, 0L, -1L);
        assertThat(arrayValue(Schema.OPTIONAL_FLOAT32_SCHEMA)).containsExactly(95f, 0f, -1f);
        // decimal.handling.mode=double
        assertThat(arrayValue(Schema.OPTIONAL_FLOAT64_SCHEMA)).containsExactly(95.0, 0.0, -1.0);
        // decimal.handling.mode=precise, scaled and variable scale decimals hold the same numeric value
        assertThat(arrayValue(Decimal.builder(2).parameter(TestHelper.PRECISION_PARAMETER_KEY, "4").optional().build()))
                .containsExactly(new BigDecimal("95.00"), new BigDecimal("0.00"), new BigDecimal("-1.00"));
        final Schema variableScale = VariableScaleDecimal.optionalSchema();
        assertThat(arrayValue(variableScale)).containsExactly(
                VariableScaleDecimal.fromLogical(variableScale, BigDecimal.valueOf(95)),
                VariableScaleDecimal.fromLogical(variableScale, BigDecimal.ZERO),
                VariableScaleDecimal.fromLogical(variableScale, BigDecimal.valueOf(-1)));
    }

    @Test
    @FixFor("debezium/dbz#2418")
    public void shouldExpressPlaceholderAsSingleElement() {
        // bytea[] with binary.handling.mode=bytes
        assertThat(arrayValue(Schema.OPTIONAL_BYTES_SCHEMA)).singleElement()
                .isEqualTo(new byte[]{ 0x5f, 0x00, (byte) 0xff });
        // bytea[] with binary.handling.mode=base64/hex, numeric[] with decimal.handling.mode=string
        assertThat(arrayValue(Schema.OPTIONAL_STRING_SCHEMA)).containsExactly(placeholder.getToastPlaceholderString());
        assertThat(arrayValue(Uuid.builder().optional().build()))
                .containsExactly(UUID.nameUUIDFromBytes(new byte[]{ 0x5f, 0x00, (byte) 0xff }).toString());
    }

    @Test
    @FixFor("debezium/dbz#2418")
    public void shouldHaveNoPlaceholderWhenElementCannotExpressIt() {
        assertThat(placeholder.getArrayValue(Schema.OPTIONAL_BOOLEAN_SCHEMA)).isEmpty();
        // hstore[] with hstore.handling.mode=map
        assertThat(placeholder.getArrayValue(SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA).optional().build())).isEmpty();
        // numeric(3,2) cannot hold 95.00
        assertThat(placeholder.getArrayValue(Decimal.builder(2).parameter(TestHelper.PRECISION_PARAMETER_KEY, "3").optional().build())).isEmpty();
        // time.precision.mode=connect
        assertThat(placeholder.getArrayValue(Date.builder().optional().build())).isEmpty();
        assertThat(placeholder.getArrayValue(Time.builder().optional().build())).isEmpty();
        assertThat(placeholder.getArrayValue(Timestamp.builder().optional().build())).isEmpty();
    }

    private List<Object> arrayValue(Schema elementSchema) {
        final List<Object> value = placeholder.getArrayValue(elementSchema).orElseThrow();
        // the event carries the value against the array schema, so it has to validate against it
        ConnectSchema.validateValue(SchemaBuilder.array(elementSchema).optional().build(), value);
        return value;
    }
}
