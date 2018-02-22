/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;

import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

import io.debezium.data.DebeziumDecimal;
import io.debezium.data.FixedScaleDecimal;
import io.debezium.data.VariableScaleDecimal;

public class CustomTypeEncodingTest {

    @Test
    public void testVariableScaleDecimal() {
        final BigDecimal testValue = new BigDecimal("138.456");
        final Struct struct = VariableScaleDecimal.fromLogical(VariableScaleDecimal.schema(), new DebeziumDecimal(testValue));
        final BigDecimal decodedValue = VariableScaleDecimal.toLogical(struct).getDecimalValue().get();
        assertEquals("Number should be same after serde", testValue, decodedValue);
    }

    @Test
    public void testSpecialVariableScaleDecimal() {
        final DebeziumDecimal testValue = DebeziumDecimal.POSITIVE_INF;
        final Struct struct = VariableScaleDecimal.fromLogical(VariableScaleDecimal.schema(), testValue);
        final DebeziumDecimal decodedValue = VariableScaleDecimal.toLogical(struct);
        assertEquals("Special value should be same after serde", testValue, decodedValue);
    }

    @Test
    public void testFixedScaleDecimal() {
        final BigDecimal testValue = new BigDecimal("138.456");
        final Struct struct = FixedScaleDecimal.fromLogical(FixedScaleDecimal.schema(3), new DebeziumDecimal(testValue));
        final BigDecimal decodedValue = FixedScaleDecimal.toLogical(FixedScaleDecimal.schema(3), struct).getDecimalValue().get();
        assertEquals("Number should be same after serde", testValue, decodedValue);
    }

    @Test
    public void testSpecialFixedScaleDecimal() {
        final DebeziumDecimal testValue = DebeziumDecimal.NOT_A_NUMBER;
        final Struct struct = FixedScaleDecimal.fromLogical(FixedScaleDecimal.schema(3), testValue);
        final DebeziumDecimal decodedValue = FixedScaleDecimal.toLogical(FixedScaleDecimal.schema(3), struct);
        assertEquals("Special value should be same after serde", testValue, decodedValue);
    }
}
