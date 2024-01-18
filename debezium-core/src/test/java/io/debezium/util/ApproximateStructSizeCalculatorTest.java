/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Collections;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

/**
 * Unit test for {@link ApproximateStructSizeCalculator}.
 */
public class ApproximateStructSizeCalculatorTest {

    @Test
    public void testGetApproximateRecordSizeWithBytesStruct() {
        // test BigDecimal
        Schema valueSchema = SchemaBuilder.struct().field("dec", Decimal.builder(10).build()).build();
        SourceRecord sourceRecord = new SourceRecord(Collections.emptyMap(), Collections.emptyMap(), "dummy",
                valueSchema, new Struct(valueSchema).put("dec", new BigDecimal("10099999.29")));
        long actual = ApproximateStructSizeCalculator.getApproximateRecordSize(sourceRecord);
        assertEquals(actual, 105);

        // test ByteBuffer
        valueSchema = SchemaBuilder.struct().field("bf", SchemaBuilder.bytes().build()).build();
        sourceRecord = new SourceRecord(Collections.emptyMap(), Collections.emptyMap(), "dummy",
                valueSchema, new Struct(valueSchema).put("bf", ByteBuffer.wrap("hello debezium".getBytes())));
        actual = ApproximateStructSizeCalculator.getApproximateRecordSize(sourceRecord);
        assertEquals(actual, 115);
    }
}
