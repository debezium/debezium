/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;

import org.junit.Test;

/**
 * @author Omar Al-Safi
 */
public class BinlogUnsignedIntegerConverterTest {

    @Test
    public void shouldConvertSignedBinlogTinyintToUnsigned() {
        assertEquals((short) 255, BinlogUnsignedIntegerConverter.convertUnsignedTinyint((short) -1));
        assertEquals((short) 255, BinlogUnsignedIntegerConverter.convertUnsignedTinyint((short) 255));
    }

    @Test
    public void shouldConvertSignedBinlogSmallintToUnsigned() {
        assertEquals(65535, BinlogUnsignedIntegerConverter.convertUnsignedSmallint(-1));
        assertEquals(65535, BinlogUnsignedIntegerConverter.convertUnsignedSmallint(65535));
    }

    @Test
    public void shouldConvertSignedBinlogMediumintToUnsigned() {
        assertEquals(16777215, BinlogUnsignedIntegerConverter.convertUnsignedMediumint(-1));
        assertEquals(16777215, BinlogUnsignedIntegerConverter.convertUnsignedMediumint(16777215));
    }

    @Test
    public void shouldConvertSignedBinlogIntToUnsigned() {
        assertEquals(4294967295L, BinlogUnsignedIntegerConverter.convertUnsignedInteger(-1L));
        assertEquals(4294967295L, BinlogUnsignedIntegerConverter.convertUnsignedInteger(4294967295L));
    }

    @Test
    public void shouldConvertSignedBinlogBigintToUnsigned() {
        assertEquals(new BigDecimal("18446744073709551615"), BinlogUnsignedIntegerConverter.convertUnsignedBigint(new BigDecimal("-1")));
        assertEquals(new BigDecimal("18446744073709551615"), BinlogUnsignedIntegerConverter.convertUnsignedBigint(new BigDecimal("18446744073709551615")));
    }

}
