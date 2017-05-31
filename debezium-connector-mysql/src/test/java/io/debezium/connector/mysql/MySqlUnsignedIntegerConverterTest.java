/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import org.junit.Test;

import java.math.BigDecimal;

import static org.junit.Assert.assertEquals;

/**
 * @author Omar Al-Safi
 */
public class MySqlUnsignedIntegerConverterTest {

    @Test
    public void shouldConvertSignedBinlogTinyintToUnsigned(){
        assertEquals((short) 255, (short) MySqlUnsignedIntegerConverter.convertUnsignedTinyint((short) -1));
        assertEquals((short) 255, (short) MySqlUnsignedIntegerConverter.convertUnsignedTinyint((short)255));
    }

    @Test
    public void shouldConvertSignedBinlogSmallintToUnsigned(){
        assertEquals(new Integer(65535), MySqlUnsignedIntegerConverter.convertUnsignedSmallint(-1));
        assertEquals(new Integer(65535), MySqlUnsignedIntegerConverter.convertUnsignedSmallint(65535));
    }

    @Test
    public void shouldConvertSignedBinlogMediumintToUnsigned(){
        assertEquals(new Integer(16777215), MySqlUnsignedIntegerConverter.convertUnsignedMediumint(-1));
        assertEquals(new Integer(16777215), MySqlUnsignedIntegerConverter.convertUnsignedMediumint(16777215));
    }

    @Test
    public void shouldConvertSignedBinlogIntToUnsigned(){
        assertEquals(new Long(4294967295L), MySqlUnsignedIntegerConverter.convertUnsignedInteger(-1L));
        assertEquals(new Long(4294967295L), MySqlUnsignedIntegerConverter.convertUnsignedInteger(4294967295L));
    }

    @Test
    public void shouldConvertSignedBinlogBigintToUnsigned(){
        assertEquals(new BigDecimal("18446744073709551615"), MySqlUnsignedIntegerConverter.convertUnsignedBigint(new BigDecimal("-1")));
        assertEquals(new BigDecimal("18446744073709551615"), MySqlUnsignedIntegerConverter.convertUnsignedBigint(new BigDecimal("18446744073709551615")));
    }

}