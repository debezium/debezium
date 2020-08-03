package io.debezium.jdbc;

import java.util.Optional;

import junit.framework.TestCase;

public class ConverterHelperTest extends TestCase {

    public void testNumsToDoubleOfNull() {
        Float input = null;
        assertEquals(ConverterHelper.numsToDouble(input), Optional.empty());
    }

    public void testNumsToDoubleOfVariousFloats() {
        float input = 0.213f;
        assertEquals(ConverterHelper.numsToDouble(input), Optional.of(0.213d));
    }
}
