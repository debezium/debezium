/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;

/**
 * Unit test for {@link MathOps}, exercising the mixed-type {@code add} overloads through the
 * {@code add(Number, Number)} entry point that {@code Document.increment} relies on.
 *
 * @author Debezium Authors
 */
public class MathOpsTest {

    // Regression: the BigInteger + BigInteger overload used to drop the first operand and double the
    // second (second.add(second)), so an increment returned 2 * increment instead of current + increment.
    @Test
    @FixFor("debezium/dbz#2446")
    public void addsTwoBigIntegers() {
        Number current = BigInteger.valueOf(100);
        Number increment = BigInteger.valueOf(5);
        assertThat(MathOps.add(current, increment)).isEqualTo(BigInteger.valueOf(105));
    }

    // Regression: the BigDecimal + BigInteger overload had the same second.add(second) defect.
    @Test
    @FixFor("debezium/dbz#2446")
    public void addsBigDecimalAndBigInteger() {
        Number current = new BigDecimal("100");
        Number increment = BigInteger.valueOf(5);
        assertThat(MathOps.add(current, increment)).isEqualTo(new BigDecimal("105"));
    }

    // The mirror overload (BigInteger + BigDecimal) was already correct; guard it against regressing.
    @Test
    @FixFor("debezium/dbz#2446")
    public void addsBigIntegerAndBigDecimal() {
        Number current = BigInteger.valueOf(100);
        Number increment = new BigDecimal("5");
        assertThat(MathOps.add(current, increment)).isEqualTo(new BigDecimal("105"));
    }

    // A negative operand must decrement, matching Document.increment's documented contract.
    @Test
    @FixFor("debezium/dbz#2446")
    public void addingNegativeBigIntegerDecrements() {
        Number current = BigInteger.valueOf(100);
        Number increment = BigInteger.valueOf(-30);
        assertThat(MathOps.add(current, increment)).isEqualTo(BigInteger.valueOf(70));
    }

    // Regression: double sums were narrowed to Float whenever they fell within the positive float range,
    // and a Double first operand was truncated with floatValue() before adding.
    @Test
    @FixFor("debezium/dbz#2923")
    public void addsTwoDoublesWithoutLosingPrecision() {
        assertThat(MathOps.add((Number) 1.123456789d, (Number) 0.0d)).isEqualTo(1.123456789d);
        assertThat(MathOps.add((Number) 0.1d, (Number) 0.2d)).isEqualTo(0.1d + 0.2d);
    }

    @Test
    @FixFor("debezium/dbz#2923")
    public void addingDoubleToIntegralOrFloatReturnsDouble() {
        assertThat(MathOps.add((Number) 1, (Number) 0.1d)).isEqualTo(1 + 0.1d);
        assertThat(MathOps.add((Number) (short) 1, (Number) 0.1d)).isEqualTo(1 + 0.1d);
        assertThat(MathOps.add((Number) 1L, (Number) 0.1d)).isEqualTo(1L + 0.1d);
        assertThat(MathOps.add((Number) 1.5f, (Number) 0.1d)).isEqualTo(1.5d + 0.1d);
        assertThat(MathOps.add((Number) 1.123456789d, (Number) 1)).isEqualTo(1.123456789d + 1);
        assertThat(MathOps.add((Number) 1.123456789d, (Number) 0.5f)).isEqualTo(1.123456789d + 0.5d);
    }

    // The result type must not depend on the sign of the sum.
    @Test
    @FixFor("debezium/dbz#2923")
    public void resultTypeDoesNotDependOnSign() {
        assertThat(MathOps.add((Number) (-1.123456789d), (Number) 0.0d)).isEqualTo(-1.123456789d);
        assertThat(MathOps.add((Number) 1.5f, (Number) 1.0f)).isEqualTo(2.5f);
        assertThat(MathOps.add((Number) (-1.5f), (Number) (-1.0f))).isEqualTo(-2.5f);
        assertThat(MathOps.add((Number) (-1.5f), (Number) 1)).isEqualTo(-0.5f);
    }

    @Test
    @FixFor("debezium/dbz#2923")
    public void floatSumOutsideFloatRangeIsReturnedAsDouble() {
        assertThat(MathOps.add((Number) Float.MAX_VALUE, (Number) Float.MAX_VALUE)).isEqualTo(2.0d * Float.MAX_VALUE);
        assertThat(MathOps.add((Number) (-Float.MAX_VALUE), (Number) (-Float.MAX_VALUE))).isEqualTo(-2.0d * Float.MAX_VALUE);
    }

    // Regression: integral sums were computed in a plain long, so a sum past Long.MAX_VALUE or below
    // Long.MIN_VALUE silently wrapped around to the opposite sign instead of widening to BigInteger.
    @Test
    @FixFor("debezium/dbz#2934")
    public void longSumOutsideLongRangeIsReturnedAsBigInteger() {
        final BigInteger longMax = BigInteger.valueOf(Long.MAX_VALUE);
        final BigInteger longMin = BigInteger.valueOf(Long.MIN_VALUE);
        assertThat(MathOps.add((Number) Long.MAX_VALUE, (Number) 1L)).isEqualTo(longMax.add(BigInteger.ONE));
        assertThat(MathOps.add((Number) Long.MAX_VALUE, (Number) Long.MAX_VALUE)).isEqualTo(longMax.add(longMax));
        assertThat(MathOps.add((Number) Long.MIN_VALUE, (Number) (-1L))).isEqualTo(longMin.subtract(BigInteger.ONE));
        assertThat(MathOps.add((Number) Long.MAX_VALUE, (Number) 1)).isEqualTo(longMax.add(BigInteger.ONE));
        assertThat(MathOps.add((Number) Long.MAX_VALUE, (Number) (short) 1)).isEqualTo(longMax.add(BigInteger.ONE));
        assertThat(MathOps.add((Number) 1, (Number) Long.MAX_VALUE)).isEqualTo(longMax.add(BigInteger.ONE));
        assertThat(MathOps.add((Number) (short) -1, (Number) Long.MIN_VALUE)).isEqualTo(longMin.subtract(BigInteger.ONE));
        assertThat(MathOps.add((Number) new AtomicLong(Long.MAX_VALUE), (Number) 1L)).isEqualTo(longMax.add(BigInteger.ONE));
    }

    // Sums that still fit keep the existing narrowing behavior.
    @Test
    @FixFor("debezium/dbz#2934")
    public void longSumWithinLongRangeIsNotWidened() {
        assertThat(MathOps.add((Number) Long.MAX_VALUE, (Number) 0L)).isEqualTo(Long.MAX_VALUE);
        assertThat(MathOps.add((Number) Long.MAX_VALUE, (Number) Long.MIN_VALUE)).isEqualTo((short) -1);
        assertThat(MathOps.add((Number) Integer.MAX_VALUE, (Number) 1)).isEqualTo((long) Integer.MAX_VALUE + 1);
    }
}
