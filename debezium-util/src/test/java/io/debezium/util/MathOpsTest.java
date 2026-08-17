/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.math.BigInteger;

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
}
