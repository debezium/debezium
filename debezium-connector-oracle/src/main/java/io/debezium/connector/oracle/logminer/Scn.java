/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Objects;

/**
 * Oracle System Change Number implementation
 *
 * @author Chris Cranford
 */
public class Scn implements Comparable<Scn> {

    public static final Scn INVALID = new Scn(new BigDecimal(-1));
    public static final Scn MAX = new Scn(new BigDecimal(-2));
    public static final Scn ZERO = new Scn(BigDecimal.ZERO);
    public static final Scn ONE = new Scn(BigDecimal.ONE);

    private BigDecimal scn;

    public Scn(BigDecimal scn) {
        assert scn.scale() == 0;
        this.scn = scn;
    }

    public static Scn valueOf(Long value) {
        return new Scn(new BigDecimal(value));
    }

    public static Scn valueof(String value) {
        return new Scn(new BigDecimal(new BigInteger(value)));
    }

    public long longValue() {
        return scn.longValue();
    }

    public Scn add(Scn value) {
        return new Scn(scn.add(value.scn));
    }

    @Override
    public int compareTo(Scn o) {
        return scn.compareTo(o.scn);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Scn scn1 = (Scn) o;
        return Objects.equals(scn, scn1.scn);
    }

    @Override
    public int hashCode() {
        return Objects.hash(scn);
    }

    @Override
    public String toString() {
        return scn != null ? scn.toString() : "null";
    }
}
