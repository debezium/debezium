/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.math.BigDecimal;
import java.util.Objects;

/**
 * Oracle System Change Number implementation
 *
 * @author Chris Cranford
 */
public class Scn implements Comparable<Scn> {

    /**
     * Represents an Scn that is considered INVALID, useful as a placeholder.
     */
    public static final Scn INVALID = new Scn(new BigDecimal(-1));

    /**
     * Represents an Scn that implies the maximum possible value of an SCN, useful as a placeholder.
     */
    public static final Scn MAX = new Scn(new BigDecimal(-2));

    /**
     * Represents an Scn with a value of {@code 0}.
     */
    public static final Scn ZERO = new Scn(BigDecimal.ZERO);

    private final BigDecimal scn;

    public Scn(BigDecimal scn) {
        assert scn.scale() == 0;
        this.scn = scn;
    }

    /**
     * Construct a {@link Scn} from an integer value.
     *
     * @param value integer value
     * @return instance of Scn
     */
    public static Scn valueOf(int value) {
        return new Scn(new BigDecimal(value));
    }

    /**
     * Construct a {@link Scn} from a long value.
     *
     * @param value long value
     * @return instance of Scn
     */
    public static Scn valueOf(long value) {
        return new Scn(new BigDecimal(value));
    }

    /**
     * Construct a {@link Scn} from a string value.
     *
     * @param value string value
     * @return instance of Scn
     */
    public static Scn valueOf(String value) {
        return new Scn(new BigDecimal(value));
    }

    /**
     * Get the Scn represented as a {@code long} data type.
     */
    public long longValue() {
        return scn.longValue();
    }

    /**
     * Returns a {@code SCn} whose value is {@code (this + value)}.
     *
     * @param value the value to be added to this {@code Scn}.
     * @return {@code this + value}
     */
    public Scn add(Scn value) {
        return new Scn(scn.add(value.scn));
    }

    /**
     * Returns a {@code Scn} whose value is {@code (this - value)}.
     *
     * @param value the value to be subtracted from this {@code Scn}.
     * @return {@code this - value}
     */
    public Scn subtract(Scn value) {
        return new Scn(scn.subtract(value.scn));
    }

    /**
     * Compares this {@code SCn} with the specified {@code Scn}.
     *
     * @param o {@code Scn} to which this {@code Scn} is to be compared
     * @return -1, 0, or 1 as this {code Scn} is numerically less than, equal to, or greater than {@code o}.
     */
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
        return scn.toString();
    }
}
