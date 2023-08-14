/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.math.BigInteger;
import java.util.Objects;

/**
 * Oracle System Change Number implementation
 *
 * @author Chris Cranford
 */
public class Scn implements Comparable<Scn> {

    /**
     * Represents an Scn that implies the maximum possible value of an SCN, useful as a placeholder.
     */
    public static final Scn MAX = new Scn(BigInteger.valueOf(-2));

    /**
     * Represents an Scn without a value.
     */
    public static final Scn NULL = new Scn(null);

    /**
     * Represents an Scn with value 1, useful for playing with inclusive/exclusive query boundaries.
     */
    public static final Scn ONE = new Scn(BigInteger.valueOf(1));

    private final BigInteger scn;

    public Scn(BigInteger scn) {
        this.scn = scn;
    }

    /**
     * Returns whether this {@link Scn} is null and contains no value.
     */
    public boolean isNull() {
        return this.scn == null;
    }

    /**
     * Construct a {@link Scn} from an integer value.
     *
     * @param value integer value
     * @return instance of Scn
     */
    public static Scn valueOf(int value) {
        return new Scn(BigInteger.valueOf(value));
    }

    /**
     * Construct a {@link Scn} from a long value.
     *
     * @param value long value
     * @return instance of Scn
     */
    public static Scn valueOf(long value) {
        return new Scn(BigInteger.valueOf(value));
    }

    /**
     * Construct a {@link Scn} from a string value.
     *
     * @param value string value, should not be null
     * @return instance of Scn
     */
    public static Scn valueOf(String value) {
        return new Scn(new BigInteger(value));
    }

    /**
     * Get the Scn represented as a {@code long} data type.
     */
    public long longValue() {
        return isNull() ? 0 : scn.longValue();
    }

    public BigInteger asBigInteger() {
        return scn;
    }

    /**
     * Returns a {@code SCn} whose value is {@code (this + value)}.
     *
     * @param value the value to be added to this {@code Scn}.
     * @return {@code this + value}
     */
    public Scn add(Scn value) {
        if (isNull() && value.isNull()) {
            return Scn.NULL;
        }
        else if (value.isNull()) {
            return new Scn(scn);
        }
        else if (isNull()) {
            return new Scn(value.scn);
        }
        return new Scn(scn.add(value.scn));
    }

    /**
     * Returns a {@code Scn} whose value is {@code (this - value)}.
     *
     * @param value the value to be subtracted from this {@code Scn}.
     * @return {@code this - value}
     */
    public Scn subtract(Scn value) {
        if (isNull() && value.isNull()) {
            return Scn.NULL;
        }
        else if (value.isNull()) {
            return new Scn(scn);
        }
        else if (isNull()) {
            return new Scn(value.scn.negate());
        }
        return new Scn(scn.subtract(value.scn));
    }

    /**
     * Compares this {@code Scn} with the specified {@code Scn}.
     *
     * @param o {@code Scn} to which this {@code Scn} is to be compared
     * @return -1, 0, or 1 as this {code Scn} is numerically less than, equal to, or greater than {@code o}.
     */
    @Override
    public int compareTo(Scn o) {
        if (isNull() && o.isNull()) {
            return 0;
        }
        else if (isNull() && !o.isNull()) {
            return -1;
        }
        else if (!isNull() && o.isNull()) {
            return 1;
        }
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
        return isNull() ? "null" : scn.toString();
    }
}
