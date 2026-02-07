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

    private static final long OVERFLOW_MARKER = Long.MIN_VALUE;

    public static final Scn MAX = new Scn(-2L, null);
    public static final Scn NULL = new Scn(0L, null, true);
    public static final Scn ONE = new Scn(1L, null);

    private final long longValue;
    private final BigInteger bigIntegerValue;
    private final boolean empty;

    public Scn(BigInteger scn) {
        if (scn == null) {
            this.longValue = 0L;
            this.bigIntegerValue = null;
            this.empty = true;
        }
        else if (scn.bitLength() < 64) {
            this.longValue = scn.longValue();
            this.bigIntegerValue = null;
            this.empty = false;
        }
        else {
            this.longValue = OVERFLOW_MARKER;
            this.bigIntegerValue = scn;
            this.empty = false;
        }
    }

    private Scn(long longValue, BigInteger bigIntegerValue) {
        this(longValue, bigIntegerValue, false);
    }

    private Scn(long longValue, BigInteger bigIntegerValue, boolean nullValue) {
        this.longValue = longValue;
        this.bigIntegerValue = bigIntegerValue;
        this.empty = nullValue;
    }

    /**
     * Returns whether this {@link Scn} is null and contains no value.
     */
    public boolean isNull() {
        return empty;
    }

    /**
     * Construct a {@link Scn} from an integer value.
     *
     * @param value integer value
     * @return instance of Scn
     */
    public static Scn valueOf(int value) {
        return new Scn(value, null);
    }

    /**
     * Construct a {@link Scn} from a long value.
     *
     * @param value long value
     * @return instance of Scn
     */
    public static Scn valueOf(long value) {
        return new Scn(value, null);
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
        if (empty) {
            return 0L;
        }
        return bigIntegerValue != null ? bigIntegerValue.longValue() : longValue;
    }

    public BigInteger asBigInteger() {
        if (empty) {
            return null;
        }
        return bigIntegerValue != null ? bigIntegerValue : BigInteger.valueOf(longValue);
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
            return new Scn(this.longValue, this.bigIntegerValue);
        }
        else if (isNull()) {
            return new Scn(value.longValue, value.bigIntegerValue);
        }

        // If either use BigInteger, compute with BigInteger
        if (this.bigIntegerValue != null && value.bigIntegerValue != null) {
            return new Scn(this.asBigInteger().add(value.asBigInteger()));
        }

        // Both are longs - check overflow
        try {
            long result = Math.addExact(this.longValue, value.longValue);
            return new Scn(result, null);
        }
        catch (ArithmeticException e) {
            // Overflow - use BigInteger
            return new Scn(BigInteger.valueOf(this.longValue).add(BigInteger.valueOf(value.longValue)));
        }
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
            return new Scn(this.longValue, this.bigIntegerValue);
        }
        else if (isNull()) {
            if (value.bigIntegerValue != null) {
                return new Scn(value.asBigInteger().negate());
            }
            try {
                long result = Math.negateExact(value.longValue);
                return new Scn(result, null);
            }
            catch (ArithmeticException e) {
                return new Scn(BigInteger.valueOf(value.longValue).negate());
            }
        }

        // If either uses BigInteger, compute with BigInteger
        if (this.bigIntegerValue != null || value.bigIntegerValue != null) {
            return new Scn(this.asBigInteger().subtract(value.asBigInteger()));
        }

        // Both are longs - check overflow
        try {
            long result = Math.subtractExact(this.longValue, value.longValue);
            return new Scn(result, null);
        }
        catch (ArithmeticException e) {
            // Overflow - use BigInteger
            return new Scn(BigInteger.valueOf(this.longValue).subtract(BigInteger.valueOf(value.longValue)));
        }
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

        if (this.bigIntegerValue != null || o.bigIntegerValue != null) {
            return this.asBigInteger().compareTo(o.asBigInteger());
        }

        return Long.compare(this.longValue, o.longValue);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final Scn other = (Scn) o;
        if (this.empty != other.empty) {
            return false;
        }
        if (this.empty) {
            return true;
        }

        if (this.bigIntegerValue != null || other.bigIntegerValue != null) {
            return Objects.equals(this.bigIntegerValue, other.bigIntegerValue);
        }

        return this.longValue == other.longValue;
    }

    @Override
    public int hashCode() {
        if (empty) {
            return 0;
        }
        if (bigIntegerValue != null) {
            return bigIntegerValue.hashCode();
        }
        return Long.hashCode(longValue);
    }

    @Override
    public String toString() {
        if (empty) {
            return "null";
        }
        if (bigIntegerValue != null) {
            return bigIntegerValue.toString();
        }
        return Long.toString(longValue);
    }
}
