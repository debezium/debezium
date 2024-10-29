/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.util.Arrays;

import io.debezium.connector.Nullable;
import io.debezium.util.Strings;

/**
 * A logical representation of SQL Server LSN (log sequence number) position. When LSN is not available
 * it is replaced with {@link Lsn#NULL} constant.
 *
 * @author Jiri Pechanec
 *
 */
public class Lsn implements Comparable<Lsn>, Nullable {
    private static final String NULL_STRING = "NULL";

    public static final Lsn NULL = new Lsn(null);

    public static final Lsn ZERO = valueOf(new byte[10]);

    private final byte[] binary;
    private int[] unsignedBinary;

    private String string;

    private Lsn(byte[] binary) {
        this.binary = binary;
    }

    /**
     * @return binary representation of the stored LSN
     */
    public byte[] getBinary() {
        return binary;
    }

    /**
     * @return true if this is a real LSN or false it it is {@code NULL}
     */
    @Override
    public boolean isAvailable() {
        return binary != null;
    }

    private int[] getUnsignedBinary() {
        if (unsignedBinary != null || binary == null) {
            return unsignedBinary;
        }

        unsignedBinary = new int[binary.length];
        for (int i = 0; i < binary.length; i++) {
            unsignedBinary[i] = Byte.toUnsignedInt(binary[i]);
        }
        return unsignedBinary;
    }

    /**
     * @return textual representation of the stored LSN
     */
    public String toString() {
        if (string != null) {
            return string;
        }
        final StringBuilder sb = new StringBuilder();
        if (binary == null) {
            return NULL_STRING;
        }
        final int[] unsigned = getUnsignedBinary();
        for (int i = 0; i < unsigned.length; i++) {
            final String byteStr = Integer.toHexString(unsigned[i]);
            if (byteStr.length() == 1) {
                sb.append('0');
            }
            sb.append(byteStr);
            if (i == 3 || i == 7) {
                sb.append(':');
            }
        }
        string = sb.toString();
        return string;
    }

    /**
     * @param lsnString - textual representation of Lsn
     * @return LSN converted from its textual representation
     */
    public static Lsn valueOf(String lsnString) {
        return (lsnString == null || NULL_STRING.equals(lsnString)) ? NULL : new Lsn(Strings.hexStringToByteArray(lsnString.replace(":", "")));
    }

    /**
     * @param lsnBinary - binary representation of Lsn
     * @return LSN converted from its binary representation
     */
    public static Lsn valueOf(byte[] lsnBinary) {
        return (lsnBinary == null) ? NULL : new Lsn(lsnBinary);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(binary);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        Lsn other = (Lsn) obj;
        if (!Arrays.equals(binary, other.binary)) {
            return false;
        }
        return true;
    }

    /**
     * Enables ordering of LSNs. The {@code NULL} LSN is always the smallest one.
     */
    @Override
    public int compareTo(Lsn o) {
        if (this == o) {
            return 0;
        }
        if (!this.isAvailable()) {
            if (!o.isAvailable()) {
                return 0;
            }
            return -1;
        }
        if (!o.isAvailable()) {
            return 1;
        }
        final int[] thisU = getUnsignedBinary();
        final int[] thatU = o.getUnsignedBinary();
        for (int i = 0; i < thisU.length; i++) {
            final int diff = thisU[i] - thatU[i];
            if (diff != 0) {
                return diff;
            }
        }
        return 0;
    }

    /**
     * Verifies whether the LSN falls into a LSN interval
     *
     * @param from start of the interval (included)
     * @param to end of the interval (excluded)
     *
     * @return true if the LSN falls into the interval
     */
    public boolean isBetween(Lsn from, Lsn to) {
        return this.compareTo(from) >= 0 && this.compareTo(to) < 0;
    }
}
