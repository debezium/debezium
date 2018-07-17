/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.util.Arrays;

import io.debezium.util.Strings;

/**
 * A logical representation of SQL Server LSN (log sequence number) position.
 *
 * @author Jiri Pechanec
 *
 */
public class Lsn implements Comparable<Lsn> {
    public static final Lsn NULL = new Lsn(null); 

    private final byte[] binary;
    private int[] unsignedBinary;

    private String string; 

    public Lsn(byte[] binary) {
        this.binary = binary;
    }

    public byte[] getBinary() {
        return binary;
    }

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

    public String toString() {
        if (string != null) {
            return string;
        }
        final StringBuilder sb = new StringBuilder();
        if (binary == null) {
            return "NULL";
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

    public static Lsn valueOf(String lsnString) {
        return (lsnString == null) ? NULL : new Lsn(Strings.hexStringToByteArray(lsnString.replace(":", "")));
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
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Lsn other = (Lsn) obj;
        if (!Arrays.equals(binary, other.binary))
            return false;
        return true;
    }

    @Override
    public int compareTo(Lsn o) {
        if (this == o) {
            return 0;
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
}
