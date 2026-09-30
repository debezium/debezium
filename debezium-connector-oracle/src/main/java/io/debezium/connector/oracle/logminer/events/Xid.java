/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.events;

import java.util.HexFormat;

public final class Xid {
    private static final HexFormat HEX_FORMAT = HexFormat.of();

    public static final long EMPTY_XID = 0xffffffffffffffffL;
    public static final int EMPTY_SQN = 0xffffffff;

    private Xid() {
    }

    public static long of(byte[] xid) {
        if (xid == null) {
            return EMPTY_XID;
        }

        return (long) xid[0] << 56
                | (xid[1] & 0xFFL) << 48
                | (xid[2] & 0xFFL) << 40
                | (xid[3] & 0xFFL) << 32
                | (xid[4] & 0xFFL) << 24
                | (xid[5] & 0xFFL) << 16
                | (xid[6] & 0xFFL) << 8
                | xid[7] & 0xFFL;
    }

    public static long of(String transactionId) {
        return transactionId == null ? EMPTY_XID : HexFormat.fromHexDigitsToLong(transactionId, 0, 16);
    }

    public static String transactionId(long xid) {
        return xid == EMPTY_XID ? null : HEX_FORMAT.toHexDigits(xid);
    }

    public static String transactionId(byte[] xid) {
        return xid == null ? null : HEX_FORMAT.formatHex(xid, 0, 8);
    }

    public static long of(long key) {
        return key * 0xf1de83e19937733dL;
    }

    public static long key(long xid) {
        return xid * 0x9e3779b97f4a7c15L;
    }

    public static int usnSltKey(long xid) {
        return (int) (xid >>> 32) * 0x9e3779b9;
    }
}
