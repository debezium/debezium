/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.events;

/**
 * A specialized codec for Oracle {@code ROWID} values to pack them into an 8-byte long value.
 *
 * @author Chris Cranford
 */
public class RowIdCodec {

    // Oracle ROWID base64 alphabet
    private static final String BASE64_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    private static final int[] BASE64_VALUES = new int[128];

    static {
        for (int i = 0; i < BASE64_CHARS.length(); i++) {
            BASE64_VALUES[BASE64_CHARS.charAt(i)] = i;
        }
    }

    public static long EMPTY_ROW_ID = RowIdCodec.encode("AAAAAAAAAAAAAAAAAA");

    private RowIdCodec() {
    }

    /**
     * Decode Oracle ROWID string to packed long
     * @param rowId the row id
     * @return the packed 8-byte value
     */
    public static long encode(String rowId) {
        if (rowId == null || rowId.length() != 18) {
            throw new IllegalArgumentException("Invalid ROWID length: " + rowId);
        }

        long result = 0;

        // Decode each character as 6-bit value and pack into long
        for (int i = 0; i < 18; i++) {
            char c = rowId.charAt(i);
            if (c >= 128) {
                throw new IllegalArgumentException("Invalid ROWID character: " + c);
            }

            int value = BASE64_VALUES[c];
            result = (result << 6) | value;
        }

        return result;
    }

    /**
     * Encode packed long back to Oracle ROWID string
     * @param packed the packed 8-byte value.
     * @return the decoded ROWID string
     */
    public static String decode(long packed) {
        char[] chars = new char[18];

        // Extract 6 bits at a time from right to left
        for (int i = 17; i >= 0; i--) {
            int value = (int) (packed & 0x3F); // Get lowest 6 bits
            chars[i] = BASE64_CHARS.charAt(value);
            packed >>>= 6; // Unsigned right shift by 6 bits
        }

        return new String(chars);
    }

}
