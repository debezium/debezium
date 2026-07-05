/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.events;

/**
 * A specialized codec for Oracle {@code ROWID} values, packing the 18-character
 * base-64 string (108 bits) losslessly into a {@link Packed} record of two {@code long} fields.
 *
 * @author Chris Cranford
 */
public class RowIdCodec {

    /** Lossless 108-bit container for an Oracle ROWID. */
    public record Packed(long high, long low) {
    }

    // Oracle ROWID base64 alphabet
    private static final String BASE64_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    private static final int[] BASE64_VALUES = new int[128];

    static {
        for (int i = 0; i < BASE64_CHARS.length(); i++) {
            BASE64_VALUES[BASE64_CHARS.charAt(i)] = i;
        }
    }

    public static final Packed EMPTY_ROW_ID = RowIdCodec.encode("AAAAAAAAAAAAAAAAAA");

    private RowIdCodec() {
    }

    /**
     * Encodes an Oracle ROWID string into a lossless {@link Packed} representation.
     *
     * @param rowId the 18-character Oracle ROWID string
     * @return the packed representation holding all 108 bits across two {@code long} fields
     */
    public static Packed encode(String rowId) {
        if (rowId == null || rowId.length() != 18) {
            throw new IllegalArgumentException("Invalid ROWID length: " + rowId);
        }

        long high = 0, low = 0;

        // Pack each character's 6-bit value into the high/low register pair.
        // Bits that would overflow the top of low are captured first and carried into high.
        for (int i = 0; i < 18; i++) {
            char c = rowId.charAt(i);
            if (c >= 128) {
                throw new IllegalArgumentException("Invalid ROWID character: " + c);
            }

            long overflow = low >>> 58; // capture the 6 bits about to leave low
            low = (low << 6) | BASE64_VALUES[c];
            high = (high << 6) | overflow;

        }

        return new Packed(high, low);
    }

    /**
     * Decodes a {@link Packed} representation back to the original Oracle ROWID string.
     *
     * @param packed the packed representation returned by {@link #encode}
     * @return the original 18-character Oracle ROWID string
     */
    public static String decode(Packed packed) {
        char[] chars = new char[18];
        long high = packed.high, low = packed.low;

        // Extract 6 bits at a time from the right, working right to left.
        for (int i = 17; i >= 0; i--) {
            int value = (int) (low & 0x3F); // lowest 6 bits of low
            chars[i] = BASE64_CHARS.charAt(value);
            low >>>= 6; // discard the 6 bits just consumed
            low |= (high & 0x3F) << 58; // borrow the bottom 6 bits of high into the top of low
            high >>>= 6; // discard the 6 bits just borrowed
        }

        return new String(chars);
    }

}
