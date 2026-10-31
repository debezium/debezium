/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.events;

/**
 * Packs a LogMiner {@code RS_ID} into primitives. The value is a fixed-format string of three lowercase
 * hexadecimal segments of 6, 8 and 4 digits, such as {@code 0x000abc.00000def.0010}: the first two segments
 * are 56 bits and fit one {@code long}, the third is 16 bits and fits a {@code short}. A value in any other
 * form is not packed and the caller keeps the string.
 *
 * @author Chris Cranford
 */
public final class RsIdCodec {

    /**
     * The two halves of a packed {@code RS_ID}.
     *
     * @param blockSequence the first two segments, the block in the upper 24 bits and the sequence in the lower 32
     * @param offset the third segment
     */
    public record Packed(long blockSequence, short offset) {
    }

    private static final int LENGTH = 22;
    private static final char[] HEX = "0123456789abcdef".toCharArray();

    private RsIdCodec() {
    }

    /**
     * @param rsId the {@code RS_ID} as LogMiner reports it, may be {@code null}
     * @return the packed value, or {@code null} when the string is not in the packed form
     */
    public static Packed encode(String rsId) {
        if (rsId == null || rsId.length() != LENGTH || rsId.charAt(0) != '0' || rsId.charAt(1) != 'x'
                || rsId.charAt(8) != '.' || rsId.charAt(17) != '.') {
            return null;
        }

        final long block = parseHex(rsId, 2, 8);
        final long sequence = parseHex(rsId, 9, 17);
        final long offset = parseHex(rsId, 18, 22);
        if (block < 0 || sequence < 0 || offset < 0) {
            return null;
        }

        return new Packed((block << 32) | sequence, (short) offset);
    }

    /**
     * @return the {@code RS_ID} string the packed value was encoded from
     */
    public static String decode(long blockSequence, short offset) {
        final char[] out = new char[LENGTH];
        out[0] = '0';
        out[1] = 'x';
        writeHex(out, 2, 6, blockSequence >>> 32);
        out[8] = '.';
        writeHex(out, 9, 8, blockSequence & 0xFFFFFFFFL);
        out[17] = '.';
        writeHex(out, 18, 4, offset & 0xFFFFL);
        return new String(out);
    }

    private static long parseHex(String value, int from, int to) {
        long result = 0;
        for (int i = from; i < to; i++) {
            final char c = value.charAt(i);
            final int digit;
            if (c >= '0' && c <= '9') {
                digit = c - '0';
            }
            else if (c >= 'a' && c <= 'f') {
                digit = c - 'a' + 10;
            }
            else {
                return -1;
            }
            result = (result << 4) | digit;
        }
        return result;
    }

    private static void writeHex(char[] out, int from, int digits, long value) {
        for (int i = digits - 1; i >= 0; i--) {
            out[from + i] = HEX[(int) (value & 0xF)];
            value >>>= 4;
        }
    }
}
