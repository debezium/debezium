/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection.pgoutput;

public enum Float16 {
    ;
    private static final int SIGN_MASK = 0x8000;
    private static final int EXPONENT_SHIFT = 10;
    private static final int SHIFTED_EXPONENT_MASK = 0x1f;
    private static final int SIGNIFICAND_MASK = 0x3ff;
    private static final int EXPONENT_BIAS = 15;

    private static final int FP32_DENORMAL_MAGIC = 126 << 23;
    private static final float FP32_DENORMAL_FLOAT = Float.intBitsToFloat(FP32_DENORMAL_MAGIC);
    private static final int FP32_QNAN_MASK = 0x400000;
    private static final int FP32_EXPONENT_BIAS = 127;
    private static final int FP32_EXPONENT_SHIFT = 23;

    /**
     * Convert float16 (half-precision) to float32.
     *<p></p>
     * The implementation is based on org.apache.arrow.memory.util.Float16 by Apache Arrow
     *
     * @param b floa16 value stored in a short
     * @return float value
     */
    public static float float16ToFloat(final short b) {
        final int bits = b & 0xffff;
        final int s = bits & SIGN_MASK;
        final int e = (bits >>> EXPONENT_SHIFT) & SHIFTED_EXPONENT_MASK;
        final int m = bits & SIGNIFICAND_MASK;
        int outE = 0;
        int outM = 0;
        if (e == 0) { // Denormal or 0
            if (m != 0) {
                // Convert denorm fp16 into normalized fp32
                float o = Float.intBitsToFloat(FP32_DENORMAL_MAGIC + m);
                o -= FP32_DENORMAL_FLOAT;
                return s == 0 ? o : -o;
            }
        }
        else {
            outM = m << 13;
            if (e == 0x1f) { // Infinite or NaN
                outE = 0xff;
                if (outM != 0) { // SNaNs are quieted
                    outM |= FP32_QNAN_MASK;
                }
            }
            else {
                outE = e - EXPONENT_BIAS + FP32_EXPONENT_BIAS;
            }
        }
        final int out = (s << 16) | (outE << FP32_EXPONENT_SHIFT) | outM;
        return Float.intBitsToFloat(out);
    }
}
