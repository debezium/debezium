/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.events;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

public class RowIdCodecTest {

    // A real Oracle ROWID whose chars at indices 3–5 are S, J, q (non-zero).
    // These bits fall in the upper 44 bits; the original single-long encode() silently
    // discarded them, producing "AAAAAAAANAAAAnkAAI" on decode instead of this value.
    private static final String ROWID_WITH_HIGH_BITS = "AAASJqAANAAAAnkAAI";

    // -------------------------------------------------------------------------
    // encode() — input validation
    // -------------------------------------------------------------------------

    @Test
    void encodeShouldThrowWhenRowIdIsNull() {
        assertThatThrownBy(() -> RowIdCodec.encode(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void encodeShouldThrowWhenRowIdIsTooShort() {
        assertThatThrownBy(() -> RowIdCodec.encode("AAAAAAAAAAAAAAAAA")) // 17 chars
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void encodeShouldThrowWhenRowIdIsTooLong() {
        assertThatThrownBy(() -> RowIdCodec.encode("AAAAAAAAAAAAAAAAAAA")) // 19 chars
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void encodeShouldThrowWhenRowIdContainsNonAsciiCharacter() {
        // 'é' = U+00E9 = 233 > 127; length is still 18 so only the character check fires
        assertThatThrownBy(() -> RowIdCodec.encode("AAAAAAAAAAAAAAAAAé"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // -------------------------------------------------------------------------
    // decode(encode()) round-trip — correctness
    // -------------------------------------------------------------------------

    @Test
    void roundTripShouldBeExactForAllARowId() {
        // All chars are 'A' (value 0); high=0, no overflow path exercised
        String allA = "AAAAAAAAAAAAAAAAAA";
        assertThat(RowIdCodec.decode(RowIdCodec.encode(allA))).isEqualTo(allA);
    }

    @Test
    void roundTripShouldBeExactForMaxValueRowId() {
        // All chars are '/' (value 63 = 0b111111); all 108 bits set
        String maxRowId = "//////////////////";
        assertThat(RowIdCodec.decode(RowIdCodec.encode(maxRowId))).isEqualTo(maxRowId);
    }

    @Test
    void roundTripShouldBeExactWhenChar7HasNonZeroUpperBits() {
        // char[7] straddles the 64-bit boundary: its upper 2 bits go into high,
        // lower 4 bits remain in low. 'Q' = 16 = 0b010000, so bit4=1 overflows into high.
        String rowId = "AAAAAAAQAAAAAAAAAA";
        assertThat(RowIdCodec.decode(RowIdCodec.encode(rowId))).isEqualTo(rowId);
    }

    @Test
    void roundTripShouldBeExactWhenHighBitsAreNonZero() {
        // Chars 3–5 (S=18, J=9, q=42) sit entirely above bit 63 and are recovered
        // via the high register — this is the data-loss regression case
        assertThat(RowIdCodec.decode(RowIdCodec.encode(ROWID_WITH_HIGH_BITS)))
                .isEqualTo(ROWID_WITH_HIGH_BITS);
    }

    @Test
    void roundTripShouldBeExactForMultipleDistinctRowIds() {
        String[] rowIds = {
                "AAASJqAANAAAAnkAAI",
                "AAAR/aAANAAAAPtAAA",
                "AAAR/aAANAAAAPtAAB",
                "AAANewAAEAAAAwTAAA",
        };
        for (String rowId : rowIds) {
            assertThat(RowIdCodec.decode(RowIdCodec.encode(rowId)))
                    .as("round-trip for %s", rowId)
                    .isEqualTo(rowId);
        }
    }

    // -------------------------------------------------------------------------
    // EMPTY_ROW_ID constant
    // -------------------------------------------------------------------------

    @Test
    void emptyRowIdShouldDecodeToAllA() {
        assertThat(RowIdCodec.decode(RowIdCodec.EMPTY_ROW_ID)).isEqualTo("AAAAAAAAAAAAAAAAAA");
    }

    @Test
    void emptyRowIdShouldEqualEncodeOfAllA() {
        assertThat(RowIdCodec.EMPTY_ROW_ID).isEqualTo(RowIdCodec.encode("AAAAAAAAAAAAAAAAAA"));
    }

    // -------------------------------------------------------------------------
    // Packed record — value equality
    // -------------------------------------------------------------------------

    @Test
    void twoPackedInstancesEncodingSameRowIdShouldBeEqual() {
        RowIdCodec.Packed a = RowIdCodec.encode(ROWID_WITH_HIGH_BITS);
        RowIdCodec.Packed b = RowIdCodec.encode(ROWID_WITH_HIGH_BITS);
        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
    }

    @Test
    void twoPackedInstancesEncodingDifferentRowIdsShouldNotBeEqual() {
        RowIdCodec.Packed a = RowIdCodec.encode("AAASJqAANAAAAnkAAI");
        RowIdCodec.Packed b = RowIdCodec.encode("AAAR/aAANAAAAPtAAA");
        assertThat(a).isNotEqualTo(b);
    }
}