/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;

/**
 * Tests for the way the SQL Server CDC {@code __$update_mask} is read. The mask decides whether a
 * max-type column that is null in an UPDATE image was really set to null or was only left out of
 * the update, in which case the unavailable value placeholder is emitted instead of the null.
 */
public class SqlServerChangeTablePointerTest {

    private static final int WIDE_TABLE_COLUMNS = 12;
    private static final int LEGAL_TEXT_ORDINAL = 2;
    private static final int REVISION_ORDINAL = 10;

    @Test
    @FixFor("dbz#1164")
    void shouldTreatColumnAsChangedWhenMaskIsUnavailable() {
        assertThat(SqlServerChangeTablePointer.isColumnChanged(null, 0)).isTrue();
    }

    @Test
    @FixFor("dbz#1164")
    void shouldTellChangedAndUnchangedColumnsApartInSingleByteMask() {
        final byte[] mask = updateMask(3, 2);

        assertThat(mask).containsExactly(0x02);
        assertThat(SqlServerChangeTablePointer.isColumnChanged(mask, 0)).isFalse();
        assertThat(SqlServerChangeTablePointer.isColumnChanged(mask, 1)).isTrue();
        assertThat(SqlServerChangeTablePointer.isColumnChanged(mask, 2)).isFalse();
    }

    @Test
    @FixFor("dbz#2650")
    void shouldTellChangedAndUnchangedColumnsApartInTableWithMoreThanEightColumns() {
        final byte[] mask = updateMask(WIDE_TABLE_COLUMNS, LEGAL_TEXT_ORDINAL);

        // The lowest ordinals live in the last byte of the mask, not in its first one.
        assertThat(mask).containsExactly(0x00, 0x02);
        assertThat(SqlServerChangeTablePointer.isColumnChanged(mask, LEGAL_TEXT_ORDINAL - 1)).isTrue();
        assertThat(SqlServerChangeTablePointer.isColumnChanged(mask, 0)).isFalse();
        assertThat(SqlServerChangeTablePointer.isColumnChanged(mask, 2)).isFalse();
    }

    @Test
    @FixFor("dbz#2650")
    void shouldReadColumnBeyondTheFirstEightColumnsFromEarlierMaskByte() {
        final byte[] mask = updateMask(WIDE_TABLE_COLUMNS, REVISION_ORDINAL);

        assertThat(mask).containsExactly(0x02, 0x00);
        assertThat(SqlServerChangeTablePointer.isColumnChanged(mask, REVISION_ORDINAL - 1)).isTrue();
        assertThat(SqlServerChangeTablePointer.isColumnChanged(mask, LEGAL_TEXT_ORDINAL - 1)).isFalse();
    }

    @Test
    @FixFor("dbz#2650")
    void shouldDetectChangedColumnStoredInHighestBitOfMaskByte() {
        final byte[] mask = updateMask(8, 8);

        assertThat(mask).containsExactly(0x80);
        assertThat(SqlServerChangeTablePointer.isColumnChanged(mask, 7)).isTrue();
        assertThat(SqlServerChangeTablePointer.isColumnChanged(mask, 6)).isFalse();
    }

    @Test
    @FixFor("dbz#2650")
    void shouldTreatColumnAsChangedWhenMaskIsTooShortToCoverIt() {
        final byte[] mask = updateMask(3, 1);

        assertThat(SqlServerChangeTablePointer.isColumnChanged(mask, 8)).isTrue();
    }

    @Test
    @FixFor("dbz#2650")
    void shouldFollowSqlServerBitLayoutForEveryCapturedColumn() {
        for (int capturedColumnCount : new int[]{ 1, 8, 9, 16, 17, 24 }) {
            for (int changedOrdinal = 1; changedOrdinal <= capturedColumnCount; changedOrdinal++) {
                final byte[] mask = updateMask(capturedColumnCount, changedOrdinal);

                for (int ordinal = 1; ordinal <= capturedColumnCount; ordinal++) {
                    assertThat(SqlServerChangeTablePointer.isColumnChanged(mask, ordinal - 1))
                            .describedAs("column %d of %d, mask for changed column %d", ordinal, capturedColumnCount, changedOrdinal)
                            .isEqualTo(ordinal == changedOrdinal);
                }
            }
        }
    }

    /**
     * Builds an update mask the way SQL Server does: one bit per captured column, the column with
     * ordinal 1 in the lowest bit of the last byte, higher ordinals continuing towards the first
     * byte. This is the layout {@code sys.fn_cdc_is_bit_set} reads.
     *
     * @param capturedColumnCount the number of columns the capture instance covers
     * @param changedOrdinals the 1-based ordinals of the columns an update changed
     * @return the mask bytes as they are stored in the {@code __$update_mask} column
     */
    private static byte[] updateMask(int capturedColumnCount, int... changedOrdinals) {
        final byte[] mask = new byte[(capturedColumnCount + 7) / 8];

        for (int ordinal : changedOrdinals) {
            final int byteIndex = mask.length - 1 - (ordinal - 1) / 8;
            mask[byteIndex] |= (byte) (1 << ((ordinal - 1) % 8));
        }

        return mask;
    }
}
