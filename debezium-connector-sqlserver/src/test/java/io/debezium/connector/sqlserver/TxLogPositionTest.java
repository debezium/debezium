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
 * Unit tests for the ordering of {@link TxLogPosition}, in particular for the {@code __$command_id} term
 * that direct mode resumes on.
 */
public class TxLogPositionTest {

    private static final Lsn TX_1 = Lsn.valueOf("00000a24:000018a8:0015");

    private static final Lsn SEQVAL_LOW = Lsn.valueOf("00000a24:000018a8:0003");
    private static final Lsn SEQVAL_HIGH = Lsn.valueOf("00000a24:000018a8:0007");

    @Test
    @FixFor("dbz#2012")
    void shouldOrderByCommandIdBeforeSequenceValue() {
        TxLogPosition lastDelete = TxLogPosition.valueOf(TX_1, SEQVAL_HIGH, 1, 5);
        TxLogPosition firstInsert = TxLogPosition.valueOf(TX_1, SEQVAL_LOW, 2, 6);

        assertThat(lastDelete).isLessThan(firstInsert);
    }

    @Test
    @FixFor("dbz#2012")
    void shouldFallBackToSequenceValueWhenOnlyOneSideKnowsItsCommandId() {
        TxLogPosition fromChangeTable = TxLogPosition.valueOf(TX_1, SEQVAL_LOW, 1, 6);
        TxLogPosition fromLegacyOffset = TxLogPosition.valueOf(TX_1, SEQVAL_HIGH, 0);

        assertThat(fromChangeTable).isLessThan(fromLegacyOffset);
        assertThat(fromLegacyOffset).isGreaterThan(fromChangeTable);
    }

    @Test
    @FixFor("dbz#2012")
    void shouldPreserveFunctionModeOrderingWhenCommandIdIsAbsent() {
        TxLogPosition lower = TxLogPosition.valueOf(TX_1, SEQVAL_LOW, 1);
        TxLogPosition higher = TxLogPosition.valueOf(TX_1, SEQVAL_HIGH, 2);

        assertThat(lower).isLessThan(higher);
        assertThat(higher).isGreaterThan(lower);

        assertThat(lower.getCommandId()).isNull();
        assertThat(higher.getCommandId()).isNull();
        assertThat(TxLogPosition.valueOf(TX_1).getCommandId()).isNull();
        assertThat(TxLogPosition.NULL.getCommandId()).isNull();
    }

    @Test
    void shouldTreatBeforeAndAfterImagesOfAnUpdateAsTheSamePosition() {
        TxLogPosition updateBefore = TxLogPosition.valueOf(TX_1, SEQVAL_LOW, 3, 7);
        TxLogPosition updateAfter = TxLogPosition.valueOf(TX_1, SEQVAL_LOW, 4, 7);

        assertThat(updateBefore).isEqualByComparingTo(updateAfter);
        assertThat(updateBefore).isEqualTo(updateAfter);
    }

    @Test
    @FixFor("dbz#2012")
    void shouldDistinguishPositionsThatDifferOnlyByCommandId() {
        TxLogPosition delete = TxLogPosition.valueOf(TX_1, SEQVAL_LOW, 1, 1);
        TxLogPosition insert = TxLogPosition.valueOf(TX_1, SEQVAL_LOW, 2, 6);

        assertThat(delete).isNotEqualTo(insert);
        assertThat(delete).isLessThan(insert);
    }
}
