/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.xstream;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;

/**
 * @author Chris Cranford
 */
public class LcrPositionTest {

    // Row LCR of the transaction reported in debezium/dbz#2782: commit SCN 0x22cfee, SCN 0x22cfed, sequence 5
    private static final String ROW_POSITION = "000000000022cfee0000000100000001000000000022cfed000000050000000102";
    private static final String TUPLE_START = "000000000022cfee0000000100000001000000000022cfed000000000000000002";
    private static final String COMMIT_POSITION = "000000000022cfee0000000100000001000000000022cfee000000010000000102";

    // A 29-byte V1 layout position (version byte 0x01), commit SCN 3, SCN 196613, with a non-zero sequence region
    private static final String V1_ROW_POSITION = "0000000000030010000000010000000000030005000000070000000101";
    private static final String V1_TUPLE_START = "0000000000030010000000010000000000030005000000000000000001";

    @Test
    @FixFor("debezium/dbz#2782")
    public void shouldZeroSequenceWhenResolvingTupleStart() {
        final LcrPosition position = LcrPosition.valueOf(ROW_POSITION);
        final LcrPosition tupleStart = position.getTupleStartPosition();

        assertThat(tupleStart.toString()).isEqualTo(TUPLE_START);
        assertThat(tupleStart.getScn()).isEqualTo(position.getScn());
        assertThat(tupleStart.getCommitScn()).isEqualTo(position.getCommitScn());
        assertThat(tupleStart).isLessThan(position);
    }

    @Test
    @FixFor("debezium/dbz#2782")
    public void shouldSortTupleStartBeforeEveryPositionOfTheTupleAndAfterEarlierTuples() {
        final LcrPosition tupleStart = LcrPosition.valueOf(ROW_POSITION).getTupleStartPosition();

        assertThat(tupleStart).isLessThan(LcrPosition.valueOf("000000000022cfee0000000100000001000000000022cfed000000010000000102"));
        assertThat(tupleStart).isLessThan(LcrPosition.valueOf(COMMIT_POSITION));
        assertThat(tupleStart).isGreaterThan(LcrPosition.valueOf("000000000022cfee0000000100000001000000000022cfec000000090000000102"));
    }

    @Test
    @FixFor("debezium/dbz#2782")
    public void shouldReturnSameInstanceWhenAlreadyAtTupleStart() {
        final LcrPosition tupleStart = LcrPosition.valueOf(TUPLE_START);
        assertThat(tupleStart.getTupleStartPosition()).isSameAs(tupleStart);
    }

    @Test
    @FixFor("debezium/dbz#2782")
    public void shouldResolveTupleStartForTheV1PositionLayout() {
        final LcrPosition position = LcrPosition.valueOf(V1_ROW_POSITION);
        final LcrPosition tupleStart = position.getTupleStartPosition();

        assertThat(tupleStart.toString()).isEqualTo(V1_TUPLE_START);
        assertThat(tupleStart.getRawPosition()).hasSize(29);
        // The 29-byte layout keeps both SCNs (ahead of the zeroed sequence) and its version byte
        assertThat(tupleStart.getScn()).isEqualTo(position.getScn());
        assertThat(tupleStart.getCommitScn()).isEqualTo(position.getCommitScn());
        assertThat(tupleStart.getRawPosition()[28]).isEqualTo((byte) 0x01);
        assertThat(tupleStart).isLessThan(position);
    }
}
