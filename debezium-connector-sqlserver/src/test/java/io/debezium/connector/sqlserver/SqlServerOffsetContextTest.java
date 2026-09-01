/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;

/**
 * Unit tests for how {@code __$command_id} is written to and read back from the connector's offsets.
 * <p>
 * The value is only recorded by direct mode, so an offset may legitimately arrive without it - written by
 * an older version, or written while the connector was running in function mode. Loading such an offset
 * has to keep working.
 */
public class SqlServerOffsetContextTest {

    private static final String COMMIT_LSN = "00000a24:000018a8:0015";
    private static final String CHANGE_LSN = "00000a24:000018a8:0003";
    private static final String OTHER_CHANGE_LSN = "00000a24:000018a8:0007";

    private SqlServerConnectorConfig connectorConfig;

    @BeforeEach
    public void beforeEach() {
        connectorConfig = new SqlServerConnectorConfig(
                Configuration.create()
                        .with(CommonConnectorConfig.TOPIC_PREFIX, "serverX")
                        .build());
    }

    @Test
    @FixFor("dbz#2012")
    void shouldRoundTripCommandIdThroughTheOffset() {
        SqlServerOffsetContext offsetContext = offsetContextWithCommandId(7);

        assertThat(offsetContext.getOffset().get(SourceInfo.COMMAND_ID_KEY)).isEqualTo(7);

        SqlServerOffsetContext loaded = new SqlServerOffsetContext.Loader(connectorConfig).load(offsetContext.getOffset());

        assertThat(loaded.getChangePosition().getCommandId()).isEqualTo(7);
        assertThat(loaded.getChangePosition().getCommitLsn()).isEqualTo(Lsn.valueOf(COMMIT_LSN));
        assertThat(loaded.getChangePosition().getInTxLsn()).isEqualTo(Lsn.valueOf(CHANGE_LSN));

        offsetContext.setChangePosition(
                TxLogPosition.valueOf(Lsn.valueOf(COMMIT_LSN), Lsn.valueOf(CHANGE_LSN), 2, 6), 1);

        assertThat(offsetContext.getChangePosition().getCommandId()).isEqualTo(6);
        assertThat(offsetContext.getOffset().get(SourceInfo.COMMAND_ID_KEY)).isEqualTo(6);
    }

    @Test
    @FixFor("dbz#2012")
    void shouldLeaveCommandIdUnknownWhenAbsentFromOffset() {
        Map<String, Object> legacyOffset = baseOffset();
        assertThat(legacyOffset).doesNotContainKey(SourceInfo.COMMAND_ID_KEY);

        SqlServerOffsetContext loaded = new SqlServerOffsetContext.Loader(connectorConfig).load(legacyOffset);

        assertThat(loaded.getChangePosition().getCommandId()).isNull();
        assertThat(loaded.getChangePosition().getCommitLsn()).isEqualTo(Lsn.valueOf(COMMIT_LSN));
        assertThat(loaded.getChangePosition().getInTxLsn()).isEqualTo(Lsn.valueOf(CHANGE_LSN));
        assertThat(loaded.getEventSerialNo()).isEqualTo(2L);
    }

    @Test
    @FixFor("dbz#2012")
    void shouldLoadCommandIdWhateverNumericTypeTheOffsetStoreReturns() {
        Map<String, Object> withInteger = baseOffset();
        withInteger.put(SourceInfo.COMMAND_ID_KEY, Integer.valueOf(9));

        Map<String, Object> withLong = baseOffset();
        withLong.put(SourceInfo.COMMAND_ID_KEY, Long.valueOf(9L));

        SqlServerOffsetContext.Loader loader = new SqlServerOffsetContext.Loader(connectorConfig);

        assertThat(loader.load(withInteger).getChangePosition().getCommandId()).isEqualTo(9);
        assertThat(loader.load(withLong).getChangePosition().getCommandId()).isEqualTo(9);
    }

    // event_serial_no logic (setChangePosition / shouldUpdateEventSerialNo). The starting serial is 1.

    @Test
    @FixFor("dbz#2012")
    void shouldIncrementEventSerialNoWhenTheChangeStaysAtTheSamePosition() {
        SqlServerOffsetContext offset = offsetContextWithCommandId(7);

        // same commit + change lsn, same command id -> another event at the same log position -> increment
        offset.setChangePosition(TxLogPosition.valueOf(Lsn.valueOf(COMMIT_LSN), Lsn.valueOf(CHANGE_LSN), 4, 7), 1);

        assertThat(offset.getEventSerialNo()).isEqualTo(2L);
    }

    @Test
    @FixFor("dbz#2012")
    void shouldResetEventSerialNoWhenTheChangeMovesToANewPosition() {
        SqlServerOffsetContext offset = offsetContextWithCommandId(7);
        offset.setChangePosition(TxLogPosition.valueOf(Lsn.valueOf(COMMIT_LSN), Lsn.valueOf(CHANGE_LSN), 4, 7), 1); // -> 2

        // different change lsn -> new position -> reset to the event count
        offset.setChangePosition(TxLogPosition.valueOf(Lsn.valueOf(COMMIT_LSN), Lsn.valueOf(OTHER_CHANGE_LSN), 2, 8), 1);

        assertThat(offset.getEventSerialNo()).isEqualTo(1L);
    }

    @Test
    @FixFor("dbz#2012")
    void shouldResetEventSerialNoOnAModeSwitchEvenAtTheSamePosition() {
        SqlServerOffsetContext offset = offsetContextWithCommandId(7); // direct: command id present
        offset.setChangePosition(TxLogPosition.valueOf(Lsn.valueOf(COMMIT_LSN), Lsn.valueOf(CHANGE_LSN), 4, 7), 1); // -> 2

        // same commit + change lsn, but command id now absent (switch to function) -> reset, not increment
        offset.setChangePosition(TxLogPosition.valueOf(Lsn.valueOf(COMMIT_LSN), Lsn.valueOf(CHANGE_LSN), 4, null), 1);

        assertThat(offset.getEventSerialNo()).isEqualTo(1L);
    }

    @Test
    @FixFor("dbz#2012")
    void shouldTreatSamePositionWithDifferentCommandIdAsTheSamePositionForSerial() {
        SqlServerOffsetContext offset = offsetContextWithCommandId(1);

        // same commit + change lsn, different (both non-null) command id -> command id is ignored in the
        // position comparison, so it counts as the same log position -> increment
        offset.setChangePosition(TxLogPosition.valueOf(Lsn.valueOf(COMMIT_LSN), Lsn.valueOf(CHANGE_LSN), 2, 2), 1);

        assertThat(offset.getEventSerialNo()).isEqualTo(2L);
    }

    private SqlServerOffsetContext offsetContextWithCommandId(int commandId) {
        return new SqlServerOffsetContext(
                connectorConfig,
                TxLogPosition.valueOf(Lsn.valueOf(COMMIT_LSN), Lsn.valueOf(CHANGE_LSN), 0, commandId),
                null,
                false);
    }

    private Map<String, Object> baseOffset() {
        Map<String, Object> offset = new HashMap<>();
        offset.put(SourceInfo.COMMIT_LSN_KEY, COMMIT_LSN);
        offset.put(SourceInfo.CHANGE_LSN_KEY, CHANGE_LSN);
        offset.put(SourceInfo.EVENT_SERIAL_NO_KEY, Long.valueOf(2));
        return offset;
    }
}
