/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.doc.FixFor;
import io.debezium.pipeline.spi.OffsetContext;

/**
 * Unit test that validates the behavior of the {@link OracleOffsetContext} and its friends.
 *
 * @author Chris Cranford
 */
@SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.ANY_LOGMINER, reason = "Only applies to LogMiner")
public class OracleOffsetContextTest {

    private OracleConnectorConfig connectorConfig;
    private OffsetContext.Loader offsetLoader;

    @BeforeEach
    void beforeEach() throws Exception {
        this.connectorConfig = new OracleConnectorConfig(TestHelper.defaultConfig().build());
        this.offsetLoader = connectorConfig.getAdapter().getOffsetContextLoader();
    }

    @Test
    @FixFor({ "DBZ-2994", "DBZ-5245" })
    public void shouldReadScnAndCommitScnAsLongValues() throws Exception {
        final Map<String, Object> offsetValues = new HashMap<>();
        offsetValues.put(SourceInfo.SCN_KEY, 12345L);
        offsetValues.put(SourceInfo.COMMIT_SCN_KEY, 23456L);

        final OracleOffsetContext offsetContext = (OracleOffsetContext) offsetLoader.load(offsetValues);
        assertThat(offsetContext.getScn()).isEqualTo(Scn.valueOf("12345"));
        if (TestHelper.isBufferedLogMiner()) {
            assertThat(offsetContext.getCommitScn().getMaxCommittedScn()).isEqualTo(Scn.valueOf("23456"));
        }
    }

    @Test
    @FixFor({ "DBZ-2994", "DBZ-5245" })
    public void shouldReadScnAndCommitScnAsStringValues() throws Exception {
        final Map<String, Object> offsetValues = new HashMap<>();
        offsetValues.put(SourceInfo.SCN_KEY, "12345");
        offsetValues.put(SourceInfo.COMMIT_SCN_KEY, "23456");

        final OracleOffsetContext offsetContext = (OracleOffsetContext) offsetLoader.load(offsetValues);
        assertThat(offsetContext.getScn()).isEqualTo(Scn.valueOf("12345"));
        if (TestHelper.isBufferedLogMiner()) {
            assertThat(offsetContext.getCommitScn().getMaxCommittedScn()).isEqualTo(Scn.valueOf("23456"));
        }
    }

    @Test
    @FixFor({ "DBZ-2994", "DBZ-5245" })
    public void shouldHandleNullScnAndCommitScnValues() throws Exception {
        final Map<String, Object> offsetValues = new HashMap<>();
        offsetValues.put(SourceInfo.SCN_KEY, null);
        offsetValues.put(SourceInfo.COMMIT_SCN_KEY, null);

        final OracleOffsetContext offsetContext = (OracleOffsetContext) offsetLoader.load(offsetValues);
        assertThat(offsetContext.getScn()).isNull();
        assertThat(offsetContext.getCommitScn().getMaxCommittedScn()).isEqualTo(Scn.NULL);
    }

    @Test
    @FixFor({ "DBZ-4937", "DBZ-5245", "debezium/dbz#2779" })
    public void shouldCorrectlySerializeOffsetsWithSnapshotBasedKeysFromOlderOffsets() throws Exception {
        // Offsets from Debezium 1.8
        final Map<String, Object> offsetValues = new HashMap<>();
        offsetValues.put(SourceInfo.SCN_KEY, "745688898023");
        offsetValues.put(SourceInfo.COMMIT_SCN_KEY, "745688898024");
        offsetValues.put("transaction_id", null);

        OracleOffsetContext offsetContext = (OracleOffsetContext) offsetLoader.load(offsetValues);

        // Write values out as Debezium 1.9
        Map<String, ?> writeValues = offsetContext.getOffset();
        assertThat(writeValues.get(SourceInfo.SCN_KEY)).isEqualTo("745688898023");
        assertThat(writeValues.get(SourceInfo.COMMIT_SCN_KEY)).isEqualTo("745688898024:1:");
        assertThat(writeValues.get(OracleOffsetContext.SNAPSHOT_COMMIT_SCN_KEY)).isNull();
        assertThat(writeValues.get("snapshot_scn")).isNull();

        // Simulate reloading of Debezium 1.9 values
        offsetContext = (OracleOffsetContext) offsetLoader.load(writeValues);

        // Write values out as Debezium 1.9
        writeValues = offsetContext.getOffset();
        assertThat(writeValues.get(SourceInfo.SCN_KEY)).isEqualTo("745688898023");
        assertThat(writeValues.get(SourceInfo.COMMIT_SCN_KEY)).isEqualTo("745688898024:1:");
        assertThat(writeValues.get(OracleOffsetContext.SNAPSHOT_COMMIT_SCN_KEY)).isNull();
        assertThat(writeValues.get("snapshot_scn")).isNull();
    }

    @Test
    @FixFor("debezium/dbz#2779")
    public void shouldRoundTripSnapshotCommitScn() throws Exception {
        final Map<String, Object> offsetValues = new HashMap<>();
        offsetValues.put(SourceInfo.SCN_KEY, "100");
        offsetValues.put(OracleOffsetContext.SNAPSHOT_COMMIT_SCN_KEY, "120");

        final OracleOffsetContext offsetContext = (OracleOffsetContext) offsetLoader.load(offsetValues);
        assertThat(offsetContext.getScn()).isEqualTo(Scn.valueOf(100));
        assertThat(offsetContext.getSnapshotCommitScn()).isEqualTo(Scn.valueOf(120));
        assertThat(offsetContext.getSnapshotAsOfScn()).isEqualTo(Scn.valueOf(120));
        assertThat(offsetContext.getEventScn()).isEqualTo(Scn.valueOf(120));
        assertThat(offsetContext.isEventScnLessThanOrEqualToSnapshotCommitScn(row(120, Scn.NULL))).isTrue();
        assertThat(offsetContext.isEventScnLessThanOrEqualToSnapshotCommitScn(row(121, Scn.NULL))).isFalse();
        assertThat(offsetContext.isEventCommitScnLessThanOrEqualToSnapshotCommitScn(row(100, Scn.valueOf(120)))).isTrue();
        assertThat(offsetContext.isEventCommitScnLessThanOrEqualToSnapshotCommitScn(row(100, Scn.valueOf(121)))).isFalse();
        assertThat(offsetContext.isEventCommitScnLessThanOrEqualToSnapshotCommitScn(row(100, Scn.NULL))).isFalse();

        final Map<String, ?> writeValues = offsetContext.getOffset();
        assertThat(writeValues.get(SourceInfo.SCN_KEY)).isEqualTo("100");
        assertThat(writeValues.get(OracleOffsetContext.SNAPSHOT_COMMIT_SCN_KEY)).isEqualTo("120");
        assertThat(writeValues.get("snapshot_scn")).isNull();
    }

    @Test
    @FixFor("debezium/dbz#2779")
    public void shouldUseOffsetScnAsSnapshotAsOfScnWithoutSnapshotCommitScn() throws Exception {
        final Map<String, Object> offsetValues = new HashMap<>();
        offsetValues.put(SourceInfo.SCN_KEY, "100");

        final OracleOffsetContext offsetContext = (OracleOffsetContext) offsetLoader.load(offsetValues);
        assertThat(offsetContext.getSnapshotCommitScn()).isEqualTo(Scn.NULL);
        assertThat(offsetContext.getSnapshotAsOfScn()).isEqualTo(Scn.valueOf(100));
        assertThat(offsetContext.getEventScn()).isEqualTo(Scn.valueOf(100));
        assertThat(offsetContext.isEventScnLessThanOrEqualToSnapshotCommitScn(row(100, Scn.valueOf(100)))).isFalse();
        assertThat(offsetContext.isEventCommitScnLessThanOrEqualToSnapshotCommitScn(row(100, Scn.valueOf(100)))).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#2779")
    public void shouldRetireSnapshotCommitScnOnceEveryRedoThreadCommittedPastIt() throws Exception {
        // Thread 1 has not committed past the snapshot yet.
        OracleOffsetContext offsetContext = loadSnapshotBoundaryOffset("700:1:");
        Map<String, ?> writeValues = offsetContext.getOffset();
        assertThat(writeValues.get(OracleOffsetContext.SNAPSHOT_COMMIT_SCN_KEY)).isEqualTo("800");

        // Thread 2 has not committed past the snapshot yet.
        offsetContext = loadSnapshotBoundaryOffset("900:1:,700:2:");
        writeValues = offsetContext.getOffset();
        assertThat(writeValues.get(OracleOffsetContext.SNAPSHOT_COMMIT_SCN_KEY)).isEqualTo("800");

        // Every redo thread has committed past the snapshot.
        offsetContext = loadSnapshotBoundaryOffset("900:1:,950:2:");
        writeValues = offsetContext.getOffset();
        assertThat(writeValues.get(OracleOffsetContext.SNAPSHOT_COMMIT_SCN_KEY)).isNull();
        assertThat(offsetContext.getSnapshotCommitScn()).isEqualTo(Scn.NULL);
    }

    @Test
    @FixFor("debezium/dbz#2779")
    public void shouldLoadLegacySnapshotScnAsSnapshotCommitScn() throws Exception {
        Map<String, Object> offsetValues = new HashMap<>();
        offsetValues.put(SourceInfo.SCN_KEY, "89");
        offsetValues.put("snapshot_scn", "100");
        offsetValues.put("snapshot_pending_tx", "abc:95,def:90");

        OracleOffsetContext offsetContext = (OracleOffsetContext) offsetLoader.load(offsetValues);
        assertThat(offsetContext.getScn()).isEqualTo(Scn.valueOf(89));
        assertThat(offsetContext.getSnapshotCommitScn()).isEqualTo(Scn.valueOf(100));

        Map<String, ?> writeValues = offsetContext.getOffset();
        assertThat(writeValues.get(SourceInfo.SCN_KEY)).isEqualTo("89");
        assertThat(writeValues.get(OracleOffsetContext.SNAPSHOT_COMMIT_SCN_KEY)).isEqualTo("100");
        assertThat(writeValues.get("snapshot_scn")).isNull();
        assertThat(writeValues.get("snapshot_pending_tx")).isNull();
    }

    @Test
    @FixFor({ "DBZ-8924" })
    @SkipWhenAdapterNameIsNot(SkipWhenAdapterNameIsNot.AdapterName.LOGMINER_UNBUFFERED)
    public void shouldCorrectlyDeserializeTransactionDetails() throws Exception {
        final Map<String, Object> offsetValues = new HashMap<>();
        offsetValues.put(SourceInfo.SCN_KEY, 12345L);
        offsetValues.put(SourceInfo.COMMIT_SCN_KEY, 23456L);
        offsetValues.put(SourceInfo.TXID_KEY, "123");
        offsetValues.put(SourceInfo.TXSEQ_KEY, 98765L);

        final OracleOffsetContext offsetContext = (OracleOffsetContext) offsetLoader.load(offsetValues);

        assertThat(offsetContext.getScn()).isEqualTo(Scn.valueOf("12345"));
        assertThat(offsetContext.getCommitScn().getMaxCommittedScn()).isEqualTo(Scn.valueOf("23456"));
        assertThat(offsetContext.getTransactionId()).isEqualTo("123");
        assertThat(offsetContext.getTransactionSequence()).isEqualTo(98765L);
    }

    private OracleOffsetContext loadSnapshotBoundaryOffset(String commitScn) {
        final Map<String, Object> offsetValues = new HashMap<>();
        offsetValues.put(SourceInfo.SCN_KEY, "790");
        offsetValues.put(SourceInfo.COMMIT_SCN_KEY, commitScn);
        offsetValues.put(OracleOffsetContext.SNAPSHOT_COMMIT_SCN_KEY, "800");
        return (OracleOffsetContext) offsetLoader.load(offsetValues);
    }

    private static LogMinerEventRow row(long scn, Scn commitScn) {
        final LogMinerEventRow row = mock(LogMinerEventRow.class);
        when(row.getScn()).thenReturn(Scn.valueOf(scn));
        when(row.getCommitScn()).thenReturn(commitScn);
        return row;
    }
}
