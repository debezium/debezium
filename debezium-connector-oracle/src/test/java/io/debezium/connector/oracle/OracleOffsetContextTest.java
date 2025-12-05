/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
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

    @Rule
    public TestRule skipTestRule = new SkipTestDependingOnAdapterNameRule();

    private OracleConnectorConfig connectorConfig;
    private OffsetContext.Loader offsetLoader;

    @Before
    public void beforeEach() throws Exception {
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
    @FixFor({ "DBZ-4937", "DBZ-5245" })
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
        assertThat(writeValues.get(OracleOffsetContext.SNAPSHOT_PENDING_TRANSACTIONS_KEY)).isNull();
        assertThat(writeValues.get(OracleOffsetContext.SNAPSHOT_SCN_KEY)).isNull();

        // Simulate reloading of Debezium 1.9 values
        offsetContext = (OracleOffsetContext) offsetLoader.load(writeValues);

        // Write values out as Debezium 1.9
        writeValues = offsetContext.getOffset();
        assertThat(writeValues.get(SourceInfo.SCN_KEY)).isEqualTo("745688898023");
        assertThat(writeValues.get(SourceInfo.COMMIT_SCN_KEY)).isEqualTo("745688898024:1:");
        assertThat(writeValues.get(OracleOffsetContext.SNAPSHOT_PENDING_TRANSACTIONS_KEY)).isNull();
        assertThat(writeValues.get(OracleOffsetContext.SNAPSHOT_SCN_KEY)).isNull();
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
}
