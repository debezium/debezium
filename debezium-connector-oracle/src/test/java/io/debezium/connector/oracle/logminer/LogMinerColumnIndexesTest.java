/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.util.TestHelper;

/**
 * Unit tests for {@link LogMinerColumnIndexes}.
 *
 * <p>These tests verify that the pre-computed column ordinals produced by
 * {@link LogMinerColumnIndexes#fromConfig(OracleConnectorConfig)} align exactly with the column
 * ordering defined in {@link AbstractLogMinerQueryBuilder#buildColumnList()}.
 *
 * <p>Fixed positions 1–9 are constants and never shift; they are tested here for completeness
 * but would never regress unless changed deliberately. Positions 10+ can shift when optional
 * columns are omitted via configuration flags, so every combination of omission is exercised.
 */
@SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.ANY_LOGMINER)
public class LogMinerColumnIndexesTest {

    // Fixed column constant values

    @Test
    void fixedColumnConstantsShouldMatchExpectedOrdinals() {
        assertThat(LogMinerColumnIndexes.SCN).isEqualTo(1);
        assertThat(LogMinerColumnIndexes.SQL_REDO).isEqualTo(2);
        assertThat(LogMinerColumnIndexes.OPERATION_CODE).isEqualTo(3);
        assertThat(LogMinerColumnIndexes.TIMESTAMP).isEqualTo(4);
        assertThat(LogMinerColumnIndexes.XID).isEqualTo(5);
        assertThat(LogMinerColumnIndexes.CSF).isEqualTo(6);
        assertThat(LogMinerColumnIndexes.TABLE_NAME).isEqualTo(7);
        assertThat(LogMinerColumnIndexes.SEG_OWNER).isEqualTo(8);
        assertThat(LogMinerColumnIndexes.OPERATION).isEqualTo(9);
    }

    // All optional columns enabled (baseline)

    @Test
    void allTrackingEnabledShouldProduceBaselineOrdinals() {
        LogMinerColumnIndexes idx = fromDefaultConfig();

        // Optional columns present
        assertThat(idx.getUsernameIndex()).isEqualTo(10);
        assertThat(idx.getRsIdIndex()).isEqualTo(13);
        assertThat(idx.getClientIdIndex()).isEqualTo(21);
        assertThat(idx.getStartTimestampIndex()).isEqualTo(24);
        assertThat(idx.getCommitTimestampIndex()).isEqualTo(25);

        // Mandatory columns at their full-set positions
        assertThat(idx.getRowIdIndex()).isEqualTo(11);
        assertThat(idx.getRollbackFlagIndex()).isEqualTo(12);
        assertThat(idx.getStatusIndex()).isEqualTo(14);
        assertThat(idx.getInfoIndex()).isEqualTo(15);
        assertThat(idx.getSsnIndex()).isEqualTo(16);
        assertThat(idx.getThreadIndex()).isEqualTo(17);
        assertThat(idx.getObjectIdIndex()).isEqualTo(18);
        assertThat(idx.getObjectVersionIndex()).isEqualTo(19);
        assertThat(idx.getDataObjectIdIndex()).isEqualTo(20);
        assertThat(idx.getStartScnIndex()).isEqualTo(22);
        assertThat(idx.getCommitScnIndex()).isEqualTo(23);
        assertThat(idx.getSequenceIndex()).isEqualTo(26);
    }

    // USERNAME disabled

    @Test
    void usernameDisabledShouldShiftSubsequentColumns() {
        LogMinerColumnIndexes idx = fromConfig(OracleConnectorConfig.LOG_MINING_BUFFER_TRACK_USERNAME, false);

        assertThat(idx.getUsernameIndex()).isNull();

        // Columns after USERNAME each shift down by 1
        assertThat(idx.getRowIdIndex()).isEqualTo(10);
        assertThat(idx.getRollbackFlagIndex()).isEqualTo(11);
        assertThat(idx.getRsIdIndex()).isEqualTo(12);
        assertThat(idx.getStatusIndex()).isEqualTo(13);
        assertThat(idx.getInfoIndex()).isEqualTo(14);
        assertThat(idx.getSsnIndex()).isEqualTo(15);
        assertThat(idx.getThreadIndex()).isEqualTo(16);
        assertThat(idx.getObjectIdIndex()).isEqualTo(17);
        assertThat(idx.getObjectVersionIndex()).isEqualTo(18);
        assertThat(idx.getDataObjectIdIndex()).isEqualTo(19);
        assertThat(idx.getClientIdIndex()).isEqualTo(20);
        assertThat(idx.getStartScnIndex()).isEqualTo(21);
        assertThat(idx.getCommitScnIndex()).isEqualTo(22);
        assertThat(idx.getStartTimestampIndex()).isEqualTo(23);
        assertThat(idx.getCommitTimestampIndex()).isEqualTo(24);
        assertThat(idx.getSequenceIndex()).isEqualTo(25);
    }

    // RS_ID disabled

    @Test
    void rsIdDisabledShouldShiftColumnsAfterRollback() {
        LogMinerColumnIndexes idx = fromConfig(OracleConnectorConfig.LOG_MINING_BUFFER_TRACK_RS_ID, false);

        // Columns before RS_ID are unaffected
        assertThat(idx.getUsernameIndex()).isEqualTo(10);
        assertThat(idx.getRowIdIndex()).isEqualTo(11);
        assertThat(idx.getRollbackFlagIndex()).isEqualTo(12);
        assertThat(idx.getRsIdIndex()).isNull();

        // Everything from STATUS onwards shifts by 1
        assertThat(idx.getStatusIndex()).isEqualTo(13);
        assertThat(idx.getInfoIndex()).isEqualTo(14);
        assertThat(idx.getSsnIndex()).isEqualTo(15);
        assertThat(idx.getThreadIndex()).isEqualTo(16);
        assertThat(idx.getObjectIdIndex()).isEqualTo(17);
        assertThat(idx.getObjectVersionIndex()).isEqualTo(18);
        assertThat(idx.getDataObjectIdIndex()).isEqualTo(19);
        assertThat(idx.getClientIdIndex()).isEqualTo(20);
        assertThat(idx.getStartScnIndex()).isEqualTo(21);
        assertThat(idx.getCommitScnIndex()).isEqualTo(22);
        assertThat(idx.getStartTimestampIndex()).isEqualTo(23);
        assertThat(idx.getCommitTimestampIndex()).isEqualTo(24);
        assertThat(idx.getSequenceIndex()).isEqualTo(25);
    }

    // CLIENT_ID disabled

    @Test
    void clientIdDisabledShouldShiftColumnsAfterDataObjectId() {
        LogMinerColumnIndexes idx = fromConfig(OracleConnectorConfig.LOG_MINING_BUFFER_TRACK_CLIENT_ID, false);

        // Columns before CLIENT_ID are unaffected
        assertThat(idx.getUsernameIndex()).isEqualTo(10);
        assertThat(idx.getRowIdIndex()).isEqualTo(11);
        assertThat(idx.getRollbackFlagIndex()).isEqualTo(12);
        assertThat(idx.getRsIdIndex()).isEqualTo(13);
        assertThat(idx.getStatusIndex()).isEqualTo(14);
        assertThat(idx.getInfoIndex()).isEqualTo(15);
        assertThat(idx.getSsnIndex()).isEqualTo(16);
        assertThat(idx.getThreadIndex()).isEqualTo(17);
        assertThat(idx.getObjectIdIndex()).isEqualTo(18);
        assertThat(idx.getObjectVersionIndex()).isEqualTo(19);
        assertThat(idx.getDataObjectIdIndex()).isEqualTo(20);
        assertThat(idx.getClientIdIndex()).isNull();

        // Everything from START_SCN onwards shifts by 1
        assertThat(idx.getStartScnIndex()).isEqualTo(21);
        assertThat(idx.getCommitScnIndex()).isEqualTo(22);
        assertThat(idx.getStartTimestampIndex()).isEqualTo(23);
        assertThat(idx.getCommitTimestampIndex()).isEqualTo(24);
        assertThat(idx.getSequenceIndex()).isEqualTo(25);
    }

    // START_TIMESTAMP disabled

    @Test
    void startTimestampDisabledShouldShiftCommitTimestampAndSequence() {
        LogMinerColumnIndexes idx = fromConfig(OracleConnectorConfig.LOG_MINING_BUFFER_TRACK_START_TIMESTAMP, false);

        // All positions up to COMMIT_SCN are unaffected
        assertThat(idx.getStartScnIndex()).isEqualTo(22);
        assertThat(idx.getCommitScnIndex()).isEqualTo(23);
        assertThat(idx.getStartTimestampIndex()).isNull();

        // COMMIT_TIMESTAMP and SEQUENCE each shift by 1
        assertThat(idx.getCommitTimestampIndex()).isEqualTo(24);
        assertThat(idx.getSequenceIndex()).isEqualTo(25);
    }

    // COMMIT_TIMESTAMP disabled

    @Test
    void commitTimestampDisabledShouldShiftSequence() {
        LogMinerColumnIndexes idx = fromConfig(OracleConnectorConfig.LOG_MINING_BUFFER_TRACK_COMMIT_TIMESTAMP, false);

        assertThat(idx.getStartTimestampIndex()).isEqualTo(24);
        assertThat(idx.getCommitTimestampIndex()).isNull();
        assertThat(idx.getSequenceIndex()).isEqualTo(25);
    }

    // All optional columns disabled

    @Test
    void allOptionalColumnsDisabledShouldProduceMinimumPositions() {
        OracleConnectorConfig config = new OracleConnectorConfig(
                TestHelper.defaultConfig()
                        .with(OracleConnectorConfig.LOG_MINING_BUFFER_TRACK_USERNAME, false)
                        .with(OracleConnectorConfig.LOG_MINING_BUFFER_TRACK_RS_ID, false)
                        .with(OracleConnectorConfig.LOG_MINING_BUFFER_TRACK_CLIENT_ID, false)
                        .with(OracleConnectorConfig.LOG_MINING_BUFFER_TRACK_START_TIMESTAMP, false)
                        .with(OracleConnectorConfig.LOG_MINING_BUFFER_TRACK_COMMIT_TIMESTAMP, false)
                        .build());
        LogMinerColumnIndexes idx = LogMinerColumnIndexes.fromConfig(config);

        // All optional indexes null
        assertThat(idx.getUsernameIndex()).isNull();
        assertThat(idx.getRsIdIndex()).isNull();
        assertThat(idx.getClientIdIndex()).isNull();
        assertThat(idx.getStartTimestampIndex()).isNull();
        assertThat(idx.getCommitTimestampIndex()).isNull();

        // Mandatory columns compressed to minimum positions (26 - 5 = 21 columns total)
        assertThat(idx.getRowIdIndex()).isEqualTo(10);
        assertThat(idx.getRollbackFlagIndex()).isEqualTo(11);
        assertThat(idx.getStatusIndex()).isEqualTo(12);
        assertThat(idx.getInfoIndex()).isEqualTo(13);
        assertThat(idx.getSsnIndex()).isEqualTo(14);
        assertThat(idx.getThreadIndex()).isEqualTo(15);
        assertThat(idx.getObjectIdIndex()).isEqualTo(16);
        assertThat(idx.getObjectVersionIndex()).isEqualTo(17);
        assertThat(idx.getDataObjectIdIndex()).isEqualTo(18);
        assertThat(idx.getStartScnIndex()).isEqualTo(19);
        assertThat(idx.getCommitScnIndex()).isEqualTo(20);
        assertThat(idx.getSequenceIndex()).isEqualTo(21);
    }

    // Resolver counts — verifies buildResolvers mirrors the column list

    // 12 mandatory cols (rowId, rollback, status, info, ssn, thread, objectId, objectVersion,
    // dataObjectId, startScn, commitScn, sequence) + 5 optional + 1 getSqlRedo = 18 total.
    private static final int RESOLVER_COUNT_ALL_ENABLED = 18;
    // With all 5 optional disabled: 18 - 5 = 13.
    private static final int RESOLVER_COUNT_ALL_DISABLED = 13;
    // With exactly one optional disabled: 18 - 1 = 17.
    private static final int RESOLVER_COUNT_ONE_DISABLED = 17;

    @Test
    void allTrackingEnabledShouldProduceFullResolverArray() {
        assertThat(fromDefaultConfig().getResolverCount()).isEqualTo(RESOLVER_COUNT_ALL_ENABLED);
    }

    @Test
    void allOptionalColumnsDisabledShouldProduceMinimumResolverArray() {
        OracleConnectorConfig config = new OracleConnectorConfig(
                TestHelper.defaultConfig()
                        .with(OracleConnectorConfig.LOG_MINING_BUFFER_TRACK_USERNAME, false)
                        .with(OracleConnectorConfig.LOG_MINING_BUFFER_TRACK_RS_ID, false)
                        .with(OracleConnectorConfig.LOG_MINING_BUFFER_TRACK_CLIENT_ID, false)
                        .with(OracleConnectorConfig.LOG_MINING_BUFFER_TRACK_START_TIMESTAMP, false)
                        .with(OracleConnectorConfig.LOG_MINING_BUFFER_TRACK_COMMIT_TIMESTAMP, false)
                        .build());
        assertThat(LogMinerColumnIndexes.fromConfig(config).getResolverCount()).isEqualTo(RESOLVER_COUNT_ALL_DISABLED);
    }

    @Test
    void usernameDisabledShouldReduceResolverCountByOne() {
        assertThat(fromConfig(OracleConnectorConfig.LOG_MINING_BUFFER_TRACK_USERNAME, false)
                .getResolverCount()).isEqualTo(RESOLVER_COUNT_ONE_DISABLED);
    }

    @Test
    void rsIdDisabledShouldReduceResolverCountByOne() {
        assertThat(fromConfig(OracleConnectorConfig.LOG_MINING_BUFFER_TRACK_RS_ID, false)
                .getResolverCount()).isEqualTo(RESOLVER_COUNT_ONE_DISABLED);
    }

    @Test
    void clientIdDisabledShouldReduceResolverCountByOne() {
        assertThat(fromConfig(OracleConnectorConfig.LOG_MINING_BUFFER_TRACK_CLIENT_ID, false)
                .getResolverCount()).isEqualTo(RESOLVER_COUNT_ONE_DISABLED);
    }

    @Test
    void startTimestampDisabledShouldReduceResolverCountByOne() {
        assertThat(fromConfig(OracleConnectorConfig.LOG_MINING_BUFFER_TRACK_START_TIMESTAMP, false)
                .getResolverCount()).isEqualTo(RESOLVER_COUNT_ONE_DISABLED);
    }

    @Test
    void commitTimestampDisabledShouldReduceResolverCountByOne() {
        assertThat(fromConfig(OracleConnectorConfig.LOG_MINING_BUFFER_TRACK_COMMIT_TIMESTAMP, false)
                .getResolverCount()).isEqualTo(RESOLVER_COUNT_ONE_DISABLED);
    }

    // Helpers

    private static LogMinerColumnIndexes fromDefaultConfig() {
        return LogMinerColumnIndexes.fromConfig(new OracleConnectorConfig(TestHelper.defaultConfig().build()));
    }

    private static LogMinerColumnIndexes fromConfig(io.debezium.config.Field field, Object value) {
        return LogMinerColumnIndexes.fromConfig(
                new OracleConnectorConfig(TestHelper.defaultConfig().with(field, value).build()));
    }
}
