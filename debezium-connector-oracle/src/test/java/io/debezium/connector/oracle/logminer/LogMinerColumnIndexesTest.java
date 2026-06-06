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
 * <p>Fixed positions 1–21 are constants and never shift; they are tested here for completeness
 * but would never regress unless changed deliberately. Positions 10+ can shift when optional
 * columns are omitted via configuration flags, so every combination of omission is exercised.
 */
@SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.ANY_LOGMINER)
public class LogMinerColumnIndexesTest {

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
        assertThat(LogMinerColumnIndexes.ROW_ID).isEqualTo(10);
        assertThat(LogMinerColumnIndexes.ROLLBACK).isEqualTo(11);
        assertThat(LogMinerColumnIndexes.STATUS).isEqualTo(12);
        assertThat(LogMinerColumnIndexes.INFO).isEqualTo(13);
        assertThat(LogMinerColumnIndexes.SSN).isEqualTo(14);
        assertThat(LogMinerColumnIndexes.THREAD).isEqualTo(15);
        assertThat(LogMinerColumnIndexes.DATA_OBJ).isEqualTo(16);
        assertThat(LogMinerColumnIndexes.DATA_OBJV).isEqualTo(17);
        assertThat(LogMinerColumnIndexes.DATA_OBJD).isEqualTo(18);
        assertThat(LogMinerColumnIndexes.START_SCN).isEqualTo(19);
        assertThat(LogMinerColumnIndexes.COMMIT_SCN).isEqualTo(20);
        assertThat(LogMinerColumnIndexes.SEQUENCE).isEqualTo(21);
    }

    @Test
    void allTrackingEnabledShouldProduceBaselineOrdinals() {
        LogMinerColumnIndexes idx = fromDefaultConfig();

        // Optional columns present
        assertThat(idx.getStartTimestampIndex()).isEqualTo(22);
        assertThat(idx.getCommitTimestampIndex()).isEqualTo(23);
        assertThat(idx.getRsIdIndex()).isEqualTo(24);
        assertThat(idx.getUsernameIndex()).isEqualTo(25);
        assertThat(idx.getClientIdIndex()).isEqualTo(26);
    }

    @Test
    void usernameDisabledShouldShiftSubsequentColumns() {
        LogMinerColumnIndexes idx = fromConfig(OracleConnectorConfig.LOG_MINING_BUFFER_TRACK_USERNAME, false);

        // Columns after USERNAME each shift down by 1
        assertThat(idx.getUsernameIndex()).isNull();
        assertThat(idx.getClientIdIndex()).isEqualTo(25);
    }

    @Test
    void rsIdDisabledShouldShiftColumnsAfterRollback() {
        LogMinerColumnIndexes idx = fromConfig(OracleConnectorConfig.LOG_MINING_BUFFER_TRACK_RS_ID, false);

        // Columns before RS_ID are unaffected
        assertThat(idx.getRsIdIndex()).isNull();
        assertThat(idx.getUsernameIndex()).isEqualTo(24);
        assertThat(idx.getClientIdIndex()).isEqualTo(25);
    }

    @Test
    void clientIdDisabledShouldShiftColumnsAfterDataObjectId() {
        LogMinerColumnIndexes idx = fromConfig(OracleConnectorConfig.LOG_MINING_BUFFER_TRACK_CLIENT_ID, false);

        // Columns before CLIENT_ID are unaffected
        assertThat(idx.getClientIdIndex()).isNull();
    }

    @Test
    void startTimestampDisabledShouldShiftCommitTimestampAndSequence() {
        LogMinerColumnIndexes idx = fromConfig(OracleConnectorConfig.LOG_MINING_BUFFER_TRACK_START_TIMESTAMP, false);

        // All positions up to START_TIMESTAMP are unaffected
        assertThat(idx.getStartTimestampIndex()).isNull();
        assertThat(idx.getCommitTimestampIndex()).isEqualTo(22);
        assertThat(idx.getRsIdIndex()).isEqualTo(23);
        assertThat(idx.getUsernameIndex()).isEqualTo(24);
        assertThat(idx.getClientIdIndex()).isEqualTo(25);
    }

    @Test
    void commitTimestampDisabledShouldShiftSequence() {
        LogMinerColumnIndexes idx = fromConfig(OracleConnectorConfig.LOG_MINING_BUFFER_TRACK_COMMIT_TIMESTAMP, false);

        // All positions up to COMMIT_TIMESTAMP are unaffected
        assertThat(idx.getCommitTimestampIndex()).isNull();
        assertThat(idx.getRsIdIndex()).isEqualTo(23);
        assertThat(idx.getUsernameIndex()).isEqualTo(24);
        assertThat(idx.getClientIdIndex()).isEqualTo(25);
    }

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

        // All optional indexes null, all fixed columns are unaffected
        assertThat(idx.getUsernameIndex()).isNull();
        assertThat(idx.getRsIdIndex()).isNull();
        assertThat(idx.getClientIdIndex()).isNull();
        assertThat(idx.getStartTimestampIndex()).isNull();
        assertThat(idx.getCommitTimestampIndex()).isNull();
    }

    // Resolver counts — verifies buildResolvers mirrors the column list

    private static final int RESOLVER_COUNT_ALL_ENABLED = 5;
    private static final int RESOLVER_COUNT_ALL_DISABLED = 0;
    private static final int RESOLVER_COUNT_ONE_DISABLED = RESOLVER_COUNT_ALL_ENABLED - 1;

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
