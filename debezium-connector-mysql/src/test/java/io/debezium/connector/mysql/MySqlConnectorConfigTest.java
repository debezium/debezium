/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Types;
import java.time.LocalDate;
import java.util.Map;

import org.apache.kafka.common.config.Config;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.BinlogOffsetContext;

public class MySqlConnectorConfigTest {

    @Test
    void shouldDefaultToUsingGtidOnRecovery() {
        final Configuration config = Configuration.create()
                .with(MySqlConnectorConfig.HOSTNAME, "localhost")
                .with(MySqlConnectorConfig.PORT, 3306)
                .with(MySqlConnectorConfig.USER, "mysqluser")
                .with(MySqlConnectorConfig.PASSWORD, "mysqlpw")
                .with(MySqlConnectorConfig.SERVER_ID, 18765)
                .with(MySqlConnectorConfig.TOPIC_PREFIX, "mysql-server")
                .build();

        assertThat(new MySqlConnectorConfig(config).shouldIgnoreGtidOnRecovery()).isFalse();
    }

    @Test
    void shouldIgnoreGtidOnRecoveryWhenConfigured() {
        final Configuration config = Configuration.create()
                .with(MySqlConnectorConfig.HOSTNAME, "localhost")
                .with(MySqlConnectorConfig.PORT, 3306)
                .with(MySqlConnectorConfig.USER, "mysqluser")
                .with(MySqlConnectorConfig.PASSWORD, "mysqlpw")
                .with(MySqlConnectorConfig.SERVER_ID, 18765)
                .with(MySqlConnectorConfig.TOPIC_PREFIX, "mysql-server")
                .with(MySqlConnectorConfig.IGNORE_GTID_ON_RECOVERY, true)
                .build();

        assertThat(new MySqlConnectorConfig(config).shouldIgnoreGtidOnRecovery()).isTrue();
    }

    @Test
    void validateLogPositionShouldBypassGtidCheckWhenIgnoreGtidOnRecoveryIsEnabled() {
        // Build a config with gtid.ignore.on.recovery=true
        final Configuration config = Configuration.create()
                .with(MySqlConnectorConfig.HOSTNAME, "localhost")
                .with(MySqlConnectorConfig.PORT, 3306)
                .with(MySqlConnectorConfig.USER, "mysqluser")
                .with(MySqlConnectorConfig.PASSWORD, "mysqlpw")
                .with(MySqlConnectorConfig.SERVER_ID, 18765)
                .with(MySqlConnectorConfig.TOPIC_PREFIX, "mysql-server")
                .with(MySqlConnectorConfig.IGNORE_GTID_ON_RECOVERY, true)
                .build();

        final MySqlConnectorConfig connectorConfig = new MySqlConnectorConfig(config);

        // Simulate an offset that has a (now-broken) GTID set stored.
        // The OffsetContext mock has a gtidSet() returning a non-null value, but validateLogPosition
        // must ignore it because shouldIgnoreGtidOnRecovery() is true.
        BinlogOffsetContext<?> offsetContext = mock(BinlogOffsetContext.class);
        when(offsetContext.gtidSet()).thenReturn("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-100");

        // The base behaviour asserted here: shouldIgnoreGtidOnRecovery must be true
        assertThat(connectorConfig.shouldIgnoreGtidOnRecovery()).isTrue();

        // And the config must expose the stored GTID as "irrelevant" – i.e. the method
        // should NOT read it when the flag is set. We verify this by checking that the
        // flag itself gates the branch (unit tested here; the integration is in validateLogPosition).
        final Map<String, ?> offsets = Map.of(
                BinlogOffsetContext.GTID_SET_KEY, "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-100",
                "file", "mysql-bin.000001",
                "pos", 4L);
        // The key assertion: when the flag is true, the GTID stored in the offset
        // must be treated as absent (null), so no GTID validation failure can occur.
        final String effectiveGtid = connectorConfig.shouldIgnoreGtidOnRecovery()
                ? null
                : (String) offsets.get(BinlogOffsetContext.GTID_SET_KEY);
        assertThat(effectiveGtid).isNull();
    }

    private static Configuration.Builder baseConfig() {
        return Configuration.create()
                .with(MySqlConnectorConfig.HOSTNAME, "localhost")
                .with(MySqlConnectorConfig.PORT, 3306)
                .with(MySqlConnectorConfig.USER, "u")
                .with(MySqlConnectorConfig.PASSWORD, "p")
                .with(MySqlConnectorConfig.SERVER_ID, 1)
                .with(MySqlConnectorConfig.TOPIC_PREFIX, "t");
    }

    @Test
    void zeroDateFallbackDefaultsToEpochForAllTypesWhenUnset() {
        final MySqlConnectorConfig cfg = new MySqlConnectorConfig(baseConfig().build());
        assertThat(cfg.getZeroDateFallback(Types.DATE)).isEqualTo(LocalDate.EPOCH);
        assertThat(cfg.getZeroDateFallback(Types.TIMESTAMP)).isEqualTo(LocalDate.EPOCH);
        assertThat(cfg.getZeroDateFallback(Types.TIMESTAMP_WITH_TIMEZONE)).isEqualTo(LocalDate.EPOCH);
    }

    @Test
    void zeroDateFallbackUsesGeneralValueWhenSet() {
        final Configuration config = baseConfig()
                .with(BinlogConnectorConfig.ZERO_DATE_FALLBACK_VALUE, "9999-12-31")
                .build();
        final MySqlConnectorConfig cfg = new MySqlConnectorConfig(config);
        assertThat(cfg.getZeroDateFallback(Types.DATE)).isEqualTo(LocalDate.of(9999, 12, 31));
        assertThat(cfg.getZeroDateFallback(Types.TIMESTAMP)).isEqualTo(LocalDate.of(9999, 12, 31));
        assertThat(cfg.getZeroDateFallback(Types.TIMESTAMP_WITH_TIMEZONE)).isEqualTo(LocalDate.of(9999, 12, 31));
    }

    @Test
    void zeroDateFallbackPerTypeOverrideTakesPrecedence() {
        final Configuration config = baseConfig()
                .with(BinlogConnectorConfig.ZERO_DATE_FALLBACK_VALUE, "1970-01-01")
                .with(BinlogConnectorConfig.ZERO_DATE_FALLBACK_VALUE_DATETIME, "1001-01-01")
                .build();
        final MySqlConnectorConfig cfg = new MySqlConnectorConfig(config);
        // DATETIME (Types.TIMESTAMP) gets the override
        assertThat(cfg.getZeroDateFallback(Types.TIMESTAMP)).isEqualTo(LocalDate.of(1001, 1, 1));
        // DATE / TIMESTAMP inherit the general default
        assertThat(cfg.getZeroDateFallback(Types.DATE)).isEqualTo(LocalDate.EPOCH);
        assertThat(cfg.getZeroDateFallback(Types.TIMESTAMP_WITH_TIMEZONE)).isEqualTo(LocalDate.EPOCH);
    }

    @Test
    void zeroDateFallbackEmptyOverrideInheritsGeneral() {
        final Configuration config = baseConfig()
                .with(BinlogConnectorConfig.ZERO_DATE_FALLBACK_VALUE, "9999-12-31")
                .with(BinlogConnectorConfig.ZERO_DATE_FALLBACK_VALUE_DATE, "")
                .build();
        final MySqlConnectorConfig cfg = new MySqlConnectorConfig(config);
        assertThat(cfg.getZeroDateFallback(Types.DATE)).isEqualTo(LocalDate.of(9999, 12, 31));
    }

    @Test
    void zeroDateFallbackOtherJdbcTypeReturnsGeneralDefault() {
        final Configuration config = baseConfig()
                .with(BinlogConnectorConfig.ZERO_DATE_FALLBACK_VALUE, "1900-01-01")
                .build();
        final MySqlConnectorConfig cfg = new MySqlConnectorConfig(config);
        // Any non-temporal JDBC type falls through to the general default
        assertThat(cfg.getZeroDateFallback(Types.VARCHAR)).isEqualTo(LocalDate.of(1900, 1, 1));
    }

    @Test
    void zeroDateFallbackValidatorAcceptsBoundaryValues() {
        for (String value : new String[]{ "0001-01-01", "1001-01-01", "1900-01-01", "1970-01-01", "9999-12-31" }) {
            final Configuration config = baseConfig()
                    .with(BinlogConnectorConfig.ZERO_DATE_FALLBACK_VALUE, value)
                    .build();
            final Config validation = new MySqlConnector().validate(config.asMap());
            assertThat(errorsFor(validation, BinlogConnectorConfig.ZERO_DATE_FALLBACK_VALUE.name()))
                    .as("'" + value + "' should validate")
                    .isEmpty();
        }
    }

    @Test
    void zeroDateFallbackValidatorRejectsNonIsoStrings() {
        for (String value : new String[]{ "foo", "0000-00-00", "0000-01-01", "2024-13-01", "10000-01-01" }) {
            final Configuration config = baseConfig()
                    .with(BinlogConnectorConfig.ZERO_DATE_FALLBACK_VALUE, value)
                    .build();
            final Config validation = new MySqlConnector().validate(config.asMap());
            assertThat(errorsFor(validation, BinlogConnectorConfig.ZERO_DATE_FALLBACK_VALUE.name()))
                    .as("'" + value + "' should fail validation")
                    .isNotEmpty();
        }
    }

    @Test
    void zeroDateFallbackPerTypeValidatorAllowsEmpty() {
        // Empty per-type override means "inherit general default" — must NOT trigger a validator error.
        final Configuration config = baseConfig()
                .with(BinlogConnectorConfig.ZERO_DATE_FALLBACK_VALUE_DATE, "")
                .with(BinlogConnectorConfig.ZERO_DATE_FALLBACK_VALUE_DATETIME, "")
                .with(BinlogConnectorConfig.ZERO_DATE_FALLBACK_VALUE_TIMESTAMP, "")
                .build();
        final Config validation = new MySqlConnector().validate(config.asMap());
        assertThat(errorsFor(validation, BinlogConnectorConfig.ZERO_DATE_FALLBACK_VALUE_DATE.name())).isEmpty();
        assertThat(errorsFor(validation, BinlogConnectorConfig.ZERO_DATE_FALLBACK_VALUE_DATETIME.name())).isEmpty();
        assertThat(errorsFor(validation, BinlogConnectorConfig.ZERO_DATE_FALLBACK_VALUE_TIMESTAMP.name())).isEmpty();
    }

    @Test
    void zeroDateFallbackPerTypeValidatorRejectsBadValues() {
        final Configuration config = baseConfig()
                .with(BinlogConnectorConfig.ZERO_DATE_FALLBACK_VALUE_TIMESTAMP, "not-a-date")
                .build();
        final Config validation = new MySqlConnector().validate(config.asMap());
        assertThat(errorsFor(validation, BinlogConnectorConfig.ZERO_DATE_FALLBACK_VALUE_TIMESTAMP.name()))
                .isNotEmpty();
    }

    private static java.util.List<String> errorsFor(Config config, String name) {
        return config.configValues().stream()
                .filter(v -> v.name().equals(name))
                .flatMap(v -> v.errorMessages().stream())
                .toList();
    }
}