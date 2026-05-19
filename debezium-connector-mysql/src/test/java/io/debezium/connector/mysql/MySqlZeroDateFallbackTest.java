/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.connector.mysql.jdbc.MySqlDefaultValueConverter;
import io.debezium.connector.mysql.jdbc.MySqlValueConverters;
import io.debezium.connector.mysql.util.MySqlValueConvertersFactory;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;

/**
 * Verifies that the per-type {@code zero.date.fallback.value} configuration
 * (general default + DATE/DATETIME/TIMESTAMP overrides) drives the sentinel
 * emitted by {@link io.debezium.connector.binlog.jdbc.BinlogDefaultValueConverter}
 * when a non-nullable column has a {@code DEFAULT '0000-00-00'} clause.
 *
 * <p>This test focuses on the DDL-parsing path. The runtime (row) path is exercised
 * by the integration test {@code BinlogDefaultValueAllZeroTimeIT}.
 */
public class MySqlZeroDateFallbackTest {

    private MySqlDefaultValueConverter buildConverter(Configuration extra) {
        Configuration.Builder builder = Configuration.create()
                .with(BinlogConnectorConfig.HOSTNAME, "localhost")
                .with(BinlogConnectorConfig.PORT, 3306)
                .with(BinlogConnectorConfig.USER, "u")
                .with(BinlogConnectorConfig.PASSWORD, "p")
                .with(BinlogConnectorConfig.SERVER_ID, 1)
                .with(BinlogConnectorConfig.TOPIC_PREFIX, "t");
        for (var entry : extra.asMap().entrySet()) {
            builder = builder.with(entry.getKey(), entry.getValue());
        }
        // The factory honors ZERO_DATE_FALLBACK_VALUE* keys via MySqlConnectorConfig.getZeroDateFallback.
        MySqlValueConverters valueConverters = new MySqlValueConvertersFactory().create(builder.build(), x -> x);
        return new MySqlDefaultValueConverter(valueConverters);
    }

    private Table tableWithColumn(String columnDef) {
        String sql = "CREATE TABLE T (A " + columnDef + ");";
        Tables tables = new Tables();
        new MySqlAntlrDdlParser().parse(sql, tables);
        return tables.forTable(new TableId(null, null, "T"));
    }

    @Test
    void datetimeUsesGeneralDefaultWhenNoOverride() {
        Configuration cfg = Configuration.create()
                .with(BinlogConnectorConfig.ZERO_DATE_FALLBACK_VALUE, "9999-12-31")
                .build();
        MySqlDefaultValueConverter converter = buildConverter(cfg);
        Table table = tableWithColumn("DATETIME NOT NULL DEFAULT '0000-00-00 00:00:00'");
        Optional<Object> def = converter.parseDefaultValue(table.columnWithName("A"), "0000-00-00 00:00:00");

        long expected = LocalDate.of(9999, 12, 31).atStartOfDay(java.time.ZoneOffset.UTC).toInstant().toEpochMilli();
        assertThat(def).isPresent().get().isEqualTo(expected);
    }

    @Test
    void datetimeOverrideTakesPrecedenceOverGeneral() {
        Configuration cfg = Configuration.create()
                .with(BinlogConnectorConfig.ZERO_DATE_FALLBACK_VALUE, "1970-01-01")
                .with(BinlogConnectorConfig.ZERO_DATE_FALLBACK_VALUE_DATETIME, "1001-01-01")
                .build();
        MySqlDefaultValueConverter converter = buildConverter(cfg);
        Table table = tableWithColumn("DATETIME NOT NULL DEFAULT '0000-00-00 00:00:00'");
        Optional<Object> def = converter.parseDefaultValue(table.columnWithName("A"), "0000-00-00 00:00:00");

        long expected = LocalDate.of(1001, 1, 1).atStartOfDay(java.time.ZoneOffset.UTC).toInstant().toEpochMilli();
        assertThat(def).isPresent().get().isEqualTo(expected); // -30578688000000L
    }

    @Test
    void dateOverrideAffectsOnlyDateColumn() {
        Configuration cfg = Configuration.create()
                .with(BinlogConnectorConfig.ZERO_DATE_FALLBACK_VALUE_DATE, "1001-01-01")
                .build();
        MySqlDefaultValueConverter converter = buildConverter(cfg);
        Table dateTable = tableWithColumn("DATE NOT NULL DEFAULT '0000-00-00'");
        Optional<Object> dateDef = converter.parseDefaultValue(dateTable.columnWithName("A"), "0000-00-00");
        assertThat(dateDef).isPresent().get().isEqualTo((int) LocalDate.of(1001, 1, 1).toEpochDay()); // -353920

        // DATETIME column should still use the general (epoch) default
        Table dtTable = tableWithColumn("DATETIME NOT NULL DEFAULT '0000-00-00 00:00:00'");
        Optional<Object> dtDef = converter.parseDefaultValue(dtTable.columnWithName("A"), "0000-00-00 00:00:00");
        assertThat(dtDef).isPresent().get().isEqualTo(0L); // 1970-01-01 epoch millis
    }

    @Test
    void nullableColumnEmitsNullRegardlessOfSentinel() {
        Configuration cfg = Configuration.create()
                .with(BinlogConnectorConfig.ZERO_DATE_FALLBACK_VALUE, "9999-12-31")
                .build();
        MySqlDefaultValueConverter converter = buildConverter(cfg);
        Table table = tableWithColumn("DATETIME NULL DEFAULT '0000-00-00 00:00:00'");
        Optional<Object> def = converter.parseDefaultValue(table.columnWithName("A"), "0000-00-00 00:00:00");
        assertThat(def).isEmpty();
    }

    @Test
    void backwardCompatibilityWhenNothingConfigured() {
        Configuration cfg = Configuration.empty();
        MySqlDefaultValueConverter converter = buildConverter(cfg);
        Table dtTable = tableWithColumn("DATETIME NOT NULL DEFAULT '0000-00-00 00:00:00'");
        Optional<Object> dtDef = converter.parseDefaultValue(dtTable.columnWithName("A"), "0000-00-00 00:00:00");
        assertThat(dtDef).isPresent().get().isEqualTo(0L);

        Table dateTable = tableWithColumn("DATE NOT NULL DEFAULT '0000-00-00'");
        Optional<Object> dateDef = converter.parseDefaultValue(dateTable.columnWithName("A"), "0000-00-00");
        assertThat(dateDef).isPresent().get().isEqualTo(0); // epoch days

        Table tsTable = tableWithColumn("TIMESTAMP NOT NULL DEFAULT '0000-00-00 00:00:00'");
        Optional<Object> tsDef = converter.parseDefaultValue(tsTable.columnWithName("A"), "0000-00-00 00:00:00");
        // TIMESTAMP_WITH_TZ default is the ZonedTimestamp ISO string at the configured offset (UTC).
        assertThat(tsDef).isPresent().get().isEqualTo("1970-01-01T00:00:00Z");
    }

    @Test
    void timestampOverrideAffectsOnlyTimestampColumn() {
        Configuration cfg = Configuration.create()
                .with(BinlogConnectorConfig.ZERO_DATE_FALLBACK_VALUE_TIMESTAMP, "9999-12-31")
                .build();
        MySqlDefaultValueConverter converter = buildConverter(cfg);
        Table tsTable = tableWithColumn("TIMESTAMP NOT NULL DEFAULT '0000-00-00 00:00:00'");
        Optional<Object> tsDef = converter.parseDefaultValue(tsTable.columnWithName("A"), "0000-00-00 00:00:00");
        assertThat(tsDef).isPresent().get().isEqualTo("9999-12-31T00:00:00Z");

        Table dtTable = tableWithColumn("DATETIME NOT NULL DEFAULT '0000-00-00 00:00:00'");
        Optional<Object> dtDef = converter.parseDefaultValue(dtTable.columnWithName("A"), "0000-00-00 00:00:00");
        // DATETIME falls back to the general default (epoch)
        assertThat(dtDef).isPresent().get().isEqualTo(0L);
    }

    /** Sanity check that {@link Timestamp#valueOf(String)} fails on a zero date — so the conversion
     *  path must rely on the sentinel rather than the JDBC driver. */
    @Test
    void zeroDateStringIsRejectedByJavaSqlTimestamp() {
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> Timestamp.valueOf("0000-00-00 00:00:00"))
                .isInstanceOf(IllegalArgumentException.class);
        // Also verify that the non-zero fallback parses fine
        LocalDateTime parsed = LocalDateTime.parse("1001-01-01T00:00:00");
        assertThat(parsed.toLocalDate()).isEqualTo(LocalDate.of(1001, 1, 1));
    }
}
