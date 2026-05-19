/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Types;
import java.time.LocalDate;

import org.junit.jupiter.api.Test;

class ZeroDateFallbackTest {

    @Test
    void defaultEpochReturnsAllSlotsAtLocalDateEpoch() {
        ZeroDateFallback fallback = ZeroDateFallback.defaultEpoch();

        assertThat(fallback.forDate()).isEqualTo(LocalDate.EPOCH);
        assertThat(fallback.forDatetime()).isEqualTo(LocalDate.EPOCH);
        assertThat(fallback.forTimestamp()).isEqualTo(LocalDate.EPOCH);
    }

    @Test
    void defaultEpochReturnsCachedSingleton() {
        assertThat(ZeroDateFallback.defaultEpoch()).isSameAs(ZeroDateFallback.defaultEpoch());
    }

    @Test
    void forJdbcTypeMapsRecognizedTemporalTypes() {
        LocalDate forDate = LocalDate.of(1001, 1, 1);
        LocalDate forDatetime = LocalDate.of(1900, 1, 1);
        LocalDate forTimestamp = LocalDate.of(2000, 6, 15);
        ZeroDateFallback fallback = new ZeroDateFallback(forDate, forDatetime, forTimestamp);

        assertThat(fallback.forJdbcType(Types.DATE)).isEqualTo(forDate);
        assertThat(fallback.forJdbcType(Types.TIMESTAMP)).isEqualTo(forDatetime);
        assertThat(fallback.forJdbcType(Types.TIMESTAMP_WITH_TIMEZONE)).isEqualTo(forTimestamp);
    }

    @Test
    void forJdbcTypeReturnsEpochForUnrecognizedTypes() {
        ZeroDateFallback fallback = new ZeroDateFallback(
                LocalDate.of(9999, 12, 31), LocalDate.of(9999, 12, 31), LocalDate.of(9999, 12, 31));

        assertThat(fallback.forJdbcType(Types.VARCHAR)).isEqualTo(LocalDate.EPOCH);
        assertThat(fallback.forJdbcType(Types.INTEGER)).isEqualTo(LocalDate.EPOCH);
        assertThat(fallback.forJdbcType(Types.TIME)).isEqualTo(LocalDate.EPOCH);
    }

    @Test
    void constructorRejectsNullArguments() {
        assertThatThrownBy(() -> new ZeroDateFallback(null, LocalDate.EPOCH, LocalDate.EPOCH))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("forDate");
        assertThatThrownBy(() -> new ZeroDateFallback(LocalDate.EPOCH, null, LocalDate.EPOCH))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("forDatetime");
        assertThatThrownBy(() -> new ZeroDateFallback(LocalDate.EPOCH, LocalDate.EPOCH, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("forTimestamp");
    }

    @Test
    void equalsAndHashCodeAreValueBased() {
        ZeroDateFallback a = new ZeroDateFallback(
                LocalDate.of(1001, 1, 1), LocalDate.of(1900, 1, 1), LocalDate.of(2000, 1, 1));
        ZeroDateFallback b = new ZeroDateFallback(
                LocalDate.of(1001, 1, 1), LocalDate.of(1900, 1, 1), LocalDate.of(2000, 1, 1));
        ZeroDateFallback c = new ZeroDateFallback(
                LocalDate.of(1001, 1, 1), LocalDate.of(1900, 1, 1), LocalDate.of(2001, 1, 1));

        assertThat(a).isEqualTo(b).hasSameHashCodeAs(b);
        assertThat(a).isNotEqualTo(c);
        assertThat(a).isNotEqualTo("not a ZeroDateFallback");
    }

    @Test
    void toStringIncludesAllSlots() {
        ZeroDateFallback fallback = new ZeroDateFallback(
                LocalDate.of(1001, 1, 1), LocalDate.of(1900, 1, 1), LocalDate.of(2000, 6, 15));

        assertThat(fallback.toString())
                .contains("1001-01-01")
                .contains("1900-01-01")
                .contains("2000-06-15");
    }
}
