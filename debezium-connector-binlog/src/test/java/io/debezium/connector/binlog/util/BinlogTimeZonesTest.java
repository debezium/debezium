/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.ZoneId;
import java.time.ZoneOffset;

import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;

class BinlogTimeZonesTest {

    @Test
    @FixFor("debezium/dbz#2866")
    void shouldParseOffsetsAndNamedTimeZones() {
        assertThat(BinlogTimeZones.parse("+09:00")).isEqualTo(ZoneOffset.ofHours(9));
        assertThat(BinlogTimeZones.parse("-5:30")).isEqualTo(ZoneOffset.ofHoursMinutes(-5, -30));
        assertThat(BinlogTimeZones.parse("Asia/Seoul")).isEqualTo(ZoneId.of("Asia/Seoul"));
        assertThat(BinlogTimeZones.parse("KST")).isNull();
        assertThat(BinlogTimeZones.parse("SYSTEM")).isNull();
        assertThat(BinlogTimeZones.parse(null)).isNull();
    }

    @Test
    @FixFor("debezium/dbz#2866")
    void shouldPreferTimeZoneConfiguredForDriver() {
        // The configured time zone is assumed to be the time zone of the server, as for the values the driver converts
        assertThat(BinlogTimeZones.serverTimeZone("UTC", "Asia/Seoul")).isEqualTo(ZoneId.of("UTC"));
        assertThat(BinlogTimeZones.serverTimeZone("Asia/Seoul", "KST")).isEqualTo(ZoneId.of("Asia/Seoul"));
    }

    @Test
    @FixFor("debezium/dbz#2866")
    void shouldUseSystemTimeZoneOfServerWithoutConfiguredTimeZone() {
        // SERVER and auto, the defaults for MySQL and MariaDB, let the driver use the time zone of the server
        assertThat(BinlogTimeZones.serverTimeZone("SERVER", "Asia/Seoul")).isEqualTo(ZoneId.of("Asia/Seoul"));
        assertThat(BinlogTimeZones.serverTimeZone("auto", "+09:00")).isEqualTo(ZoneOffset.ofHours(9));
        assertThat(BinlogTimeZones.serverTimeZone(null, "UTC")).isEqualTo(ZoneId.of("UTC"));
    }

    @Test
    @FixFor("debezium/dbz#2866")
    void shouldNotResolveAbbreviatedSystemTimeZone() {
        assertThat(BinlogTimeZones.serverTimeZone("SERVER", "KST")).isNull();
    }
}
