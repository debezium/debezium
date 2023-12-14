/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

/**
 * Test paring of various Oracle version strings.
 *
 * @author vjuranek
 */
public class OracleDatabaseVersionTest {

    @Test
    public void shouldParseOracle19c() throws Exception {
        String banner = "Oracle Database 19c Enterprise Edition Release 19.0.0.0.0 - Production\nVersion 19.3.0.0.0";
        OracleDatabaseVersion version = OracleDatabaseVersion.parse(banner);
        assertOracleVersion(version, 19, 3, 0, 0, 0, banner);
    }

    @Test
    public void shouldParseOracle21c() throws Exception {
        String banner = "Oracle Database 21c Express Edition Release 21.0.0.0.0 - Production\nVersion 21.3.0.0.0";
        OracleDatabaseVersion version = OracleDatabaseVersion.parse(banner);
        assertOracleVersion(version, 21, 3, 0, 0, 0, banner);
    }

    @Test
    public void shouldParseOracle23c() throws Exception {
        String banner = "Oracle Database 23c Enterprise Edition Release 23.0.0.0.0\nVersion 23.4.0.23.10";
        OracleDatabaseVersion version = OracleDatabaseVersion.parse(banner);
        assertOracleVersion(version, 23, 4, 0, 23, 10, banner);
    }

    private void assertOracleVersion(
                                     OracleDatabaseVersion actual,
                                     int expectedMajor,
                                     int expectedMaintenance,
                                     int expectedAppServer,
                                     int expectedComponent,
                                     int expectedPlatform,
                                     String expectedBanner) {
        assertThat(actual.getMajor()).isEqualTo(expectedMajor);
        assertThat(actual.getMaintenance()).isEqualTo(expectedMaintenance);
        assertThat(actual.getAppServer()).isEqualTo(expectedAppServer);
        assertThat(actual.getComponent()).isEqualTo(expectedComponent);
        assertThat(actual.getPlatform()).isEqualTo(expectedPlatform);
        assertThat(actual.getBanner()).isEqualTo(expectedBanner);
    }

}
