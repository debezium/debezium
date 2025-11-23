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
    public void shouldParseOracleVersion() {
        String version = "23.4.0.23.10";
        OracleDatabaseVersion oracleDatabaseVersion = OracleDatabaseVersion.parse(version);
        assertThat(oracleDatabaseVersion.getMajor()).isEqualTo(23);
        assertThat(oracleDatabaseVersion.getMaintenance()).isEqualTo(4);
        assertThat(oracleDatabaseVersion.getAppServer()).isEqualTo(0);
        assertThat(oracleDatabaseVersion.getComponent()).isEqualTo(23);
        assertThat(oracleDatabaseVersion.getPlatform()).isEqualTo(10);
        assertThat(oracleDatabaseVersion.getVersion()).isEqualTo(version);
    }
}
