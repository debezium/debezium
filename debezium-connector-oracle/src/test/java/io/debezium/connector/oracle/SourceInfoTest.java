/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.relational.TableId;

import java.time.Instant;

import static org.fest.assertions.Assertions.assertThat;

public class SourceInfoTest {

    private SourceInfo source;

    @Before
    public void beforeEach() {
        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(
                Configuration.create()
                    .with(OracleConnectorConfig.SERVER_NAME, "serverX")
                    .with(OracleConnectorConfig.DATABASE_NAME, "mydb")
                    .build()
        );
        source = new SourceInfo(connectorConfig);
        source.setSourceTime(Instant.now());
        source.setTableId(new TableId("c", "s", "t"));
    }

    @Test
    public void versionIsPresent() {
        assertThat(source.struct().getString(SourceInfo.DEBEZIUM_VERSION_KEY)).isEqualTo(Module.version());
    }

    @Test
    public void connectorIsPresent() {
        assertThat(source.struct().getString(SourceInfo.DEBEZIUM_CONNECTOR_KEY)).isEqualTo(Module.name());
    }
}
