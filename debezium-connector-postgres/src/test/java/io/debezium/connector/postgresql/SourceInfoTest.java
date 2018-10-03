/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.fest.assertions.Assertions.assertThat;

import io.debezium.relational.TableId;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Jiri Pechanec
 *
 */
public class SourceInfoTest {

    private SourceInfo source;

    @Before
    public void beforeEach() {
        source = new SourceInfo("serverX", "databaseX");
        source.update(1L, new TableId("catalogNameX", "schemaNameX", "tableNameX"));
    }

    @Test
    public void versionIsPresent() {
        assertThat(source.source().getString(SourceInfo.DEBEZIUM_VERSION_KEY)).isEqualTo(Module.version());
    }

    @Test
    public void connectorIsPresent() {
        assertThat(source.source().getString(SourceInfo.DEBEZIUM_CONNECTOR_KEY)).isEqualTo(Module.name());
    }
}
