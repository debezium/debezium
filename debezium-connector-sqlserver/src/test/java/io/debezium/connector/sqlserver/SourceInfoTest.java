/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.fest.assertions.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;

import io.debezium.relational.RelationalDatabaseSourceInfo;
import io.debezium.relational.TableId;

public class SourceInfoTest {

    private SourceInfo source;
    private TableId tableId;

    @Before
    public void beforeEach() {
        source = new SourceInfo("serverX");
        source.setChangeLsn(Lsn.NULL);
        tableId = new TableId("c", "s", "t");
    }

    @Test
    public void versionIsPresent() {
        assertThat(source.struct(tableId).getString(SourceInfo.DEBEZIUM_VERSION_KEY)).isEqualTo(Module.version());
    }

    @Test
    public void connectorIsPresent() {
        assertThat(source.struct(tableId).getString(SourceInfo.DEBEZIUM_CONNECTOR_KEY)).isEqualTo(Module.name());
    }

    @Test
    public void tableIdIsPresent() {
        assertThat(source.struct(tableId).getString(RelationalDatabaseSourceInfo.DB_NAME_KEY)).isEqualTo("c");
        assertThat(source.struct(tableId).getString(RelationalDatabaseSourceInfo.SCHEMA_NAME_KEY)).isEqualTo("s");
        assertThat(source.struct(tableId).getString(RelationalDatabaseSourceInfo.TABLE_NAME_KEY)).isEqualTo("t");
    }
}
