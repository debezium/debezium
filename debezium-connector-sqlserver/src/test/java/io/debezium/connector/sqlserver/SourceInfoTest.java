/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.AbstractSourceInfoStructMaker;
import io.debezium.connector.SnapshotRecord;
import io.debezium.relational.TableId;

public class SourceInfoTest {

    private SourceInfo source;

    @Before
    public void beforeEach() {
        final SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(
                Configuration.create()
                        .with(CommonConnectorConfig.TOPIC_PREFIX, "serverX")
                        .build());
        source = new SourceInfo(connectorConfig);
        source.setChangeLsn(Lsn.valueOf(new byte[]{ 0x01 }));
        source.setCommitLsn(Lsn.valueOf(new byte[]{ 0x02 }));
        source.setSnapshot(SnapshotRecord.TRUE);
        source.setSourceTime(Instant.ofEpochMilli(3000));
        source.setTableId(new TableId("c", "s", "t"));
        source.setEventSerialNo(30L);
    }

    @Test
    public void versionIsPresent() {
        assertThat(source.struct().getString(SourceInfo.DEBEZIUM_VERSION_KEY)).isEqualTo(Module.version());
    }

    @Test
    public void connectorIsPresent() {
        assertThat(source.struct().getString(SourceInfo.DEBEZIUM_CONNECTOR_KEY)).isEqualTo(Module.name());
    }

    @Test
    public void serverNameIsPresent() {
        assertThat(source.struct().getString(SourceInfo.SERVER_NAME_KEY)).isEqualTo("serverX");
    }

    @Test
    public void changeLsnIsPresent() {
        assertThat(source.struct().getString(SourceInfo.CHANGE_LSN_KEY)).isEqualTo(Lsn.valueOf(new byte[]{ 0x01 }).toString());
    }

    @Test
    public void commitLsnIsPresent() {
        assertThat(source.struct().getString(SourceInfo.COMMIT_LSN_KEY)).isEqualTo(Lsn.valueOf(new byte[]{ 0x02 }).toString());
    }

    @Test
    public void eventSerialNoIsPresent() {
        assertThat(source.struct().getInt64(SourceInfo.EVENT_SERIAL_NO_KEY)).isEqualTo(30L);
    }

    @Test
    public void snapshotIsPresent() {
        assertThat(source.struct().getString(SourceInfo.SNAPSHOT_KEY)).isEqualTo("true");
    }

    @Test
    public void timestampIsPresent() {
        assertThat(source.struct().getInt64(SourceInfo.TIMESTAMP_KEY)).isEqualTo(3000);
    }

    @Test
    public void tableIdIsPresent() {
        assertThat(source.struct().getString(SourceInfo.DATABASE_NAME_KEY)).isEqualTo("c");
        assertThat(source.struct().getString(SourceInfo.SCHEMA_NAME_KEY)).isEqualTo("s");
        assertThat(source.struct().getString(SourceInfo.TABLE_NAME_KEY)).isEqualTo("t");
    }

    @Test
    public void schemaIsCorrect() {
        final Schema schema = SchemaBuilder.struct()
                .name("io.debezium.connector.sqlserver.Source")
                .field("version", Schema.STRING_SCHEMA)
                .field("connector", Schema.STRING_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field("ts_ms", Schema.INT64_SCHEMA)
                .field("snapshot", AbstractSourceInfoStructMaker.SNAPSHOT_RECORD_SCHEMA)
                .field("db", Schema.STRING_SCHEMA)
                .field("sequence", Schema.OPTIONAL_STRING_SCHEMA)
                .field("schema", Schema.STRING_SCHEMA)
                .field("table", Schema.STRING_SCHEMA)
                .field("change_lsn", Schema.OPTIONAL_STRING_SCHEMA)
                .field("commit_lsn", Schema.OPTIONAL_STRING_SCHEMA)
                .field("event_serial_no", Schema.OPTIONAL_INT64_SCHEMA)
                .build();

        assertThat(source.struct().schema()).isEqualTo(schema);
    }
}
