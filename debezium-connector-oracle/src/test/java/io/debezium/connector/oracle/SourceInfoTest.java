/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.AbstractSourceInfoStructMaker;
import io.debezium.data.VerifyRecord;
import io.debezium.relational.TableId;

public class SourceInfoTest {

    private SourceInfo source;

    @Before
    public void beforeEach() {
        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(
                Configuration.create()
                        .with(CommonConnectorConfig.TOPIC_PREFIX, "serverX")
                        .with(OracleConnectorConfig.DATABASE_NAME, "mydb")
                        .build());
        source = new SourceInfo(connectorConfig);
        source.setSourceTime(Instant.now());
        source.tableEvent(new TableId("c", "s", "t"));
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
    public void schemaIsCorrect() {
        final Schema schema = SchemaBuilder.struct()
                .name("io.debezium.connector.oracle.Source")
                .field("version", Schema.STRING_SCHEMA)
                .field("connector", Schema.STRING_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field("ts_ms", Schema.INT64_SCHEMA)
                .field("snapshot", AbstractSourceInfoStructMaker.SNAPSHOT_RECORD_SCHEMA)
                .field("db", Schema.STRING_SCHEMA)
                .field("sequence", Schema.OPTIONAL_STRING_SCHEMA)
                .field("schema", Schema.STRING_SCHEMA)
                .field("table", Schema.STRING_SCHEMA)
                .field("txId", Schema.OPTIONAL_STRING_SCHEMA)
                .field("scn", Schema.OPTIONAL_STRING_SCHEMA)
                .field("commit_scn", Schema.OPTIONAL_STRING_SCHEMA)
                .field("lcr_position", Schema.OPTIONAL_STRING_SCHEMA)
                .field("rs_id", Schema.OPTIONAL_STRING_SCHEMA)
                .field("ssn", Schema.OPTIONAL_INT64_SCHEMA)
                .field("redo_thread", Schema.OPTIONAL_INT32_SCHEMA)
                .field("user_name", Schema.OPTIONAL_STRING_SCHEMA)
                .field("redo_sql", Schema.OPTIONAL_STRING_SCHEMA)
                .field("row_id", Schema.OPTIONAL_STRING_SCHEMA)
                .build();

        VerifyRecord.assertConnectSchemasAreEqual(null, source.schema(), schema);
    }
}
