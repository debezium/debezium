/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaFactory;
import io.debezium.time.Conversions;

/**
 * @author Jiri Pechanec
 *
 */
public class SourceInfoTest {

    private SourceInfo source;

    @Before
    public void beforeEach() {
        source = new SourceInfo(new PostgresConnectorConfig(
                Configuration.create()
                        .with(CommonConnectorConfig.TOPIC_PREFIX, "serverX")
                        .with(PostgresConnectorConfig.DATABASE_NAME, "serverX")
                        .build()));
        source.update(Conversions.toInstantFromMicros(123_456_789L), new TableId("catalogNameX", "schemaNameX", "tableNameX"));
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
    @FixFor("DBZ-934")
    public void canHandleNullValues() {
        source.update(null, null, null, null, null, null);
    }

    @Test
    public void shouldHaveTimestamp() {
        assertThat(source.struct().getInt64("ts_ms")).isEqualTo(123_456L);
    }

    @Test
    public void schemaIsCorrect() {
        final Schema schema = SchemaBuilder.struct()
                .name("io.debezium.connector.postgresql.Source")
                .version(SchemaFactory.SOURCE_INFO_DEFAULT_SCHEMA_VERSION)
                .field("version", Schema.STRING_SCHEMA)
                .field("connector", Schema.STRING_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field("ts_ms", Schema.INT64_SCHEMA)
                .field("snapshot", SchemaFactory.get().snapshotRecordSchema())
                .field("db", Schema.STRING_SCHEMA)
                .field("sequence", Schema.OPTIONAL_STRING_SCHEMA)
                .field("ts_us", Schema.OPTIONAL_INT64_SCHEMA)
                .field("ts_ns", Schema.OPTIONAL_INT64_SCHEMA)
                .field("schema", Schema.STRING_SCHEMA)
                .field("table", Schema.STRING_SCHEMA)
                .field("txId", Schema.OPTIONAL_INT64_SCHEMA)
                .field("lsn", Schema.OPTIONAL_INT64_SCHEMA)
                .field("xmin", Schema.OPTIONAL_INT64_SCHEMA)
                .build();

        VerifyRecord.assertConnectSchemasAreEqual(null, source.struct().schema(), schema);
    }
}
