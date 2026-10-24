/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;

@Tag("UnitTests")
class JdbcKafkaSinkRecordTest extends AbstractBaseJdbcSinkTest {

    @Test
    @FixFor("debezium/dbz#2917")
    void shouldUsePrimaryKeyFieldAsColumnForPrimitiveKey() {
        final JdbcSinkConnectorConfig config = getConfig(Map.of(
                JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, "record_key",
                JdbcSinkConnectorConfig.PRIMARY_KEY_FIELDS, "id"));
        final Schema valueSchema = SchemaBuilder.struct()
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        final SinkRecord kafkaRecord = new SinkRecord("topic", 0,
                Schema.OPTIONAL_STRING_SCHEMA, "4711_0001_X",
                valueSchema, new Struct(valueSchema).put("name", "John"),
                42);

        final JdbcKafkaSinkRecord record = new JdbcKafkaSinkRecord(kafkaRecord, config);

        assertThat(record.keyFieldNames()).containsExactly("id");
        assertThat(record.nonKeyFieldNames()).containsExactly("name");
        assertThat(record.jdbcFields().get("id").getSchema()).isEqualTo(Schema.OPTIONAL_STRING_SCHEMA);
        assertThat(record.jdbcFields().get("id").isKey()).isTrue();
        assertThat(record.filteredKey().get("id")).isEqualTo("4711_0001_X");
    }
}
