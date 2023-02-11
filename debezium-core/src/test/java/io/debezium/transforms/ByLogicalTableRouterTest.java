/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import io.debezium.doc.FixFor;
import io.debezium.relational.history.HistoryRecord.Fields;

/**
 * @author Mario Mueller
 */
public class ByLogicalTableRouterTest {

    @Test(expected = ConnectException.class)
    public void testBrokenKeyReplacementConfigurationNullValue() {
        final ByLogicalTableRouter<SourceRecord> subject = new ByLogicalTableRouter<>();
        final Map<String, String> props = new HashMap<>();

        props.put("topic.regex", "someValidRegex(.*)");
        props.put("topic.replacement", "$1");
        props.put("key.field.regex", "If this is set, key.field.replacement must be non-empty");
        subject.configure(props);
    }

    @Test(expected = ConnectException.class)
    public void testBrokenKeyReplacementConfigurationEmptyValue() {
        final ByLogicalTableRouter<SourceRecord> subject = new ByLogicalTableRouter<>();
        final Map<String, String> props = new HashMap<>();

        props.put("topic.regex", "someValidRegex(.*)");
        props.put("topic.replacement", "$1");
        props.put("key.field.regex", "If this is set, key.field.replacement must be non-empty");
        props.put("key.field.replacement", "");
        subject.configure(props);
    }

    @Test
    public void testKeyReplacementWorkingConfigurationWithAvroSchemaNameAdjustment() {
        testKeyReplacementWorkingConfiguration("avro",
                "mysql_server_1.inventory.customers_all_shards.Key");
    }

    @Test
    @FixFor("DBZ-3535")
    public void testKeyReplacementWorkingConfigurationWithNoSchemaNameAdjustment() {
        testKeyReplacementWorkingConfiguration("none",
                "mysql-server-1.inventory.customers_all_shards.Key");
    }

    private void testKeyReplacementWorkingConfiguration(String schemaNameAdjustmentMode, String expectedKeySchemaName) {
        final ByLogicalTableRouter<SourceRecord> router = new ByLogicalTableRouter<>();
        final Map<String, String> props = new HashMap<>();

        props.put("topic.regex", "(.*)customers_shard(.*)");
        props.put("topic.replacement", "$1customers_all_shards");
        props.put("key.field.name", "shard_id");
        props.put("key.field.regex", "(.*)customers_shard_(.*)");
        props.put("key.field.replacement", "$2");
        props.put("schema.name.adjustment.mode", schemaNameAdjustmentMode);
        router.configure(props);

        Schema keySchema = SchemaBuilder.struct()
                .name("mysql-server-1.inventory.customers_shard_1.Key")
                .field("id", SchemaBuilder.int64().build())
                .build();

        Struct key1 = new Struct(keySchema).put("id", 123L);

        SourceRecord record1 = new SourceRecord(
                new HashMap<>(), new HashMap<>(), "mysql-server-1.inventory.customers_shard_1", keySchema, key1, null, null);

        SourceRecord transformed1 = router.apply(record1);
        assertThat(transformed1).isNotNull();
        assertThat(transformed1.topic()).isEqualTo("mysql-server-1.inventory.customers_all_shards");

        assertThat(transformed1.keySchema().name()).isEqualTo(expectedKeySchemaName);
        assertThat(transformed1.keySchema().fields()).hasSize(2);
        assertThat(transformed1.keySchema().fields().get(0).name()).isEqualTo("id");
        assertThat(transformed1.keySchema().fields().get(1).name()).isEqualTo("shard_id");

        assertThat(((Struct) transformed1.key()).get("id")).isEqualTo(123L);
        assertThat(((Struct) transformed1.key()).get("shard_id")).isEqualTo("1");

        Struct key2 = new Struct(keySchema).put("id", 123L);

        SourceRecord record2 = new SourceRecord(
                new HashMap<>(), new HashMap<>(), "mysql-server-1.inventory.customers_shard_2", keySchema, key2, null, null);

        SourceRecord transformed2 = router.apply(record2);
        assertThat(transformed2).isNotNull();
        assertThat(transformed2.topic()).isEqualTo("mysql-server-1.inventory.customers_all_shards");

        assertThat(transformed2.keySchema().name()).isEqualTo(expectedKeySchemaName);
        assertThat(transformed2.keySchema().fields()).hasSize(2);
        assertThat(transformed2.keySchema().fields().get(0).name()).isEqualTo("id");
        assertThat(transformed2.keySchema().fields().get(1).name()).isEqualTo("shard_id");

        assertThat(((Struct) transformed2.key()).get("id")).isEqualTo(123L);
        assertThat(((Struct) transformed2.key()).get("shard_id")).isEqualTo("2");

        Struct key3 = new Struct(keySchema).put("id", 456L);

        SourceRecord record3 = new SourceRecord(
                new HashMap<>(), new HashMap<>(), "mysql-server-1.inventory.customers_shard_2", keySchema, key3, null, null);

        SourceRecord transformed3 = router.apply(record3);
        assertThat(transformed3).isNotNull();
        assertThat(transformed3.topic()).isEqualTo("mysql-server-1.inventory.customers_all_shards");

        assertThat(transformed3.keySchema().name()).isEqualTo(expectedKeySchemaName);
        assertThat(transformed3.keySchema().fields()).hasSize(2);
        assertThat(transformed3.keySchema().fields().get(0).name()).isEqualTo("id");
        assertThat(transformed3.keySchema().fields().get(1).name()).isEqualTo("shard_id");

        assertThat(((Struct) transformed3.key()).get("id")).isEqualTo(456L);
        assertThat(((Struct) transformed3.key()).get("shard_id")).isEqualTo("2");
    }

    @Test
    public void testHeartbeatMessageTopicReplacementTopic() {
        final ByLogicalTableRouter<SourceRecord> router = new ByLogicalTableRouter<>();
        final Map<String, String> props = new HashMap<>();
        final String keyFieldName = "serverName";
        final String keyOriginalKeyTopic = "originalTopic";
        final String replacedTopic = "debezium_heartbeat_central";

        props.put("topic.regex", "__debezium-heartbeat(.*)");
        props.put("topic.replacement", replacedTopic);
        props.put("key.field.name", keyOriginalKeyTopic);
        router.configure(props);

        Schema keySchema = SchemaBuilder.struct()
                .name("io.debezium.connector.mysql.ServerNameKey")
                .field(keyFieldName, Schema.STRING_SCHEMA)
                .build();

        Struct key1 = new Struct(keySchema).put(keyFieldName, "test_server_name_db");
        SourceRecord record1 = new SourceRecord(
                new HashMap<>(), new HashMap<>(), "__debezium-heartbeat.test_server_name_db", keySchema, key1, null, null);

        SourceRecord transformed1 = router.apply(record1);
        assertThat(transformed1).isNotNull();
        assertThat(transformed1.topic()).isEqualTo(replacedTopic);

        assertThat(transformed1.keySchema().name()).isEqualTo(replacedTopic + ".Key");
        assertThat(transformed1.keySchema().fields()).hasSize(2);
        assertThat(transformed1.keySchema().fields().get(0).name()).isEqualTo(keyFieldName);
        assertThat(transformed1.keySchema().fields().get(1).name()).isEqualTo(keyOriginalKeyTopic);

        assertThat(((Struct) transformed1.key()).get(keyFieldName)).isEqualTo("test_server_name_db");
        assertThat(((Struct) transformed1.key()).get(keyOriginalKeyTopic)).isEqualTo("__debezium-heartbeat.test_server_name_db");

        assertThat(transformed1.value()).isNull();

        Struct key2 = new Struct(keySchema).put(keyFieldName, "test_server_name_db_2");
        SourceRecord record2 = new SourceRecord(
                new HashMap<>(), new HashMap<>(), "__debezium-heartbeat.test_server_name_db_2", keySchema, key2, null, null);

        SourceRecord transformed2 = router.apply(record2);
        assertThat(transformed2).isNotNull();
        assertThat(transformed2.topic()).isEqualTo(replacedTopic);

        assertThat(transformed2.keySchema().name()).isEqualTo(replacedTopic + ".Key");
        assertThat(transformed2.keySchema().fields()).hasSize(2);
        assertThat(transformed2.keySchema().fields().get(0).name()).isEqualTo(keyFieldName);
        assertThat(transformed2.keySchema().fields().get(1).name()).isEqualTo(keyOriginalKeyTopic);

        assertThat(((Struct) transformed2.key()).get(keyFieldName)).isEqualTo("test_server_name_db_2");
        assertThat(((Struct) transformed2.key()).get(keyOriginalKeyTopic)).isEqualTo("__debezium-heartbeat.test_server_name_db_2");

        assertThat(transformed2.value()).isNull();
    }

    @Test(expected = ConnectException.class)
    public void testBrokenTopicReplacementConfigurationNullValue() {
        final ByLogicalTableRouter<SourceRecord> subject = new ByLogicalTableRouter<>();
        final Map<String, String> props = new HashMap<>();

        // topic.replacement is not set, therefore null. Must crash.
        props.put("topic.regex", "someValidRegex(.*)");
        subject.configure(props);
    }

    @Test(expected = ConnectException.class)
    public void testBrokenTopicReplacementConfigurationEmptyValue() {
        final ByLogicalTableRouter<SourceRecord> subject = new ByLogicalTableRouter<>();
        final Map<String, String> props = new HashMap<>();

        // topic.replacement is set to empty string. Must crash.
        props.put("topic.regex", "someValidRegex(.*)");
        props.put("topic.replacement", "");
        subject.configure(props);
    }

    @Test
    @FixFor("DBZ-1086")
    public void testKeyNullValue() {
        final ByLogicalTableRouter<SourceRecord> router = new ByLogicalTableRouter<>();
        final Map<String, String> props = new HashMap<>();

        props.put("topic.regex", "(.*)customers_shard(.*)");
        props.put("topic.replacement", "$1customers_all_shards");
        props.put("key.field.name", "shard_id");
        props.put("key.field.regex", "(.*)customers_shard_(.*)");
        props.put("key.field.replacement", "$2");
        router.configure(props);

        SourceRecord record1 = new SourceRecord(
                new HashMap<>(), new HashMap<>(), "mysql-server-1.inventory.customers_shard_1", null, null, null, null);

        SourceRecord transformed1 = router.apply(record1);
        assertThat(transformed1).isNotNull();
        assertThat(transformed1.topic()).isEqualTo("mysql-server-1.inventory.customers_all_shards");

        assertThat(transformed1.keySchema()).isNull();
        assertThat(transformed1.key()).isNull();
    }

    @Test
    @FixFor("DBZ-2034")
    public void testNamespaceReplacementWithoutKeyChange() {
        final ByLogicalTableRouter<SourceRecord> router = new ByLogicalTableRouter<>();
        final Map<String, String> props = new HashMap<>();

        props.put("topic.regex", "(.*).dbz_shard_\\d+.(.*)");
        props.put("topic.replacement", "dbz_core.$2");
        props.put("key.enforce.uniqueness", "false");
        router.configure(props);

        Schema keySchema = SchemaBuilder.struct()
                .name("s21.dbz_shard_21.address.Key")
                .field("id", SchemaBuilder.int64().build())
                .build();

        Struct key1 = new Struct(keySchema).put("id", 123L);

        SourceRecord record1 = new SourceRecord(
                new HashMap<>(), new HashMap<>(), "s21.dbz_shard_21.address", keySchema, key1, null, null);

        SourceRecord transformed1 = router.apply(record1);
        assertThat(transformed1).isNotNull();
        assertThat(transformed1.topic()).isEqualTo("dbz_core.address"); // no change expected

        assertThat(transformed1.keySchema().name()).isEqualTo("dbz_core.address.Key");
        assertThat(transformed1.keySchema().fields()).hasSize(1);
        assertThat(transformed1.keySchema().fields().get(0).name()).isEqualTo("id");

        assertThat(((Struct) transformed1.key()).get("id")).isEqualTo(123L);

        Struct key2 = new Struct(keySchema).put("id", 123L);

        // This second record is basically the same as the first, and should _actually_ be indistinguishable
        keySchema = SchemaBuilder.struct()
                .name("s22.dbz_shard_22.address.Key")
                .field("id", SchemaBuilder.int64().build())
                .build();
        SourceRecord record2 = new SourceRecord(
                new HashMap<>(), new HashMap<>(), "s22.dbz_shard_22.address", keySchema, key2, null, null);

        SourceRecord transformed2 = router.apply(record2);
        assertThat(transformed2).isNotNull();
        assertThat(transformed2.topic()).isEqualTo("dbz_core.address");

        assertThat(transformed2.keySchema().name()).isEqualTo("dbz_core.address.Key");
        assertThat(transformed2.keySchema().fields()).hasSize(1);
        assertThat(transformed2.keySchema().fields().get(0).name()).isEqualTo("id");

        assertThat(((Struct) transformed2.key()).get("id")).isEqualTo(123L);
    }

    @Test
    @FixFor("DBZ-2412")
    public void shouldHandleSchemaChangeEvent() throws Exception {
        final ByLogicalTableRouter<SourceRecord> router = new ByLogicalTableRouter<>();

        final Map<String, String> props = new HashMap<>();

        props.put("topic.regex", "(.*)");
        props.put("topic.replacement", "$1_rerouted");

        router.configure(props);

        Schema schemaChangeKeySchema = SchemaBuilder.struct()
                .name("io.debezium.connector.mysql.SchemaChangeKey")
                .field(Fields.DATABASE_NAME, Schema.STRING_SCHEMA)
                .build();

        // just a basic non-CDC envelope
        Schema schemaChangeValueSchema = SchemaBuilder.struct()
                .name("io.debezium.connector.mysql.SchemaChangeValue")
                .field(Fields.DATABASE_NAME, Schema.OPTIONAL_STRING_SCHEMA);

        final SourceRecord record = new SourceRecord(null, Collections.singletonMap("offset", "1"), "schema_changes", 0,
                schemaChangeKeySchema, new Struct(schemaChangeKeySchema).put(Fields.DATABASE_NAME, "my-db"), schemaChangeValueSchema,
                new Struct(schemaChangeValueSchema).put(Fields.DATABASE_NAME, "my-db"));

        SourceRecord transformed = router.apply(record);
        assertThat(transformed.topic()).isEqualTo("schema_changes_rerouted");
        assertThat(transformed.value()).isSameAs(record.value());
    }

    // FIXME: This SMT can use more tests for more detailed coverage.
    // The creation of a DBZ-ish SourceRecord is required for each test
}
