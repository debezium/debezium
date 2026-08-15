/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import static io.debezium.junit.EqualityCheck.LESS_THAN;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotMode;
import io.debezium.doc.FixFor;
import io.debezium.junit.SkipWhenDatabaseVersion;

/**
 * Integration test to verify behaviour of tables which include domain types
 */
@SkipWhenDatabaseVersion(check = LESS_THAN, major = 11, minor = 0, reason = "Domain type array columns not supported")
public class DomainTypesIT extends AbstractRecordsProducerTest {

    @BeforeEach
    void before() throws SQLException {
        TestHelper.dropAllSchemas();
        TestHelper.execute("CREATE SCHEMA domaintypes");
        TestHelper.execute("CREATE DOMAIN nmtoken AS text CHECK (VALUE ~* '^[A-Z0-9\\.\\_\\-\\:]+$');");
        TestHelper.execute("CREATE TABLE domaintypes.t1 (id serial primary key, token nmtoken, tokens nmtoken[]);");
    }

    @Test
    @FixFor("DBZ-3657")
    public void shouldNotChokeOnDomainTypeInArray() throws Exception {
        start(PostgresConnector.class, TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, "domaintypes")
                .build());
        assertConnectorIsRunning();

        TestHelper.execute("INSERT INTO domaintypes.t1 (id, token, tokens) values (default, 'foo', '{\"bar\",\"baz\"}')");

        final TestConsumer consumer = testConsumer(1, "domaintypes");
        consumer.await(TestHelper.waitTimeForRecords() * 30, TimeUnit.SECONDS);
        SourceRecord record = consumer.remove();
        Struct value = (Struct) record.value();
        Struct after = (Struct) value.get("after");
        assertThat(after.get("token")).isEqualTo("foo");
        assertThat(getFieldNames(after)).containsOnly("id", "token");
    }

    @Test
    @FixFor("DBZ-3657")
    public void shouldExportDomainTypeInArrayAsUnknown() throws Exception {
        start(PostgresConnector.class, TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, "domaintypes")
                .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, true)
                .build());
        assertConnectorIsRunning();

        TestHelper.execute("INSERT INTO domaintypes.t1 (id, token, tokens) values (default, 'foo', '{\"bar\",\"baz\"}')");

        final TestConsumer consumer = testConsumer(1, "domaintypes");
        consumer.await(TestHelper.waitTimeForRecords() * 30, TimeUnit.SECONDS);
        SourceRecord record = consumer.remove();
        Struct value = (Struct) record.value();
        Struct after = (Struct) value.get("after");
        assertThat(after.get("tokens")).isEqualTo(Arrays.asList(ByteBuffer.wrap("bar".getBytes()), ByteBuffer.wrap("baz".getBytes())));
    }

    @Test
    @FixFor("debezium/dbz#2350")
    public void shouldSnapshotDomainColumnsIdenticallyToTheirBaseType() throws Exception {
        // getColumnValue() resolves a column's type to decide how to read it (NUMERIC/MONEY/temporal are read
        // specially). A domain must be read exactly like its base type. This snapshots (initial mode -> the
        // getColumnValue path) a table whose columns come in plain/domain pairs holding identical values, and
        // asserts each domain column emits the same value as its base column -- i.e. it took the same branch.
        TestHelper.execute(
                "CREATE DOMAIN domaintypes.num_dom AS numeric;",
                "CREATE DOMAIN domaintypes.money_dom AS money;",
                "CREATE DOMAIN domaintypes.ts_dom AS timestamp;",
                "CREATE TABLE domaintypes.t2 ("
                        + "id serial primary key, "
                        + "plain_num numeric, dom_num domaintypes.num_dom, "
                        + "plain_money money, dom_money domaintypes.money_dom, "
                        + "plain_ts timestamp, dom_ts domaintypes.ts_dom);",
                "INSERT INTO domaintypes.t2 (id, plain_num, dom_num, plain_money, dom_money, plain_ts, dom_ts) "
                        + "VALUES (default, 123.45, 123.45, 67.89, 67.89, "
                        + "'2021-03-08 12:34:56', '2021-03-08 12:34:56');");

        start(PostgresConnector.class, TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, "domaintypes")
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "domaintypes.t2")
                .build());
        assertConnectorIsRunning();

        final TestConsumer consumer = testConsumer(1, "domaintypes");
        consumer.await(TestHelper.waitTimeForRecords() * 30, TimeUnit.SECONDS);
        SourceRecord record = consumer.remove();
        Struct after = (Struct) ((Struct) record.value()).get("after");

        assertThat(after.get("dom_num")).isEqualTo(after.get("plain_num"));
        assertThat(after.get("dom_money")).isEqualTo(after.get("plain_money"));
        assertThat(after.get("dom_ts")).isEqualTo(after.get("plain_ts"));
    }

    private List<String> getFieldNames(Struct struct) {
        return struct.schema()
                .fields()
                .stream()
                .map(Field::name)
                .collect(Collectors.toList());
    }
}
