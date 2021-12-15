/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import static io.debezium.junit.EqualityCheck.LESS_THAN;
import static org.fest.assertions.Assertions.assertThat;

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Before;
import org.junit.Test;

import io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotMode;
import io.debezium.doc.FixFor;
import io.debezium.junit.SkipWhenDatabaseVersion;

/**
 * Integration test to verify behaviour of tables which include domain types
 */
@SkipWhenDatabaseVersion(check = LESS_THAN, major = 11, minor = 0, reason = "Domain type array columns not supported")
public class DomainTypesIT extends AbstractRecordsProducerTest {

    @Before
    public void before() throws SQLException {
        TestHelper.dropAllSchemas();
        TestHelper.execute("CREATE SCHEMA domaintypes");
        TestHelper.execute("CREATE DOMAIN nmtoken AS text CHECK (VALUE ~* '^[A-Z0-9\\.\\_\\-\\:]+$');");
        TestHelper.execute("CREATE TABLE domaintypes.t1 (id serial primary key, token nmtoken, tokens nmtoken[]);");
    }

    @Test
    @FixFor("DBZ-3657")
    public void shouldNotChokeOnDomainTypes() throws Exception {
        start(PostgresConnector.class, TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL_ONLY)
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
    }
}
