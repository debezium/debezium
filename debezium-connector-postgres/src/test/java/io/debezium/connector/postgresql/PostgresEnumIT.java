/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import static io.debezium.connector.postgresql.TestHelper.topicName;
import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.data.Envelope;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.junit.logging.LogInterceptor;

public class PostgresEnumIT extends AbstractAsyncEngineConnectorTest {

    @BeforeAll
    static void beforeClass() throws SQLException {
        TestHelper.dropAllSchemas();
    }

    @BeforeEach
    void before() {
        initializeConnectorTestFramework();
    }

    @AfterEach
    void after() {
        stopConnector();
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.dropPublication();
        TestHelper.resetWalSenderTimeout();
    }

    @Test
    public void shouldReproduceTypeRegistryDuplicateEnumNameBug() throws Exception {
        final LogInterceptor typeRegistryLogInterceptor = new LogInterceptor(TypeRegistry.class);

        TestHelper.execute(
                "DROP TABLE IF EXISTS public.enum_test CASCADE;",
                "DROP TABLE IF EXISTS public.int_test CASCADE;",
                "DROP TABLE IF EXISTS test.enum_test CASCADE;",
                "DROP TABLE IF EXISTS test.int_test CASCADE;",
                "DROP TYPE IF EXISTS public.test_type CASCADE;",
                "DROP TYPE IF EXISTS test.test_type CASCADE;",
                "DROP TYPE IF EXISTS \"bug.status\" CASCADE;",
                "DROP DOMAIN IF EXISTS public.test_type CASCADE;",
                "DROP DOMAIN IF EXISTS test.test_type CASCADE;",
                "CREATE SCHEMA IF NOT EXISTS test;",
                "CREATE TYPE public.test_type AS ENUM ('X', 'Y');",
                "CREATE DOMAIN test.test_type AS INTEGER;",
                "CREATE TYPE \"bug.status\" AS ENUM ('new', 'open', 'closed');",
                "CREATE TABLE public.enum_test (id int4 NOT NULL, value public.test_type DEFAULT 'X'::public.test_type, status \"bug.status\" DEFAULT 'new'::\"bug.status\", CONSTRAINT enum_test_pkey PRIMARY KEY (id));",
                "CREATE TABLE test.int_test (id int4 NOT NULL, value test.test_type DEFAULT 42, CONSTRAINT int_test_pkey PRIMARY KEY (id));",
                "CREATE TYPE test.test_type2 AS ENUM ('A', 'B');",
                "CREATE DOMAIN public.test_type2 AS INTEGER;",
                "CREATE TABLE test.enum_test (id int4 NOT NULL, value test.test_type2 DEFAULT 'A'::test.test_type2, CONSTRAINT enum_test_pkey PRIMARY KEY (id));",
                "CREATE TABLE public.int_test (id int4 NOT NULL, value public.test_type2 DEFAULT 100, CONSTRAINT int_test_pkey PRIMARY KEY (id));");

        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.NO_DATA)
                .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, "public,test")
                .with(PostgresConnectorConfig.PLUGIN_NAME, PostgresConnectorConfig.LogicalDecoder.PGOUTPUT)
                .build();
        start(PostgresConnector.class, config);
        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);

        assertThat(typeRegistryLogInterceptor.containsWarnMessage("is already mapped")).isTrue();

        TestHelper.execute(
                "INSERT INTO public.enum_test(id, value, status) VALUES (1, 'Y'::public.test_type, 'open'::\"bug.status\");",
                "INSERT INTO test.int_test(id, value) VALUES (1, 123);");

        SourceRecords records = consumeRecordsByTopic(2);
        List<SourceRecord> publicEnumRecords = records.recordsForTopic(topicName("public.enum_test"));
        List<SourceRecord> testIntRecords = records.recordsForTopic(topicName("test.int_test"));
        assertThat(publicEnumRecords).isNotNull().hasSize(1);
        assertThat(testIntRecords).isNotNull().hasSize(1);

        Struct after1 = ((Struct) publicEnumRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
        Struct after2 = ((Struct) testIntRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after1.get("value")).isEqualTo("Y");
        assertThat(after1.get("status")).isEqualTo("open");
        assertThat(after2.get("value")).isInstanceOf(Integer.class).isEqualTo(123);

        TestHelper.execute(
                "INSERT INTO test.enum_test(id, value) VALUES (1, 'B'::test.test_type2);",
                "INSERT INTO public.int_test(id, value) VALUES (1, 456);");

        records = consumeRecordsByTopic(2);
        List<SourceRecord> testEnumRecords = records.recordsForTopic(topicName("test.enum_test"));
        List<SourceRecord> publicIntRecords = records.recordsForTopic(topicName("public.int_test"));
        assertThat(testEnumRecords).isNotNull().hasSize(1);
        assertThat(publicIntRecords).isNotNull().hasSize(1);

        Struct after3 = ((Struct) testEnumRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
        Struct after4 = ((Struct) publicIntRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after3.get("value")).isInstanceOf(String.class).isEqualTo("B");
        assertThat(after4.get("value")).isInstanceOf(Integer.class).isEqualTo(456);
    }

}
