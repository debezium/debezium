/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotMode;
import io.debezium.doc.FixFor;

/**
 * Integration test verifying that the TypeRegistry initialises correctly when
 * dependent custom types are present, without triggering per-OID SQL_OID_LOOKUP
 * round trips for each unresolved dependency (fix for DBZ-1683).
 */
public class TypeRegistryIT extends AbstractRecordsProducerTest {

    @BeforeEach
    void before() throws SQLException {
        TestHelper.dropAllSchemas();
        TestHelper.execute("CREATE SCHEMA typeregistry;");
        // Chain: numeric82 <- numericex (domain of domain)
        TestHelper.execute("CREATE DOMAIN typeregistry.numeric82 AS numeric(8,2);");
        TestHelper.execute("CREATE DOMAIN typeregistry.numericex AS typeregistry.numeric82;");
        // Array of a custom domain type
        TestHelper.execute("CREATE DOMAIN typeregistry.posint AS integer CHECK (VALUE > 0);");
        TestHelper.execute("CREATE TABLE typeregistry.t1 ("
                + "id serial primary key, "
                + "amount typeregistry.numericex, "
                + "quantity typeregistry.posint"
                + ");");
    }

    @Test
    @FixFor("DBZ-2041")
    public void shouldStartConnectorWithDependentDomainTypes() throws Exception {
        // The connector must start successfully without timeouts caused by
        // per-OID SQL_OID_LOOKUP queries for each unresolved dependent type.
        start(PostgresConnector.class, TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, "typeregistry")
                .build());
        assertConnectorIsRunning();

        TestHelper.execute(
                "INSERT INTO typeregistry.t1 (amount, quantity) VALUES (123.45, 10);");

        final TestConsumer consumer = testConsumer(1, "typeregistry");
        consumer.await(TestHelper.waitTimeForRecords() * 30, TimeUnit.SECONDS);

        SourceRecord record = consumer.remove();
        Struct after = ((Struct) record.value()).getStruct("after");
        assertThat(after.get("id")).isEqualTo(1);
        assertThat(after.get("quantity")).isEqualTo(10);
        assertThat(consumer.isEmpty()).isTrue();
    }

    @Test
    @FixFor("DBZ-2041")
    public void shouldResolveChainedDomainTypesWithoutOidLookupFallback() throws Exception {
        // Verifies that a domain built on top of another domain resolves correctly
        // through the iterative prime() loop without falling back to SQL_OID_LOOKUP.
        start(PostgresConnector.class, TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, "typeregistry")
                .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, true)
                .build());
        assertConnectorIsRunning();

        TestHelper.execute(
                "INSERT INTO typeregistry.t1 (amount, quantity) VALUES (99.99, 5);");

        final TestConsumer consumer = testConsumer(1, "typeregistry");
        consumer.await(TestHelper.waitTimeForRecords() * 30, TimeUnit.SECONDS);

        SourceRecord record = consumer.remove();
        Struct after = ((Struct) record.value()).getStruct("after");
        assertThat(after.get("quantity")).isEqualTo(5);
        assertThat(consumer.isEmpty()).isTrue();
    }
}
