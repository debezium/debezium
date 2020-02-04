/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.junit.Test;

import com.datastax.driver.core.TableMetadata;

public class SchemaProcessorTest extends EmbeddedCassandraConnectorTestBase {

    @Test
    public void testProcess() throws Exception {
        CassandraConnectorContext context = generateTaskContext();
        SchemaProcessor schemaProcessor = new SchemaProcessor(context);
        SchemaHolder.KeyValueSchema keyValueSchema;
        String namespacePrefix = "io.debezium.connector.cassandra" + "."
                + EmbeddedCassandraConnectorTestBase.TEST_KAFKA_TOPIC_PREFIX + "."
                + EmbeddedCassandraConnectorTestBase.TEST_KEYSPACE;
        String expectedKeySchemaName;
        String expectedValueSchemaName;

        assertEquals(0, context.getSchemaHolder().getCdcEnabledTableMetadataSet().size());

        context.getCassandraClient().execute("CREATE TABLE IF NOT EXISTS " + keyspaceTable("table1") + " (a int, b text, PRIMARY KEY(a)) WITH cdc = false;");
        schemaProcessor.process();
        assertEquals(0, context.getSchemaHolder().getCdcEnabledTableMetadataSet().size());
        keyValueSchema = context.getSchemaHolder().getOrUpdateKeyValueSchema(new KeyspaceTable(TEST_KEYSPACE, "table1"));
        assertNull(keyValueSchema);

        context.getCassandraClient().execute("ALTER TABLE " + keyspaceTable("table1") + " WITH cdc = true;");
        schemaProcessor.process();
        assertEquals(1, context.getSchemaHolder().getCdcEnabledTableMetadataSet().size());
        keyValueSchema = context.getSchemaHolder().getOrUpdateKeyValueSchema(new KeyspaceTable(TEST_KEYSPACE, "table1"));
        assertNotNull(keyValueSchema);
        expectedKeySchemaName = namespacePrefix + "." + "table1" + "." + "Key";
        assertEquals(expectedKeySchemaName, keyValueSchema.keySchema().name());
        expectedValueSchemaName = namespacePrefix + "." + "table1" + "." + "Value";
        assertEquals(expectedValueSchemaName, keyValueSchema.valueSchema().name());

        context.getCassandraClient().execute("CREATE TABLE IF NOT EXISTS " + keyspaceTable("table2") + " (a int, b text, PRIMARY KEY(a)) WITH cdc = true;");
        schemaProcessor.process();
        assertEquals(2, context.getSchemaHolder().getCdcEnabledTableMetadataSet().size());

        keyValueSchema = context.getSchemaHolder().getOrUpdateKeyValueSchema(new KeyspaceTable(TEST_KEYSPACE, "table1"));
        assertNotNull(keyValueSchema);
        expectedKeySchemaName = namespacePrefix + "." + "table1" + "." + "Key";
        assertEquals(expectedKeySchemaName, keyValueSchema.keySchema().name());
        expectedValueSchemaName = namespacePrefix + "." + "table1" + "." + "Value";
        assertEquals(expectedValueSchemaName, keyValueSchema.valueSchema().name());

        keyValueSchema = context.getSchemaHolder().getOrUpdateKeyValueSchema(new KeyspaceTable(TEST_KEYSPACE, "table2"));
        assertNotNull(keyValueSchema);
        expectedKeySchemaName = namespacePrefix + "." + "table2" + "." + "Key";
        assertEquals(expectedKeySchemaName, keyValueSchema.keySchema().name());
        expectedValueSchemaName = namespacePrefix + "." + "table2" + "." + "Value";
        assertEquals(expectedValueSchemaName, keyValueSchema.valueSchema().name());

        context.getCassandraClient().execute("ALTER TABLE " + keyspaceTable("table2") + " ADD c text");
        schemaProcessor.process();
        assertEquals(2, context.getSchemaHolder().getCdcEnabledTableMetadataSet().size());
        TableMetadata expectedTm1 = context.getCassandraClient().getCdcEnabledTableMetadata(TEST_KEYSPACE, "table1");
        TableMetadata expectedTm2 = context.getCassandraClient().getCdcEnabledTableMetadata(TEST_KEYSPACE, "table2");
        TableMetadata tm1 = context.getSchemaHolder().getOrUpdateKeyValueSchema(new KeyspaceTable(TEST_KEYSPACE, "table1")).tableMetadata();
        TableMetadata tm2 = context.getSchemaHolder().getOrUpdateKeyValueSchema(new KeyspaceTable(TEST_KEYSPACE, "table2")).tableMetadata();
        assertEquals(expectedTm1, tm1);
        assertEquals(expectedTm2, tm2);

        deleteTestKeyspaceTables();
        context.cleanUp();
    }
}
