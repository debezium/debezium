/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import com.datastax.driver.core.TableMetadata;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class SchemaProcessorTest extends EmbeddedCassandraConnectorTestBase {
    private CassandraConnectorContext context;
    private AtomicBoolean globalTaskState;
    private SchemaProcessor schemaProcessor;

    @Before
    public void setUp() throws Exception {
        context = generateTaskContext();
        globalTaskState = new AtomicBoolean(true);
        schemaProcessor = new SchemaProcessor(context, globalTaskState);
    }

    @After
    public void tearDown() {
        globalTaskState.set(false);
        context.cleanUp();
    }

    @Test
    public void testRefreshSchemas() {
        assertEquals(0, context.getSchemaHolder().getCdcEnabledTableMetadataSet().size());

        context.getCassandraClient().execute("CREATE TABLE IF NOT EXISTS " + keyspaceTable("table1") + " (a int, b text, PRIMARY KEY(a)) WITH cdc = false;");
        schemaProcessor.refreshSchemas();
        assertEquals(0, context.getSchemaHolder().getCdcEnabledTableMetadataSet().size());
        assertNull(context.getSchemaHolder().getOrUpdateKeyValueSchema(new KeyspaceTable(TEST_KEYSPACE, "table1")));

        context.getCassandraClient().execute("ALTER TABLE " + keyspaceTable("table1") + " WITH cdc = true;");
        schemaProcessor.refreshSchemas();
        assertEquals(1, context.getSchemaHolder().getCdcEnabledTableMetadataSet().size());
        assertNotNull(context.getSchemaHolder().getOrUpdateKeyValueSchema(new KeyspaceTable(TEST_KEYSPACE, "table1")));

        context.getCassandraClient().execute("CREATE TABLE IF NOT EXISTS " + keyspaceTable("table2") + " (a int, b text, PRIMARY KEY(a)) WITH cdc = true;");
        schemaProcessor.refreshSchemas();
        assertEquals(2, context.getSchemaHolder().getCdcEnabledTableMetadataSet().size());
        assertNotNull(context.getSchemaHolder().getOrUpdateKeyValueSchema(new KeyspaceTable(TEST_KEYSPACE, "table1")));
        assertNotNull(context.getSchemaHolder().getOrUpdateKeyValueSchema(new KeyspaceTable(TEST_KEYSPACE, "table2")));

        context.getCassandraClient().execute("ALTER TABLE " + keyspaceTable("table2") + " ADD c text");
        schemaProcessor.refreshSchemas();
        assertEquals(2, context.getSchemaHolder().getCdcEnabledTableMetadataSet().size());
        TableMetadata expectedTm1 = context.getCassandraClient().getCdcEnabledTableMetadata(TEST_KEYSPACE, "table1");
        TableMetadata expectedTm2 = context.getCassandraClient().getCdcEnabledTableMetadata(TEST_KEYSPACE, "table2");
        TableMetadata tm1 = context.getSchemaHolder().getOrUpdateKeyValueSchema(new KeyspaceTable(TEST_KEYSPACE, "table1")).tableMetadata();
        TableMetadata tm2 = context.getSchemaHolder().getOrUpdateKeyValueSchema(new KeyspaceTable(TEST_KEYSPACE, "table2")).tableMetadata();
        assertEquals(expectedTm1, tm1);
        assertEquals(expectedTm2, tm2);

        deleteTestKeyspaceTables();
    }
}
