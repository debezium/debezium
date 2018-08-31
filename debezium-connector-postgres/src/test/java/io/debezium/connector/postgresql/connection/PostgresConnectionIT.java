/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql.connection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.After;
import org.junit.Test;

import io.debezium.connector.postgresql.TestHelper;
import io.debezium.relational.TableId;
import io.debezium.util.Testing;

/**
 * Integration test for {@link PostgresConnection}
 * 
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public class PostgresConnectionIT {

    @After
    public void after() {
        Testing.Print.disable();
    }

    @Test
    public void shouldReportValidTxId() throws SQLException {
        try (PostgresConnection connection = TestHelper.create()) {
            connection.connect();
            assertTrue(connection.currentTransactionId() > 0);
        }
        
        try (PostgresConnection connection = TestHelper.create()) {
            connection.connect();
            connection.setAutoCommit(false);
            Long txId = connection.currentTransactionId();
            connection.executeWithoutCommitting("SELECT 1;");
            assertEquals("tx id should be the same", txId, connection.currentTransactionId());
            connection.connection().commit();
        }
    }

    @Test
    public void shouldReportValidXLogPos() throws SQLException {
        try (PostgresConnection connection = TestHelper.create()) {
            connection.connect();
            assertTrue(connection.currentXLogLocation() > 0);
        }
    }

    @Test
    public void shouldReadServerInformation() throws Exception {
        try (PostgresConnection connection = TestHelper.create()) {
            ServerInfo serverInfo = connection.serverInfo();
            assertNotNull(serverInfo);
            assertNotNull(serverInfo.server());
            assertNotNull(serverInfo.username());
            assertNotNull(serverInfo.database());
            Map<String, String> permissionsByRoleName = serverInfo.permissionsByRoleName();
            assertNotNull(permissionsByRoleName);
            assertTrue(!permissionsByRoleName.isEmpty());
        }
    }

    @Test
    public void shouldReadReplicationSlotInfo() throws Exception {
        try (PostgresConnection connection = TestHelper.create()) {
            ServerInfo.ReplicationSlot slotInfo = connection.readReplicationSlotInfo("test", "test");
            assertEquals(ServerInfo.ReplicationSlot.INVALID, slotInfo);
        }
    }

    @Test
    public void shouldPrintReplicateIdentityInfo() throws Exception {
        String statement = "DROP SCHEMA IF EXISTS public CASCADE;" +
                           "CREATE SCHEMA public;" +
                           "CREATE TABLE test(pk serial, PRIMARY KEY (pk));";
        TestHelper.execute(statement);
        try (PostgresConnection connection = TestHelper.create()) {
            assertEquals(ServerInfo.ReplicaIdentity.DEFAULT, connection.readReplicaIdentityInfo(TableId.parse("public.test")));
        }
    }

    @Test
    public void shouldDropReplicationSlot() throws Exception {
        try (PostgresConnection connection = TestHelper.create()) {
            // try to drop a non existent slot
            assertFalse(connection.dropReplicationSlot("test"));
        }
        // create a new replication slot via a replication connection
        try (ReplicationConnection connection = TestHelper.createForReplication("test", false)) {
            assertTrue(connection.isConnected());
        }
        // drop the slot from the previous connection
        try (PostgresConnection connection = TestHelper.create()) {
            // try to drop the previous slot
            assertTrue(connection.dropReplicationSlot("test"));
        }
    }

    @Test
    public void shouldDetectRunningConncurrentTxOnInit() throws Exception {
        Testing.Print.enable();
        // drop the slot from the previous connection
        final String slotName = "block";
        try (PostgresConnection connection = TestHelper.create()) {
            // try to drop the previous slot
            connection.dropReplicationSlot(slotName);
            connection.execute(
                    "DROP SCHEMA IF EXISTS public CASCADE",
                    "CREATE SCHEMA public",
                    "CREATE TABLE test(pk serial, PRIMARY KEY (pk))"
            );
        }

        try (PostgresConnection blockingConnection = TestHelper.create("blocker")) {
            // Create an unfinished TX
            blockingConnection.connection().setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            blockingConnection.connection().setAutoCommit(false);
            blockingConnection.executeWithoutCommitting(
                    "INSERT INTO test VALUES(default)"
            );
            Testing.print("Blocking exception started");

            final Future<?> f1 = Executors.newSingleThreadExecutor().submit(() -> {
                // Create a replication connection that is blocked till the concurrent TX is completed
                try (ReplicationConnection replConnection = TestHelper.createForReplication(slotName, false)) {
                    Testing.print("Connecting with replication connection 1");
                    assertTrue(replConnection.isConnected());
                    Testing.print("Replication connection 1 - completed");
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            Thread.sleep(3000);

            final Future<?> f2 = Executors.newSingleThreadExecutor().submit(() -> {
                // Create a replication connection that receives confirmed_flush_lsn == null
                try (ReplicationConnection replConnection = TestHelper.createForReplication(slotName, false)) {
                    Testing.print("Connecting with replication connection 2");
                    assertTrue(replConnection.isConnected());
                    Testing.print("Replication connection 2 - completed");
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            Thread.sleep(3000);
            blockingConnection.connection().commit();
            Testing.print("Blocking exception finished");

            Thread.sleep(6000);

            f1.get();
            f2.get();
            // drop the slot from the previous connection
            try (PostgresConnection connection = TestHelper.create()) {
                // try to drop the previous slot
                assertTrue(connection.dropReplicationSlot(slotName));
            }
        }
    }
}
