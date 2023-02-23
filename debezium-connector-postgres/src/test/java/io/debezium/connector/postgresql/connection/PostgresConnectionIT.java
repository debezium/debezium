/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql.connection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.postgresql.jdbc.PgConnection;

import io.debezium.connector.postgresql.TestHelper;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
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
            assertEquals(ReplicaIdentityInfo.ReplicaIdentity.DEFAULT.toString(), connection.readReplicaIdentityInfo(TableId.parse("public.test")).toString());
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
            connection.initConnection();
            assertTrue(connection.isConnected());
        }
        // drop the slot from the previous connection
        try (PostgresConnection connection = TestHelper.create()) {
            // try to drop the previous slot
            assertTrue(connection.dropReplicationSlot("test"));
        }
    }

    @Test
    @FixFor("DBZ-934")
    @Ignore
    // Temporary slots no longer supported due to DBZ-2338
    public void temporaryReplicationSlotsShouldGetDroppedAutomatically() throws Exception {
        try (ReplicationConnection replicationConnection = TestHelper.createForReplication("test", true)) {
            replicationConnection.initConnection();
            PgConnection pgConnection = getUnderlyingConnection(replicationConnection);

            // temporary replication slots are not supported by Postgres < 10
            if (pgConnection.getServerMajorVersion() < 10) {
                return;
            }

            // simulate ungraceful shutdown by closing underlying database connection
            pgConnection.close();

            try (PostgresConnection connection = TestHelper.create()) {
                assertFalse("postgres did not drop replication slot", connection.dropReplicationSlot("test"));
            }
        }
    }

    private PgConnection getUnderlyingConnection(ReplicationConnection connection) throws Exception {
        Field connField = JdbcConnection.class.getDeclaredField("conn");
        connField.setAccessible(true);

        return (PgConnection) connField.get(connection);
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
                    "CREATE TABLE test(pk serial, PRIMARY KEY (pk))");
        }

        try (PostgresConnection blockingConnection = TestHelper.create("blocker")) {
            // Create an unfinished TX
            blockingConnection.connection().setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            blockingConnection.connection().setAutoCommit(false);
            blockingConnection.executeWithoutCommitting(
                    "INSERT INTO test VALUES(default)");
            Testing.print("Blocking exception started");

            final Future<?> f1 = Executors.newSingleThreadExecutor().submit(() -> {
                // Create a replication connection that is blocked till the concurrent TX is completed
                try (ReplicationConnection replConnection = TestHelper.createForReplication(slotName, false)) {
                    Testing.print("Connecting with replication connection 1");
                    replConnection.initConnection();
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
                    replConnection.initConnection();
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

    @Test
    public void shouldSupportPG95RestartLsn() throws Exception {
        String slotName = "pg95";
        try (ReplicationConnection replConnection = TestHelper.createForReplication(slotName, false)) {
            replConnection.initConnection();
            assertTrue(replConnection.isConnected());
        }
        try (PostgresConnection conn = buildPG95PGConn("pg95")) {
            ServerInfo.ReplicationSlot slotInfo = conn.readReplicationSlotInfo(slotName, TestHelper.decoderPlugin().getPostgresPluginName());
            assertNotNull(slotInfo);
            assertNotEquals(ServerInfo.ReplicationSlot.INVALID, slotInfo);
            conn.dropReplicationSlot(slotName);
        }

    }

    @Test
    public void shouldSupportFallbackToRestartLsn() throws Exception {
        String slotName = "emptyconfirmed";
        try (ReplicationConnection replConnection = TestHelper.createForReplication(slotName, false)) {
            replConnection.initConnection();
            assertTrue(replConnection.isConnected());
        }
        try (PostgresConnection withIdleTransaction = new PostgresConnection(JdbcConfiguration.adapt(TestHelper.defaultJdbcConfig()),
                PostgresConnection.CONNECTION_GENERAL);
                PostgresConnection withEmptyConfirmedFlushLSN = buildConnectionWithEmptyConfirmedFlushLSN(slotName)) {
            withIdleTransaction.setAutoCommit(false);
            withIdleTransaction.query("select 1", connection -> {
            });
            ServerInfo.ReplicationSlot slotInfo = withEmptyConfirmedFlushLSN.readReplicationSlotInfo(slotName, TestHelper.decoderPlugin().getPostgresPluginName());
            assertNotNull(slotInfo);
            assertNotEquals(ServerInfo.ReplicationSlot.INVALID, slotInfo);
            withEmptyConfirmedFlushLSN.dropReplicationSlot(slotName);
        }
    }

    // "fake" a pg95 response by not returning confirmed_flushed_lsn
    private PostgresConnection buildPG95PGConn(String name) {
        return new PostgresConnection(JdbcConfiguration.adapt(TestHelper.defaultJdbcConfig()), name) {
            @Override
            protected ServerInfo.ReplicationSlot queryForSlot(String slotName, String database, String pluginName,
                                                              ResultSetMapper<ServerInfo.ReplicationSlot> map)
                    throws SQLException {

                String fields = "slot_name, plugin, slot_type, datoid, database, active, active_pid, xmin, catalog_xmin, restart_lsn";
                return prepareQueryAndMap("select " + fields + " from pg_replication_slots where slot_name = ? and database = ? and plugin = ?", statement -> {
                    statement.setString(1, slotName);
                    statement.setString(2, database);
                    statement.setString(3, pluginName);
                }, map);
            }
        };
    }

    private PostgresConnection buildConnectionWithEmptyConfirmedFlushLSN(String name) {
        return new PostgresConnection(JdbcConfiguration.adapt(TestHelper.defaultJdbcConfig()), name) {
            @Override
            protected ServerInfo.ReplicationSlot queryForSlot(String slotName, String database, String pluginName,
                                                              ResultSetMapper<ServerInfo.ReplicationSlot> map)
                    throws SQLException {

                String fields = "slot_name, plugin, slot_type, datoid, database, active, active_pid, xmin, catalog_xmin, restart_lsn";
                return prepareQueryAndMap(
                        "select " + fields + ", NULL as confirmed_flush_lsn from pg_replication_slots where slot_name = ? and database = ? and plugin = ?", statement -> {
                            statement.setString(1, slotName);
                            statement.setString(2, database);
                            statement.setString(3, pluginName);
                        }, map);
            }
        };
    }
}
