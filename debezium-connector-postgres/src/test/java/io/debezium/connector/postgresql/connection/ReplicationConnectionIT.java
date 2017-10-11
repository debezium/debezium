/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql.connection;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.junit.Before;
import org.junit.Test;

import io.debezium.connector.postgresql.DecoderDifferences;
import io.debezium.connector.postgresql.TestHelper;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;

/**
 * Integration test for {@link ReplicationConnection}
 * 
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public class ReplicationConnectionIT {
    
    @Before
    public void before() throws Exception {
        TestHelper.dropAllSchemas();
        String statement = "CREATE SCHEMA public;" + 
                           "CREATE TABLE table_with_pk (a SERIAL, b VARCHAR(30), c TIMESTAMP NOT NULL, PRIMARY KEY(a, c));" +
                           "CREATE TABLE table_without_pk (a SERIAL, b NUMERIC(5,2), c TEXT);";
        TestHelper.execute(statement);
    } 
  
    @Test
    public void shouldCreateAndDropReplicationSlots() throws Exception {
        // create a replication connection which should be dropped once it's closed
        try (ReplicationConnection connection = TestHelper.createForReplication("test1", true)) {
            ReplicationStream stream = connection.startStreaming();  
            assertNull(stream.lastReceivedLSN());                              
            stream.close();
        }    
        // create a replication connection which should be dropped once it's closed
        try (ReplicationConnection connection = TestHelper.createForReplication("test2", true)) {
            ReplicationStream stream = connection.startStreaming();
            assertNull(stream.lastReceivedLSN());
            stream.close();
        }
    }
    
    @Test(expected = IllegalStateException.class)
    public void shouldNotAllowMultipleReplicationSlotsOnTheSameDBSlotAndPlugin() throws Exception {
        // create a replication connection which should be dropped once it's closed
        try (ReplicationConnection conn1 = TestHelper.createForReplication("test1", true)) {
            conn1.startStreaming();
            try (ReplicationConnection conn2 = TestHelper.createForReplication("test1", false)) {
                conn2.startStreaming();
                fail("Should not be able to create 2 replication connections on the same db, plugin and slot");
            }
        }
    }
   
    @Test
    public void shouldReceiveAndDecodeIndividualChanges() throws Exception {
        // create a replication connection which should be dropped once it's closed
        try (ReplicationConnection connection = TestHelper.createForReplication("test", true)) {
            ReplicationStream stream = connection.startStreaming(); // this creates the replication slot
            int expectedMessages = DecoderDifferences.updatesWithoutPK(insertLargeTestData(), 1);
            expectedMessagesFromStream(stream, expectedMessages);
        }
    }
    
    @Test
    public void shouldReceiveSameChangesIfNotFlushed() throws Exception {
        // don't drop the replication slot once this is finished
        String slotName = "test";
        int receivedMessagesCount = startInsertStop(slotName, null);
    
        // create a new replication connection with the same slot and check that without the LSN having been flushed,
        // we'll get back the same message again from before
        try (ReplicationConnection connection = TestHelper.createForReplication(slotName, true)) {
            ReplicationStream stream = connection.startStreaming(); // this should receive the same message as before since we haven't flushed
            expectedMessagesFromStream(stream, receivedMessagesCount);
        }
    }
    
    
    @Test
    public void shouldNotReceiveSameChangesIfFlushed() throws Exception {
        // don't drop the replication slot once this is finished
        String slotName = "test";
        startInsertStop(slotName, this::flushLSN);

        // create a new replication connection with the same slot and check that we don't get back the same changes that we've 
        // flushed
        try (ReplicationConnection connection = TestHelper.createForReplication(slotName, true)) {
            ReplicationStream stream = connection.startStreaming();
            //even when flushing the last received location, the server will send back the last record after reconnecting, not sure why that is...
            expectedMessagesFromStream(stream, 0);
        }
    }
    
    @Test
    public void shouldReceiveMissedChangesWhileDown() throws Exception {
        String slotName = "test";
        startInsertStop(slotName, this::flushLSN);

        // run some more SQL while the slot is stopped 
        // this deletes 2 entries so each of them will have a message
        TestHelper.execute("DELETE FROM table_with_pk WHERE a < 3;");
        int additionalMessages = 2;
        
        // create a new replication connection with the same slot and check that we get the additional messages
        try (ReplicationConnection connection = TestHelper.createForReplication(slotName, true)) {
            ReplicationStream stream = connection.startStreaming();
            expectedMessagesFromStream(stream, additionalMessages);
        }
    }
    
    @Test
    public void shouldResumeFromLastReceivedLSN() throws Exception {
        String slotName = "test";
        AtomicLong lastReceivedLSN = new AtomicLong(0);
        startInsertStop(slotName, stream -> lastReceivedLSN.compareAndSet(0, stream.lastReceivedLSN()));
        assertTrue(lastReceivedLSN.get() > 0);
    
        // resume replication from the last received LSN and don't expect anything else
        try (ReplicationConnection connection = TestHelper.createForReplication(slotName, true)) {
            ReplicationStream stream = connection.startStreaming(lastReceivedLSN.get());
            expectedMessagesFromStream(stream, 0);
        }
    }    
    
    @Test
    public void shouldTolerateInvalidLSNValues() throws Exception {
        String slotName = "test";
        startInsertStop(slotName, null);
    
        // resume replication from the last received LSN and don't expect anything else
        try (ReplicationConnection connection = TestHelper.createForReplication(slotName, true)) {
            ReplicationStream stream = connection.startStreaming(Long.MAX_VALUE);
            expectedMessagesFromStream(stream, 0);
            // this deletes 2 entries so each of them will have a message
            TestHelper.execute("DELETE FROM table_with_pk WHERE a < 3;");
            // don't expect any messages because we've started stream from a very big (i.e. the end) position
            expectedMessagesFromStream(stream, 0);
        }
    }
    
    @Test
    public void shouldReceiveOneMessagePerDMLOnTransactionCommit() throws Exception {
        try (ReplicationConnection connection = TestHelper.createForReplication("test", true)) {
            ReplicationStream stream = connection.startStreaming();
            String statement = "DROP TABLE IF EXISTS table_with_pk;" +
                               "DROP TABLE IF EXISTS table_without_pk;" +
                               "CREATE TABLE table_with_pk (a SERIAL, b VARCHAR(30), c TIMESTAMP NOT NULL, PRIMARY KEY(a, c));" +
                               "CREATE TABLE table_without_pk (a SERIAL, b NUMERIC(5,2), c TEXT);" +
                               "INSERT INTO table_with_pk (b, c) VALUES('val1', now()); " +
                               "INSERT INTO table_with_pk (b, c) VALUES('val2', now()); ";
            TestHelper.execute(statement);
            expectedMessagesFromStream(stream, 2);
        }
    }
    
    @Test
    public void shouldNotReceiveMessagesOnTransactionRollback() throws Exception {
        try (ReplicationConnection connection = TestHelper.createForReplication("test", true)) {
            ReplicationStream stream = connection.startStreaming();
            String statement = "DROP TABLE IF EXISTS table_with_pk;" +
                               "CREATE TABLE table_with_pk (a SERIAL, b VARCHAR(30), c TIMESTAMP NOT NULL, PRIMARY KEY(a, c));" +
                               "INSERT INTO table_with_pk (b, c) VALUES('val1', now()); " +
                               "ROLLBACK;";
            TestHelper.execute(statement);
            expectedMessagesFromStream(stream, 0);
        }
    }
    
    @Test
    public void shouldGeneratesEventsForMultipleSchemas() throws Exception {
        try (ReplicationConnection connection = TestHelper.createForReplication("test", true)) {
            ReplicationStream stream = connection.startStreaming();
            String statements = "CREATE SCHEMA schema1;" +
                                "CREATE SCHEMA schema2;" +
                                "DROP TABLE IF EXISTS schema1.table;" +
                                "DROP TABLE IF EXISTS schema2.table;" +
                                "CREATE TABLE schema1.table (a SERIAL, b VARCHAR(30), c TIMESTAMP NOT NULL, PRIMARY KEY(a, c));" +
                                "CREATE TABLE schema2.table (a SERIAL, b VARCHAR(30), c TIMESTAMP NOT NULL, PRIMARY KEY(a, c));" +
                                "INSERT INTO schema1.table (b, c) VALUES('Value for schema1', now());" +
                                "INSERT INTO schema2.table (b, c) VALUES('Value for schema2', now());";
            TestHelper.execute(statements);
            expectedMessagesFromStream(stream, 2);
        }    
    }
    
    private void flushLSN(ReplicationStream stream) {
        try {
            stream.flushLSN();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private int startInsertStop(String slotName, Consumer<ReplicationStream> streamProcessor) throws Exception {
        // start a replication connection, insert some data and stop without dropping the slot....
        int expectedMessageCount;
        try (ReplicationConnection connection = TestHelper.createForReplication(slotName, false)) {
            try {
                ReplicationStream stream = connection.startStreaming(); // this creates the replication slot
                expectedMessageCount = insertSmallTestData();
                expectedMessagesFromStream(stream, expectedMessageCount);
                if (streamProcessor != null) {
                    streamProcessor.accept(stream);
                }
            } catch (Throwable t) {
                // make sure we always drop the slot if something fails - note the connection was created with the drop on close 
                // set to false
                try (PostgresConnection conn = TestHelper.create()) {
                    conn.dropReplicationSlot(slotName);
                }
                throw t;
            }
        }
        // wait a bit to make sure the replication connection has been stopped
        Thread.sleep(100);
        return expectedMessageCount;
    }
    
    private List<ReplicationMessage> expectedMessagesFromStream(ReplicationStream stream,
                                                                int expectedMessages) throws Exception {
        List<ReplicationMessage> actualMessages = new ArrayList<>();

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Semaphore latch = new Semaphore(0);
        Metronome metronome = Metronome.sleeper(50, TimeUnit.MILLISECONDS, Clock.SYSTEM);
        Future<?> result = executorService.submit(() -> {
            while (!Thread.interrupted()) {
                for(;;) {
                    List<ReplicationMessage> message = new ArrayList<>();
                    stream.readPending(x -> message.add(x));
                    if (message.isEmpty()) {
                        break;
                    }
                    actualMessages.addAll(message);
                    latch.release(message.size());
                }
                metronome.pause();
            }
            return null;
        });

        try {
            if (!latch.tryAcquire(expectedMessages, 10, TimeUnit.SECONDS)) {
                result.cancel(true);
                fail("expected " + expectedMessages + " messages, but read only " + actualMessages.size());
            }
        } finally {
            executorService.shutdownNow();
        }
        return actualMessages;
    }
    
    private int insertSmallTestData() throws Exception {
        String statement = "INSERT INTO table_with_pk (b, c) VALUES('Backup and Restore', now());" +
                           "INSERT INTO table_with_pk (b, c) VALUES('Tuning', now());";
        TestHelper.execute(statement);
        // we expect 2 messages from the above
        return 2;
    }
    
    private int insertLargeTestData() throws Exception {
        String statement = "INSERT INTO table_with_pk (b, c) VALUES('Backup and Restore', now());" +
                           "INSERT INTO table_with_pk (b, c) VALUES('Tuning', now());" +
                           "DELETE FROM table_with_pk WHERE a < 3;" + // deletes 2 records
                           "INSERT INTO table_without_pk (b,c) VALUES (1, 'Foo');" +
                           "UPDATE table_without_pk SET c = 'Bar' WHERE c = 'Foo';" +
                           "ALTER TABLE table_without_pk REPLICA IDENTITY FULL;" +
                           "UPDATE table_without_pk SET c = 'Baz' WHERE c = 'Bar';" +
                           "DELETE FROM table_without_pk WHERE c = 'Baz';";
        
        // Postgres WILL NOT fire any tuple changes (UPDATES or DELETES) for tables which don't have a PK by default EXCEPT
        // if that table has a REPLICA IDENTITY of FULL or INDEX. 
        // See http://michael.otacoo.com/postgresql-2/postgres-9-4-feature-highlight-replica-identity-logical-replication/
        // ...so we expect 8 messages for the above DML
        TestHelper.execute(statement);
        return 8;
    }
}
