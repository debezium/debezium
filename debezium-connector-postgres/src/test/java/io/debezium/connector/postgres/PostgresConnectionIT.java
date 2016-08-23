/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgres;

import java.sql.SQLException;
import java.sql.Types;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.Ignore;
import org.junit.Test;
import org.postgresql.util.PGobject;
import com.google.protobuf.InvalidProtocolBufferException;

import io.debezium.connector.postgresql.proto.PgProto;
import io.debezium.util.Testing;

@Ignore
public class PostgresConnectionIT implements Testing {
    
    @Test
    public void shouldInitWALReaderAndReadLog() throws SQLException {
        try (PostgresConnection conn = PostgresConnection.forTestDatabase("postgres")) {
            conn.connect();
            
            conn.execute("DROP TABLE IF EXISTS table_with_pk ");
            conn.execute("DROP TABLE IF EXISTS table_without_pk ");
            conn.execute("CREATE TABLE table_with_pk (a SERIAL, b VARCHAR(30), c TIMESTAMP NOT NULL, PRIMARY KEY(a, c));",
                         "CREATE TABLE table_without_pk (a SERIAL, b NUMERIC(5,2), c TEXT);");

            conn.execute("INSERT INTO table_with_pk (b, c) VALUES('Backup and Restore', now());");
            conn.execute("INSERT INTO table_with_pk (b, c) VALUES('Tuning', now());");
            conn.execute("DELETE FROM table_with_pk WHERE a < 3;");

            initLogicalReplication(conn);
            consumeChanges(conn);
            
            // Postgres WILL NOT fire any changes (UPDATES or DELETES) for tables which don't have a PK by default EXCEPT
            // if that table has a REPLICA IDENTITY of FULL. 
            // See http://michael.otacoo.com/postgresql-2/postgres-9-4-feature-highlight-replica-identity-logical-replication/
            conn.execute("ALTER TABLE table_without_pk REPLICA IDENTITY FULL");
            
            conn.execute("INSERT INTO table_with_pk (b, c) VALUES('Backup and Restore', now());");
            conn.execute("INSERT INTO table_with_pk (b, c) VALUES('Tuning', now());");
            conn.execute("DELETE FROM table_with_pk WHERE a < 3;");
            
            conn.execute("INSERT INTO table_without_pk (b,c) VALUES (1, 'Tapir');");
            conn.execute("UPDATE table_without_pk SET c = 'Anita' WHERE c = 'Tapir';");
            
            consumeChanges(conn);
//            peekChanges(conn);
//            consumeChanges(conn);
        }
    }
    
    private void stopLogicalReplication(PostgresConnection conn) throws Exception {
        conn.call("SELECT * FROM pg_drop_replication_slot(?);", statement -> statement.setString(1, "test_slot"), null);
    } 
    
    @Test
    public void startMonitoring() throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        try (PostgresConnection conn = PostgresConnection.forTestDatabase("postgres")) {
            conn.connect();
            initLogicalReplication(conn);
            executorService.submit(() -> {
                try {
                    while (true) {
                        consumeChanges(conn);
                        Thread.sleep(1000);
                    }
                } catch (InterruptedException e) {
                    return null;
                }
            }).get();
        } finally {
            executorService.shutdown();
        }
    }
    
    private void peekChanges(PostgresConnection conn) throws SQLException {
        callStatementAndPrintMessage(conn, "SELECT data FROM pg_logical_slot_peek_binary_changes(?, ?, ?, ?, ?);");
    }
     
    private void consumeChanges(PostgresConnection conn) throws SQLException {
        callStatementAndPrintMessage(conn, "SELECT data FROM pg_logical_slot_get_binary_changes(?, ?, ?, ?, ?);");
    }
    
    private void callStatementAndPrintMessage(PostgresConnection conn, String stmt) throws SQLException {
        conn.call(stmt,
                  statement -> {
                      statement.setString(1, "test_slot");
                      PGobject pgLSN = new PGobject();
                      pgLSN.setType("pg_lsn");
                      pgLSN.setValue(null);
                      statement.setObject(2, null);
                      statement.setNull(3, Types.INTEGER);
                      statement.setString(4, "debug-mode");
                      statement.setString(5, "0");
                  }, rs -> {
                    while (rs.next()) {
                        byte[] protobufContent = rs.getBytes(1);
                        PgProto.RowMessage message = null;
                        try {
                            message = PgProto.RowMessage.parseFrom(protobufContent);
                            System.out.println(message.toString());
                        } catch (InvalidProtocolBufferException e) {
                            throw new RuntimeException(e);
                        }
                    
                    }
                });     
    }
    
    private void initLogicalReplication(PostgresConnection conn) throws SQLException {
        try {
            conn.call("SELECT * FROM pg_create_logical_replication_slot(?, ?)", statement -> {
                statement.setString(1, "test_slot");
                statement.setString(2, "decoderbufs");
            }, null);
        } catch (SQLException e) {
            if (!e.getMessage().toLowerCase().contains("already exists")) {
                throw e;
            }
        }
    }
}
