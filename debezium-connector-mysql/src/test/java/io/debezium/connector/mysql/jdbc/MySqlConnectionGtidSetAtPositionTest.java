/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.debezium.connector.mysql.gtid.MySqlGtidSet;
import io.debezium.doc.FixFor;

/**
 * Unit tests for {@link MySqlConnection.GtidSetAtPosition}, using {@code SHOW BINLOG EVENTS} rows captured from MySQL 8.4.
 */
public class MySqlConnectionGtidSetAtPositionTest {

    private static final String S = "11111111-1111-1111-1111-111111111111";
    private static final String D = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";

    // First file: two local transactions and one with a GTID from another source, then a rotation.
    private static final Object[][] FIRST_FILE = {
            { "Format_desc", 4L, 127L, "Server ver: 8.4.11, Binlog ver: 4" },
            { "Previous_gtids", 127L, 158L, "" },
            { "Gtid", 158L, 235L, "SET @@SESSION.GTID_NEXT= '" + S + ":1'" },
            { "Query", 235L, 334L, "CREATE DATABASE d /* xid=9 */" },
            { "Gtid", 334L, 411L, "SET @@SESSION.GTID_NEXT= '" + S + ":2'" },
            { "Query", 411L, 544L, "CREATE TABLE d.t (id INT PRIMARY KEY, v VARCHAR(10)) /* xid=10 */" },
            { "Gtid", 544L, 623L, "SET @@SESSION.GTID_NEXT= '" + S + ":3'" },
            { "Query", 623L, 694L, "BEGIN" },
            { "Table_map", 694L, 746L, "table_id: 101 (d.t)" },
            { "Write_rows", 746L, 788L, "table_id: 101 flags: STMT_END_F" },
            { "Xid", 788L, 819L, "COMMIT /* xid=11 */" },
            { "Gtid", 819L, 898L, "SET @@SESSION.GTID_NEXT= '" + D + ":1'" },
            { "Query", 898L, 969L, "BEGIN" },
            { "Table_map", 969L, 1021L, "table_id: 101 (d.t)" },
            { "Write_rows", 1021L, 1069L, "table_id: 101 flags: STMT_END_F" },
            { "Xid", 1069L, 1100L, "COMMIT /* xid=13 */" },
            { "Gtid", 1100L, 1179L, "SET @@SESSION.GTID_NEXT= '" + D + ":2'" },
            { "Query", 1179L, 1250L, "BEGIN" },
            { "Table_map", 1250L, 1302L, "table_id: 101 (d.t)" },
            { "Write_rows", 1302L, 1350L, "table_id: 101 flags: STMT_END_F" },
            { "Xid", 1350L, 1381L, "COMMIT /* xid=16 */" },
            { "Rotate", 1381L, 1428L, "mysql-bin.000002;pos=4" },
    };

    // Second file: Previous_gtids carries both sources, then a single-row, a two-row, a DDL and a single-row transaction.
    private static final Object[][] SECOND_FILE = {
            { "Format_desc", 4L, 127L, "Server ver: 8.4.11, Binlog ver: 4" },
            { "Previous_gtids", 127L, 238L, S + ":1-3,\n" + D + ":1-2" },
            { "Gtid", 238L, 317L, "SET @@SESSION.GTID_NEXT= '" + S + ":4'" },
            { "Query", 317L, 388L, "BEGIN" },
            { "Table_map", 388L, 440L, "table_id: 101 (d.t)" },
            { "Write_rows", 440L, 482L, "table_id: 101 flags: STMT_END_F" },
            { "Xid", 482L, 513L, "COMMIT /* xid=19 */" },
            { "Gtid", 513L, 592L, "SET @@SESSION.GTID_NEXT= '" + S + ":5'" },
            { "Query", 592L, 663L, "BEGIN" },
            { "Table_map", 663L, 715L, "table_id: 101 (d.t)" },
            { "Write_rows", 715L, 757L, "table_id: 101 flags: STMT_END_F" },
            { "Table_map", 757L, 809L, "table_id: 101 (d.t)" },
            { "Write_rows", 809L, 851L, "table_id: 101 flags: STMT_END_F" },
            { "Xid", 851L, 882L, "COMMIT /* xid=21 */" },
            { "Gtid", 882L, 959L, "SET @@SESSION.GTID_NEXT= '" + S + ":6'" },
            { "Query", 959L, 1072L, "ALTER TABLE d.t ADD COLUMN w INT /* xid=24 */" },
            { "Gtid", 1072L, 1151L, "SET @@SESSION.GTID_NEXT= '" + S + ":7'" },
            { "Query", 1151L, 1222L, "BEGIN" },
            { "Table_map", 1222L, 1275L, "table_id: 103 (d.t)" },
            { "Write_rows", 1275L, 1321L, "table_id: 103 flags: STMT_END_F" },
            { "Xid", 1321L, 1352L, "COMMIT /* xid=25 */" },
    };

    private static final Object[][] ANONYMOUS_FILE = {
            { "Format_desc", 4L, 127L, "Server ver: 8.4.11, Binlog ver: 4" },
            { "Previous_gtids", 127L, 158L, "" },
            { "Anonymous_Gtid", 158L, 235L, "SET @@SESSION.GTID_NEXT= 'ANONYMOUS'" },
            { "Query", 235L, 306L, "BEGIN" },
            { "Xid", 306L, 337L, "COMMIT /* xid=11 */" },
    };

    @Test
    @FixFor("debezium/dbz#2731")
    public void shouldUsePreviousGtidsAtStartOfFile() {
        assertGtidSet(SECOND_FILE, 4, S + ":1-3," + D + ":1-2");
        assertGtidSet(SECOND_FILE, 238, S + ":1-3," + D + ":1-2");
    }

    @Test
    @FixFor("debezium/dbz#2731")
    public void shouldIncludeTransactionsEndingAtOrBeforePosition() {
        assertGtidSet(SECOND_FILE, 513, S + ":1-4," + D + ":1-2");
        assertGtidSet(SECOND_FILE, 1072, S + ":1-6," + D + ":1-2");
        assertGtidSet(FIRST_FILE, 819, S + ":1-3");
        assertGtidSet(FIRST_FILE, 1381, S + ":1-3," + D + ":1-2");
    }

    @Test
    @FixFor("debezium/dbz#2731")
    public void shouldExcludeTransactionThePositionFallsInside() {
        // At the BEGIN of a transaction, between two of its row events, and inside an event.
        assertGtidSet(SECOND_FILE, 317, S + ":1-3," + D + ":1-2");
        assertGtidSet(SECOND_FILE, 757, S + ":1-4," + D + ":1-2");
        assertGtidSet(SECOND_FILE, 800, S + ":1-4," + D + ":1-2");
    }

    @Test
    @FixFor("debezium/dbz#2731")
    public void shouldIncludeLastTransactionAtEndOfFile() {
        assertGtidSet(SECOND_FILE, 1352, S + ":1-7," + D + ":1-2");
        assertGtidSet(SECOND_FILE, 1_000_000, S + ":1-7," + D + ":1-2");
    }

    @Test
    @FixFor("debezium/dbz#2731")
    public void shouldReturnEmptySetWithoutGtids() {
        assertGtidSet(ANONYMOUS_FILE, 337, "");
    }

    private static void assertGtidSet(Object[][] events, long position, String expected) {
        final MySqlConnection.GtidSetAtPosition resolver = new MySqlConnection.GtidSetAtPosition(position);
        for (Object[] event : events) {
            if (!resolver.accept((String) event[0], (Long) event[1], (Long) event[2], (String) event[3])) {
                break;
            }
        }
        assertThat(new MySqlGtidSet(resolver.gtidSet())).as("GTID set at position %d", position).isEqualTo(new MySqlGtidSet(expected));
    }
}
