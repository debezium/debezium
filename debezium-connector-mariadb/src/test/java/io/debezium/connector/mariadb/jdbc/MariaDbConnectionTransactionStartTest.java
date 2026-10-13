/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;

/**
 * Unit tests for {@link MariaDbConnection.TransactionStart}, using {@code SHOW BINLOG EVENTS} rows captured from MariaDB 11.4.
 */
public class MariaDbConnectionTransactionStartTest {

    // Two DDL transactions, a single-row transaction and a two-row transaction.
    private static final Object[][] EVENTS = {
            { "Format_desc", 4L },
            { "Gtid_list", 256L },
            { "Binlog_checkpoint", 285L },
            { "Gtid", 330L },
            { "Query", 372L },
            { "Gtid", 453L },
            { "Query", 495L },
            { "Gtid", 618L },
            { "Annotate_rows", 660L },
            { "Table_map", 709L },
            { "Write_rows_v1", 750L },
            { "Xid", 788L },
            { "Gtid", 819L },
            { "Annotate_rows", 861L },
            { "Table_map", 910L },
            { "Write_rows_v1", 951L },
            { "Annotate_rows", 989L },
            { "Table_map", 1038L },
            { "Write_rows_v1", 1079L },
            { "Xid", 1117L },
    };

    @Test
    @FixFor("debezium/dbz#2731")
    public void shouldKeepPositionAtTransactionBoundary() {
        assertTransactionStart(330, 330);
        assertTransactionStart(618, 618);
        assertTransactionStart(819, 819);
    }

    @Test
    @FixFor("debezium/dbz#2731")
    public void shouldMoveBackToStartOfTransactionThePositionFallsInside() {
        // Right after the GTID event, between two row events, and inside an event.
        assertTransactionStart(660, 618);
        assertTransactionStart(989, 819);
        assertTransactionStart(1000, 819);
    }

    @Test
    @FixFor("debezium/dbz#2731")
    public void shouldMoveBackToStartOfFileInsideHeader() {
        assertTransactionStart(100, 4);
    }

    @Test
    @FixFor("debezium/dbz#2731")
    public void shouldKeepPositionAtOrAfterEndOfFile() {
        assertTransactionStart(1148, 1148);
        assertTransactionStart(99_999, 99_999);
    }

    private static void assertTransactionStart(long position, long expected) {
        final MariaDbConnection.TransactionStart finder = new MariaDbConnection.TransactionStart(position);
        for (Object[] event : EVENTS) {
            if (!finder.accept((String) event[0], (Long) event[1])) {
                break;
            }
        }
        assertThat(finder.transactionStart()).as("transaction start for position %d", position).isEqualTo(expected);
    }
}
