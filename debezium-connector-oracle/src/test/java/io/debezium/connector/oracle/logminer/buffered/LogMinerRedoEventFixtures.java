/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.LogMinerRedoEvent;
import io.debezium.relational.TableId;

/**
 * Samples of the shapes a stored {@link LogMinerRedoEvent} takes, and a field-by-field comparison.
 *
 * @author Chris Cranford
 */
public final class LogMinerRedoEventFixtures {

    public static final Scn SCN = Scn.valueOf("8800160");
    public static final TableId TABLE_ID = TableId.parse("ORCLPDB1.DEBEZIUM.CUSTOMERS");
    public static final String ROW_ID = "AAAVn/AAEAAAAFlAAA";
    public static final String RS_ID = "0x000013.00001a2b.0010";
    public static final Instant CHANGE_TIME = Instant.parse("2026-10-24T12:34:56.789Z");

    /**
     * Longer than the 65,535 encoded bytes {@code DataOutputStream.writeUTF} can write, which a joined CSF
     * redo statement can exceed.
     */
    public static final int LONG_REDO_SQL_LENGTH = 70_000;

    private LogMinerRedoEventFixtures() {
    }

    public static LogMinerRedoEvent rollbackMarker() {
        return new LogMinerRedoEvent(EventType.DELETE, true, SCN, TABLE_ID, ROW_ID, RS_ID, CHANGE_TIME, null, 0, null, 0L, 70_123L, 70_124L);
    }

    public static LogMinerRedoEvent internalEvent() {
        return new LogMinerRedoEvent(EventType.INTERNAL, false, SCN, TABLE_ID, ROW_ID, RS_ID, CHANGE_TIME, null, 0, null, 1L, 70_123L, 70_124L);
    }

    public static LogMinerRedoEvent insert() {
        return new LogMinerRedoEvent(EventType.INSERT, false, SCN, TABLE_ID, ROW_ID, RS_ID, CHANGE_TIME,
                "insert into \"DEBEZIUM\".\"CUSTOMERS\"(\"ID\",\"NAME\") values ('1','Kafka');", 0, null, 0L, 70_123L, 70_124L);
    }

    public static LogMinerRedoEvent updateWithErrorStatus() {
        return new LogMinerRedoEvent(EventType.UPDATE, false, SCN, TABLE_ID, ROW_ID, RS_ID, CHANGE_TIME,
                "update \"UNKNOWN\".\"OBJ# 70123\" set \"COL 2\" = 'Kafka' where \"COL 1\" = '1';", 2, "Object information not available", 0L, 70_123L, 70_124L);
    }

    public static LogMinerRedoEvent xmlBegin() {
        return new LogMinerRedoEvent(EventType.XML_BEGIN, false, SCN, TABLE_ID, ROW_ID, RS_ID, CHANGE_TIME,
                "XML DOC BEGIN:  select \"XML_DATA\" from \"DEBEZIUM\".\"CUSTOMERS\" where \"ID\" = '1'", 0, "XML DOC (BINARY)", 2L, 70_123L, 70_124L);
    }

    public static LogMinerRedoEvent lobWriteWithLongRedo() {
        return new LogMinerRedoEvent(EventType.LOB_WRITE, false, SCN, TABLE_ID, ROW_ID, RS_ID, CHANGE_TIME,
                "declare loc_c CLOB; buf_c VARCHAR2(" + LONG_REDO_SQL_LENGTH + ") := '" + "x".repeat(LONG_REDO_SQL_LENGTH) + "'; end;",
                0, null, 0L, 70_123L, 70_124L);
    }

    public static LogMinerRedoEvent xmlEnd() {
        return new LogMinerRedoEvent(EventType.XML_END, false, SCN, TABLE_ID, ROW_ID, RS_ID, CHANGE_TIME, null, 0, null, 1L, 70_123L, 70_124L);
    }

    /**
     * One instance per shape, the rollback marker first.
     */
    public static List<LogMinerRedoEvent> allEvents() {
        return List.of(rollbackMarker(), internalEvent(), insert(), updateWithErrorStatus(), xmlBegin(), lobWriteWithLongRedo(), xmlEnd());
    }

    public static void assertSameEvent(LogMinerRedoEvent expected, LogMinerRedoEvent actual) {
        assertThat(actual).isNotNull();
        assertThat(actual.getEventType()).isEqualTo(expected.getEventType());
        assertThat(actual.isRollback()).isEqualTo(expected.isRollback());
        assertThat(actual.getScn()).isEqualTo(expected.getScn());
        assertThat(actual.getTableId()).isEqualTo(expected.getTableId());
        assertThat(actual.getRowIdAsString()).isEqualTo(expected.getRowIdAsString());
        assertThat(actual.getRsId()).isEqualTo(expected.getRsId());
        assertThat(actual.getChangeTime()).isEqualTo(expected.getChangeTime());
        assertThat(actual.getRedoSql()).isEqualTo(expected.getRedoSql());
        assertThat(actual.getStatus()).isEqualTo(expected.getStatus());
        assertThat(actual.getInfo()).isEqualTo(expected.getInfo());
        assertThat(actual.getTransactionSequence()).isEqualTo(expected.getTransactionSequence());
        assertThat(actual.getObjectId()).isEqualTo(expected.getObjectId());
        assertThat(actual.getDataObjectId()).isEqualTo(expected.getDataObjectId());
        assertThat(actual).isEqualTo(expected);
    }
}
