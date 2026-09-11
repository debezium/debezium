/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;
import io.debezium.connector.oracle.logminer.events.RollbackToSavepointEvent;
import io.debezium.doc.FixFor;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.relational.TableId;

public abstract class AbstractFindRolledBackRangeTest<T extends Transaction> {
    protected static final TableId TABLE = TableId.parse("db.schema.table");
    protected static final TableId OTHER_TABLE = TableId.parse("db.schema.other");
    protected static final Instant CHANGE_TIME = Instant.now();

    private CacheProvider<T> cacheProvider;
    private TransactionFactory<T> transactionFactory;

    @BeforeEach
    void beforeEach() {
        cacheProvider = getCacheProvider();
        transactionFactory = getTransactionFactory();
    }

    @AfterEach
    void afterEach() throws Exception {
        if (cacheProvider != null) {
            cacheProvider.close();
            cacheProvider = null;
        }
        transactionFactory = null;
    }

    // INSERT

    @Test
    @FixFor("debezium/dbz#1960")
    public void testRollbackInsertScalar() throws Exception {
        // CREATE TABLE DBZ1960_01(ID NUMERIC(9,0) PRIMARY KEY, STR0 VARCHAR(50));
        // SAVEPOINT s1;
        // INSERT INTO DBZ1960_01 (ID, STR0) VALUES (1, 'STR0-1-0');
        // ROLLBACK TO SAVEPOINT s1;
        // Version: 26ai Free Release 23.26.2.0.0
        LogInterceptor logInterceptor = new LogInterceptor(AbstractLogMinerTransactionCache.class);
        LogMinerEvent[] events = new LogMinerEvent[]{
                event(EventType.INSERT, 0, "BBBBBBBBBBBBBBBBBB", "1"),
                event(EventType.DELETE, 1, "BBBBBBBBBBBBBBBBBB", "2"), };
        LogMinerEvent[] expected = new LogMinerEvent[]{
                event(EventType.DELETE, 1, "BBBBBBBBBBBBBBBBBB", "2"), };
        assertThat(cache(events)).isEqualTo(expected);
        assertThat(logInterceptor.containsWarnMessage("Manual investigation is required")).isFalse();
        assertThat(logInterceptor.containsWarnMessage("Please enable 'log.mining.include.internal.events'")).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void testRollbackInsertEmpty() throws Exception {
        // CREATE TABLE DBZ1960_02(ID NUMERIC(9,0) PRIMARY KEY, LOB0 CLOB);
        // SAVEPOINT s1;
        // INSERT INTO DBZ1960_02 (ID, LOB0) VALUES (1, EMPTY_CLOB());
        // ROLLBACK TO SAVEPOINT s1;
        // Version: 26ai Free Release 23.26.2.0.0
        LogInterceptor logInterceptor = new LogInterceptor(AbstractLogMinerTransactionCache.class);
        LogMinerEvent[] events = new LogMinerEvent[]{
                event(EventType.INSERT, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.INTERNAL, 0, "BBBBBBBBBBBBBBBBBB", "2"),
                event(EventType.DELETE, 1, "BBBBBBBBBBBBBBBBBB", "3"), };
        LogMinerEvent[] expected = new LogMinerEvent[]{
                event(EventType.DELETE, 1, "BBBBBBBBBBBBBBBBBB", "3"), };
        assertThat(cache(events)).isEqualTo(expected);
        assertThat(logInterceptor.containsWarnMessage("Manual investigation is required")).isFalse();
        assertThat(logInterceptor.containsWarnMessage("Please enable 'log.mining.include.internal.events'")).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void testRollbackInsertInline() throws Exception {
        // CREATE TABLE DBZ1960_03(ID NUMERIC(9,0) PRIMARY KEY, LOB0 CLOB);
        // SAVEPOINT s1;
        // INSERT INTO DBZ1960_03 (ID, LOB0) VALUES (1, 'LOB0-1-0');
        // ROLLBACK TO SAVEPOINT s1;
        // Version: 26ai Free Release 23.26.2.0.0
        LogInterceptor logInterceptor = new LogInterceptor(AbstractLogMinerTransactionCache.class);
        LogMinerEvent[] events = new LogMinerEvent[]{
                event(EventType.INSERT, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.UPDATE, 0, "BBBBBBBBBBBBBBBBBB", "2"),
                event(EventType.DELETE, 1, "BBBBBBBBBBBBBBBBBB", "3"), };
        LogMinerEvent[] expected = new LogMinerEvent[]{
                event(EventType.DELETE, 1, "BBBBBBBBBBBBBBBBBB", "3"), };
        assertThat(cache(events)).isEqualTo(expected);
        assertThat(logInterceptor.containsWarnMessage("Manual investigation is required")).isFalse();
        assertThat(logInterceptor.containsWarnMessage("Please enable 'log.mining.include.internal.events'")).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void testRollbackInsertOutOfLine() throws Exception {
        // CREATE TABLE DBZ1960_04(ID NUMERIC(9,0) PRIMARY KEY, XML0 XMLTYPE, LOB0 CLOB, EXT0 VARCHAR2(8000));
        // SAVEPOINT s1;
        // INSERT INTO DBZ1960_04 (ID, XML0, LOB0, EXT0) VALUES (1, XMLTYPE('<XML0><ID>1</ID><V>0</V></XML0>'), RPAD('LOB0-1-', 1985, '0'), RPAD('EXT0-1-', 4000, '0'));
        // ROLLBACK TO SAVEPOINT s1;
        // Version: 26ai Free Release 23.26.2.0.0
        LogInterceptor logInterceptor = new LogInterceptor(AbstractLogMinerTransactionCache.class);
        LogMinerEvent[] events = new LogMinerEvent[]{
                event(EventType.INSERT, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.SELECT_LOB_LOCATOR, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.LOB_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "2"),
                event(EventType.EXTENDED_STRING_BEGIN, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.EXTENDED_STRING_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "3"),
                event(EventType.XML_BEGIN, 0, "AAAAAAAAAAAAAAAAAA", "4"),
                event(EventType.XML_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "4"),
                event(EventType.XML_END, 0, "AAAAAAAAAAAAAAAAAA", "4"),
                event(EventType.INTERNAL, 0, "BBBBBBBBBBBBBBBBBB", "5"),
                event(EventType.DELETE, 1, "BBBBBBBBBBBBBBBBBB", "6"), };
        LogMinerEvent[] expected = new LogMinerEvent[]{
                event(EventType.DELETE, 1, "BBBBBBBBBBBBBBBBBB", "6"), };
        assertThat(cache(events)).isEqualTo(expected);
        assertThat(logInterceptor.containsWarnMessage("Manual investigation is required")).isFalse();
        assertThat(logInterceptor.containsWarnMessage("Please enable 'log.mining.include.internal.events'")).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void testRollbackInsertInlineAndOutOfLine() throws Exception {
        // CREATE TABLE DBZ1960_05(ID NUMERIC(9,0) PRIMARY KEY, EXT0 VARCHAR2(8000), XML0 XMLTYPE, LOB0 CLOB, LOB1 CLOB);
        // SAVEPOINT s1;
        // INSERT INTO DBZ1960_05 (ID, EXT0, XML0, LOB0, LOB1) VALUES (1, RPAD('EXT0-1-', 4000, '0'), XMLTYPE('<XML0><ID>1</ID><V>0</V></XML0>'), RPAD('LOB0-1-', 1985, '0'), 'LOB1-1-0');
        // ROLLBACK TO SAVEPOINT s1;
        // Version: 26ai Free Release 23.26.2.0.0
        LogInterceptor logInterceptor = new LogInterceptor(AbstractLogMinerTransactionCache.class);
        LogMinerEvent[] events = new LogMinerEvent[]{
                event(EventType.INSERT, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.EXTENDED_STRING_BEGIN, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.EXTENDED_STRING_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "2"),
                event(EventType.SELECT_LOB_LOCATOR, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.LOB_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "3"),
                event(EventType.XML_BEGIN, 0, "AAAAAAAAAAAAAAAAAA", "4"),
                event(EventType.XML_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "4"),
                event(EventType.XML_END, 0, "AAAAAAAAAAAAAAAAAA", "4"),
                event(EventType.UPDATE, 0, "BBBBBBBBBBBBBBBBBB", "5"),
                event(EventType.DELETE, 1, "BBBBBBBBBBBBBBBBBB", "6"), };
        LogMinerEvent[] expected = new LogMinerEvent[]{
                event(EventType.DELETE, 1, "BBBBBBBBBBBBBBBBBB", "6"), };
        assertThat(cache(events)).isEqualTo(expected);
        assertThat(logInterceptor.containsWarnMessage("Manual investigation is required")).isFalse();
        assertThat(logInterceptor.containsWarnMessage("Please enable 'log.mining.include.internal.events'")).isFalse();
    }

    // UPDATE

    @Test
    @FixFor("debezium/dbz#1960")
    public void testRollbackUpdateScalar() throws Exception {
        // CREATE TABLE DBZ1960_06(ID NUMERIC(9,0) PRIMARY KEY, STR0 VARCHAR(50));
        // INSERT INTO DBZ1960_06 (ID, STR0) VALUES (1, 'STR0-1-0');
        // SAVEPOINT s1;
        // UPDATE DBZ1960_06 SET STR0 = 'STR0-1-1' WHERE ID = 1;
        // ROLLBACK TO SAVEPOINT s1;
        // Version: 26ai Free Release 23.26.2.0.0
        LogInterceptor logInterceptor = new LogInterceptor(AbstractLogMinerTransactionCache.class);
        LogMinerEvent[] events = new LogMinerEvent[]{
                event(EventType.INSERT, 0, "BBBBBBBBBBBBBBBBBB", "1"),
                event(EventType.UPDATE, 0, "BBBBBBBBBBBBBBBBBB", "2"),
                event(EventType.UPDATE, 1, "BBBBBBBBBBBBBBBBBB", "3"), };
        LogMinerEvent[] expected = new LogMinerEvent[]{
                event(EventType.INSERT, 0, "BBBBBBBBBBBBBBBBBB", "1"),
                event(EventType.UPDATE, 1, "BBBBBBBBBBBBBBBBBB", "3"), };
        assertThat(cache(events)).isEqualTo(expected);
        assertThat(logInterceptor.containsWarnMessage("Manual investigation is required")).isFalse();
        assertThat(logInterceptor.containsWarnMessage("Please enable 'log.mining.include.internal.events'")).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void testRollbackUpdateEmpty() throws Exception {
        // CREATE TABLE DBZ1960_07(ID NUMERIC(9,0) PRIMARY KEY, LOB0 CLOB);
        // INSERT INTO DBZ1960_07 (ID, LOB0) VALUES (1, NULL);
        // SAVEPOINT s1;
        // UPDATE DBZ1960_07 SET LOB0 = EMPTY_CLOB() WHERE ID = 1;
        // ROLLBACK TO SAVEPOINT s1;
        // Version: 26ai Free Release 23.26.2.0.0
        LogInterceptor logInterceptor = new LogInterceptor(AbstractLogMinerTransactionCache.class);
        LogMinerEvent[] events = new LogMinerEvent[]{
                event(EventType.INSERT, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.UPDATE, 0, "BBBBBBBBBBBBBBBBBB", "2"),
                event(EventType.UPDATE, 0, "AAAAAAAAAAAAAAAAAA", "3"),
                event(EventType.INTERNAL, 0, "BBBBBBBBBBBBBBBBBB", "4"),
                event(EventType.UPDATE, 1, "BBBBBBBBBBBBBBBBBB", "5"), };
        LogMinerEvent[] expected = new LogMinerEvent[]{
                event(EventType.INSERT, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.UPDATE, 0, "BBBBBBBBBBBBBBBBBB", "2"),
                event(EventType.UPDATE, 1, "BBBBBBBBBBBBBBBBBB", "5"), };
        assertThat(cache(events)).isEqualTo(expected);
        assertThat(logInterceptor.containsWarnMessage("Manual investigation is required")).isFalse();
        assertThat(logInterceptor.containsWarnMessage("Please enable 'log.mining.include.internal.events'")).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void testRollbackUpdateInline() throws Exception {
        // CREATE TABLE DBZ1960_08(ID NUMERIC(9,0) PRIMARY KEY, LOB0 CLOB);
        // INSERT INTO DBZ1960_08 (ID, LOB0) VALUES (1, 'LOB0-1-0');
        // SAVEPOINT s1;
        // UPDATE DBZ1960_08 SET LOB0 = 'LOB0-1-1' WHERE ID = 1;
        // ROLLBACK TO SAVEPOINT s1;
        // Version: 26ai Free Release 23.26.2.0.0
        LogInterceptor logInterceptor = new LogInterceptor(AbstractLogMinerTransactionCache.class);
        LogMinerEvent[] events = new LogMinerEvent[]{
                event(EventType.INSERT, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.UPDATE, 0, "BBBBBBBBBBBBBBBBBB", "2"),
                event(EventType.UPDATE, 0, "AAAAAAAAAAAAAAAAAA", "3"),
                event(EventType.UPDATE, 0, "BBBBBBBBBBBBBBBBBB", "4"),
                event(EventType.UPDATE, 1, "BBBBBBBBBBBBBBBBBB", "5"), };
        LogMinerEvent[] expected = new LogMinerEvent[]{
                event(EventType.INSERT, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.UPDATE, 0, "BBBBBBBBBBBBBBBBBB", "2"),
                event(EventType.UPDATE, 1, "BBBBBBBBBBBBBBBBBB", "5"), };
        assertThat(cache(events)).isEqualTo(expected);
        assertThat(logInterceptor.containsWarnMessage("Manual investigation is required")).isFalse();
        assertThat(logInterceptor.containsWarnMessage("Please enable 'log.mining.include.internal.events'")).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void testRollbackUpdateOutOfLine() throws Exception {
        // CREATE TABLE DBZ1960_09(ID NUMERIC(9,0) PRIMARY KEY, XML0 XMLTYPE, LOB0 CLOB, EXT0 VARCHAR2(8000));
        // INSERT INTO DBZ1960_09 (ID, XML0, LOB0, EXT0) VALUES (1, XMLTYPE('<XML0><ID>1</ID><V>0</V></XML0>'), 'LOB0-1-0', 'EXT0-1-0');
        // SAVEPOINT s1;
        // UPDATE DBZ1960_09 SET XML0 = XMLTYPE('<XML0><ID>1</ID><V>1</V></XML0>'), LOB0 = RPAD('LOB0-1-', 1985, '1'), EXT0 = RPAD('EXT0-1-', 4000, '1') WHERE ID = 1;
        // ROLLBACK TO SAVEPOINT s1;
        // Version: 26ai Free Release 23.26.2.0.0
        LogInterceptor logInterceptor = new LogInterceptor(AbstractLogMinerTransactionCache.class);
        LogMinerEvent[] events = new LogMinerEvent[]{
                event(EventType.INSERT, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.XML_BEGIN, 0, "AAAAAAAAAAAAAAAAAA", "2"),
                event(EventType.XML_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "2"),
                event(EventType.XML_END, 0, "AAAAAAAAAAAAAAAAAA", "2"),
                event(EventType.UPDATE, 0, "BBBBBBBBBBBBBBBBBB", "3"),
                event(EventType.SELECT_LOB_LOCATOR, 0, "AAAAAAAAAAAAAAAAAA", "4"),
                event(EventType.LOB_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "5"),
                event(EventType.EXTENDED_STRING_BEGIN, 0, "AAAAAAAAAAAAAAAAAA", "4"),
                event(EventType.EXTENDED_STRING_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "6"),
                event(EventType.XML_BEGIN, 0, "AAAAAAAAAAAAAAAAAA", "7"),
                event(EventType.XML_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "7"),
                event(EventType.XML_END, 0, "AAAAAAAAAAAAAAAAAA", "7"),
                // event(EventType.INTERNAL, 0, "BBBBBBBBBBBBBBBBBB", "8"), // SEQUENCE#=1
                event(EventType.UPDATE, 1, "BBBBBBBBBBBBBBBBBB", "9"), };
        LogMinerEvent[] expected = new LogMinerEvent[]{
                event(EventType.INSERT, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.XML_BEGIN, 0, "AAAAAAAAAAAAAAAAAA", "2"),
                event(EventType.XML_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "2"),
                event(EventType.XML_END, 0, "AAAAAAAAAAAAAAAAAA", "2"),
                event(EventType.UPDATE, 0, "BBBBBBBBBBBBBBBBBB", "3"),
                event(EventType.UPDATE, 1, "BBBBBBBBBBBBBBBBBB", "9"), };
        assertThat(cache(events)).isEqualTo(expected);
        assertThat(logInterceptor.containsWarnMessage("Manual investigation is required")).isFalse();
        assertThat(logInterceptor.containsWarnMessage("Please enable 'log.mining.include.internal.events'")).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void testRollbackUpdateXml() throws Exception {
        // CREATE TABLE DBZ1960_10(ID NUMERIC(9,0) PRIMARY KEY, XML0 XMLTYPE);
        // INSERT INTO DBZ1960_10 (ID, XML0) VALUES (1, XMLTYPE('<XML0><ID>1</ID><V>0</V></XML0>'));
        // SAVEPOINT s1;
        // UPDATE DBZ1960_10 SET XML0 = XMLTYPE('<XML0><ID>1</ID><V>1</V></XML0>') WHERE ID = 1;
        // ROLLBACK TO SAVEPOINT s1;
        // Version: 26ai Free Release 23.26.2.0.0
        LogInterceptor logInterceptor = new LogInterceptor(AbstractLogMinerTransactionCache.class);
        LogMinerEvent[] events = new LogMinerEvent[]{
                event(EventType.INSERT, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.XML_BEGIN, 0, "AAAAAAAAAAAAAAAAAA", "2"),
                event(EventType.XML_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "2"),
                event(EventType.XML_END, 0, "AAAAAAAAAAAAAAAAAA", "2"),
                event(EventType.INTERNAL, 0, "BBBBBBBBBBBBBBBBBB", "3"),
                event(EventType.XML_BEGIN, 0, "AAAAAAAAAAAAAAAAAA", "4"),
                event(EventType.XML_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "4"),
                event(EventType.XML_END, 0, "AAAAAAAAAAAAAAAAAA", "4"),
                event(EventType.INTERNAL, 0, "BBBBBBBBBBBBBBBBBB", "5"),
                event(EventType.UPDATE, 1, "BBBBBBBBBBBBBBBBBB", "6"), };
        LogMinerEvent[] expected = new LogMinerEvent[]{
                event(EventType.INSERT, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.XML_BEGIN, 0, "AAAAAAAAAAAAAAAAAA", "2"),
                event(EventType.XML_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "2"),
                event(EventType.XML_END, 0, "AAAAAAAAAAAAAAAAAA", "2"),
                event(EventType.INTERNAL, 0, "BBBBBBBBBBBBBBBBBB", "3"),
                event(EventType.UPDATE, 1, "BBBBBBBBBBBBBBBBBB", "6"), };
        assertThat(cache(events)).isEqualTo(expected);
        assertThat(logInterceptor.containsWarnMessage("Manual investigation is required")).isFalse();
        assertThat(logInterceptor.containsWarnMessage("Please enable 'log.mining.include.internal.events'")).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void testRollbackUpdateScalarAndOutOfLine() throws Exception {
        // CREATE TABLE DBZ1960_11(ID NUMERIC(9,0) PRIMARY KEY, STR0 VARCHAR2(50), XML0 XMLTYPE, LOB0 CLOB, EXT0 VARCHAR2(8000));
        // INSERT INTO DBZ1960_11 (ID, STR0, EXT0, LOB0) VALUES (1, 'STR0-1-0', 'EXT0-1-0', 'LOB0-1-0');
        // SAVEPOINT s1;
        // UPDATE DBZ1960_11 SET STR0 = 'STR0-1-1', XML0 = XMLTYPE('<XML0><ID>1</ID><V>1</V></XML0>'), EXT0 = RPAD('EXT0-1-', 4000, '1'), LOB0 = RPAD('LOB0-1-', 1985, '1') WHERE ID = 1;
        // ROLLBACK TO SAVEPOINT s1;
        // Version: 26ai Free Release 23.26.2.0.0
        LogInterceptor logInterceptor = new LogInterceptor(AbstractLogMinerTransactionCache.class);
        LogMinerEvent[] events = new LogMinerEvent[]{
                event(EventType.INSERT, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.UPDATE, 0, "BBBBBBBBBBBBBBBBBB", "2"),
                event(EventType.UPDATE, 0, "AAAAAAAAAAAAAAAAAA", "3"),
                event(EventType.EXTENDED_STRING_BEGIN, 0, "AAAAAAAAAAAAAAAAAA", "3"),
                event(EventType.EXTENDED_STRING_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "4"),
                event(EventType.SELECT_LOB_LOCATOR, 0, "AAAAAAAAAAAAAAAAAA", "3"),
                event(EventType.LOB_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "5"),
                event(EventType.XML_BEGIN, 0, "AAAAAAAAAAAAAAAAAA", "6"),
                event(EventType.XML_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "6"),
                event(EventType.XML_END, 0, "AAAAAAAAAAAAAAAAAA", "6"),
                // event(EventType.INTERNAL, 0, "BBBBBBBBBBBBBBBBBB", "7"), // SEQUENCE#=1
                event(EventType.UPDATE, 1, "BBBBBBBBBBBBBBBBBB", "8"), };
        LogMinerEvent[] expected = new LogMinerEvent[]{
                event(EventType.INSERT, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.UPDATE, 0, "BBBBBBBBBBBBBBBBBB", "2"),
                event(EventType.UPDATE, 1, "BBBBBBBBBBBBBBBBBB", "8"), };
        assertThat(cache(events)).isEqualTo(expected);
        assertThat(logInterceptor.containsWarnMessage("Manual investigation is required")).isFalse();
        assertThat(logInterceptor.containsWarnMessage("Please enable 'log.mining.include.internal.events'")).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void testRollbackUpdateScalarAndXml() throws Exception {
        // CREATE TABLE DBZ1960_12(ID NUMERIC(9,0) PRIMARY KEY, STR0 VARCHAR2(50), XML0 XMLTYPE);
        // INSERT INTO DBZ1960_12 (ID, STR0, XML0) VALUES (1, 'STR0-1-0', XMLTYPE('<XML0><ID>1</ID><V>0</V></XML0>'));
        // SAVEPOINT s1;
        // UPDATE DBZ1960_12 SET STR0 = 'STR0-1-1', XML0 = XMLTYPE('<XML0><ID>1</ID><V>1</V></XML0>') WHERE ID = 1;
        // ROLLBACK TO SAVEPOINT s1;
        // Version: 26ai Free Release 23.26.2.0.0
        LogInterceptor logInterceptor = new LogInterceptor(AbstractLogMinerTransactionCache.class);
        LogMinerEvent[] events = new LogMinerEvent[]{
                event(EventType.INSERT, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.XML_BEGIN, 0, "AAAAAAAAAAAAAAAAAA", "2"),
                event(EventType.XML_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "2"),
                event(EventType.XML_END, 0, "AAAAAAAAAAAAAAAAAA", "2"),
                event(EventType.INTERNAL, 0, "BBBBBBBBBBBBBBBBBB", "3"),
                event(EventType.UPDATE, 0, "AAAAAAAAAAAAAAAAAA", "4"),
                event(EventType.XML_BEGIN, 0, "AAAAAAAAAAAAAAAAAA", "5"),
                event(EventType.XML_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "5"),
                event(EventType.XML_END, 0, "AAAAAAAAAAAAAAAAAA", "5"),
                event(EventType.INTERNAL, 0, "BBBBBBBBBBBBBBBBBB", "6"),
                event(EventType.UPDATE, 1, "BBBBBBBBBBBBBBBBBB", "7"), };
        LogMinerEvent[] expected = new LogMinerEvent[]{
                event(EventType.INSERT, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.XML_BEGIN, 0, "AAAAAAAAAAAAAAAAAA", "2"),
                event(EventType.XML_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "2"),
                event(EventType.XML_END, 0, "AAAAAAAAAAAAAAAAAA", "2"),
                event(EventType.INTERNAL, 0, "BBBBBBBBBBBBBBBBBB", "3"),
                event(EventType.UPDATE, 1, "BBBBBBBBBBBBBBBBBB", "7"), };
        assertThat(cache(events)).isEqualTo(expected);
        assertThat(logInterceptor.containsWarnMessage("Manual investigation is required")).isFalse();
        assertThat(logInterceptor.containsWarnMessage("Please enable 'log.mining.include.internal.events'")).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void testRollbackUpdateInlineAndOutOfLine() throws Exception {
        // CREATE TABLE DBZ1960_13(ID NUMERIC(9,0) PRIMARY KEY, LOB0 CLOB, XML0 XMLTYPE, LOB1 CLOB, EXT0 VARCHAR2(8000));
        // INSERT INTO DBZ1960_13 (ID, LOB0, XML0, EXT0, LOB1) VALUES (1, 'LOB0-1-0', XMLTYPE('<XML0><ID>1</ID><V>0</V></XML0>'), 'EXT0-1-0', 'LOB1-1-0');
        // SAVEPOINT s1;
        // UPDATE DBZ1960_13 SET LOB0 = RPAD('LOB0-1-', 1985, '1'), XML0 = XMLTYPE('<XML0><ID>1</ID><V>1</V></XML0>'), EXT0 = RPAD('EXT0-1-', 4000, '1'), LOB1 = 'LOB1-1-1' WHERE ID = 1;
        // ROLLBACK TO SAVEPOINT s1;
        // Version: 26ai Free Release 23.26.2.0.0
        LogInterceptor logInterceptor = new LogInterceptor(AbstractLogMinerTransactionCache.class);
        LogMinerEvent[] events = new LogMinerEvent[]{
                event(EventType.INSERT, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.XML_BEGIN, 0, "AAAAAAAAAAAAAAAAAA", "2"),
                event(EventType.XML_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "2"),
                event(EventType.XML_END, 0, "AAAAAAAAAAAAAAAAAA", "2"),
                event(EventType.UPDATE, 0, "BBBBBBBBBBBBBBBBBB", "3"),
                event(EventType.SELECT_LOB_LOCATOR, 0, "AAAAAAAAAAAAAAAAAA", "4"),
                event(EventType.LOB_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "5"),
                event(EventType.EXTENDED_STRING_BEGIN, 0, "AAAAAAAAAAAAAAAAAA", "4"),
                event(EventType.EXTENDED_STRING_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "6"),
                event(EventType.XML_BEGIN, 0, "AAAAAAAAAAAAAAAAAA", "7"),
                event(EventType.XML_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "7"),
                event(EventType.XML_END, 0, "AAAAAAAAAAAAAAAAAA", "7"),
                event(EventType.UPDATE, 0, "BBBBBBBBBBBBBBBBBB", "8"),
                event(EventType.UPDATE, 1, "BBBBBBBBBBBBBBBBBB", "9"), };
        LogMinerEvent[] expected = new LogMinerEvent[]{
                event(EventType.INSERT, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.XML_BEGIN, 0, "AAAAAAAAAAAAAAAAAA", "2"),
                event(EventType.XML_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "2"),
                event(EventType.XML_END, 0, "AAAAAAAAAAAAAAAAAA", "2"),
                event(EventType.UPDATE, 0, "BBBBBBBBBBBBBBBBBB", "3"),
                event(EventType.UPDATE, 1, "BBBBBBBBBBBBBBBBBB", "9"), };
        assertThat(cache(events)).isEqualTo(expected);
        assertThat(logInterceptor.containsWarnMessage("Manual investigation is required")).isFalse();
        assertThat(logInterceptor.containsWarnMessage("Please enable 'log.mining.include.internal.events'")).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void testRollbackUpdateScalarAndInlineAndOutOfLine() throws Exception {
        // CREATE TABLE DBZ1960_14(ID NUMERIC(9,0) PRIMARY KEY, STR0 VARCHAR2(50), LOB0 CLOB, XML0 XMLTYPE, LOB1 CLOB, EXT0 VARCHAR2(8000));
        // INSERT INTO DBZ1960_14 (ID, STR0, LOB0, EXT0, LOB1) VALUES (1, 'STR0-1-0', 'LOB0-1-0', 'EXT0-1-0', 'LOB1-1-0');
        // SAVEPOINT s1;
        // UPDATE DBZ1960_14 SET STR0 = 'STR0-1-1', EXT0 = RPAD('EXT0-1-', 4000, '1'), XML0 = XMLTYPE('<XML0><ID>1</ID><V>1</V></XML0>'), LOB0 = RPAD('LOB0-1-', 1985, '1'), LOB1 = 'LOB1-1-1' WHERE ID = 1;
        // ROLLBACK TO SAVEPOINT s1;
        // Version: 26ai Free Release 23.26.2.0.0
        LogInterceptor logInterceptor = new LogInterceptor(AbstractLogMinerTransactionCache.class);
        LogMinerEvent[] events = new LogMinerEvent[]{
                event(EventType.INSERT, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.UPDATE, 0, "BBBBBBBBBBBBBBBBBB", "2"),
                event(EventType.UPDATE, 0, "AAAAAAAAAAAAAAAAAA", "3"),
                event(EventType.EXTENDED_STRING_BEGIN, 0, "AAAAAAAAAAAAAAAAAA", "3"),
                event(EventType.EXTENDED_STRING_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "4"),
                event(EventType.SELECT_LOB_LOCATOR, 0, "AAAAAAAAAAAAAAAAAA", "3"),
                event(EventType.LOB_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "5"),
                event(EventType.XML_BEGIN, 0, "AAAAAAAAAAAAAAAAAA", "6"),
                event(EventType.XML_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "6"),
                event(EventType.XML_END, 0, "AAAAAAAAAAAAAAAAAA", "6"),
                event(EventType.UPDATE, 0, "BBBBBBBBBBBBBBBBBB", "7"),
                event(EventType.UPDATE, 1, "BBBBBBBBBBBBBBBBBB", "8"), };
        LogMinerEvent[] expected = new LogMinerEvent[]{
                event(EventType.INSERT, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.UPDATE, 0, "BBBBBBBBBBBBBBBBBB", "2"),
                event(EventType.UPDATE, 1, "BBBBBBBBBBBBBBBBBB", "8"), };
        assertThat(cache(events)).isEqualTo(expected);
        assertThat(logInterceptor.containsWarnMessage("Manual investigation is required")).isFalse();
        assertThat(logInterceptor.containsWarnMessage("Please enable 'log.mining.include.internal.events'")).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void testRollbackLobWrite() throws Exception {
        // CREATE TABLE DBZ1960_15(ID NUMERIC(9,0) PRIMARY KEY, LOB0 CLOB);
        // INSERT INTO DBZ1960_15 (ID, LOB0) VALUES (1, 'LOB0-1-0');
        // SAVEPOINT s1;
        // DECLARE
        // loc CLOB;
        // BEGIN
        // SELECT LOB0 INTO loc FROM DBZ1960_15 WHERE ID = 1 FOR UPDATE;
        // DBMS_LOB.WRITE(loc, 1985, 1, RPAD('LOB0-1-', 1985, '1'));
        // END;
        // ROLLBACK TO SAVEPOINT s1;
        // Version: 26ai Free Release 23.26.2.0.0
        LogInterceptor logInterceptor = new LogInterceptor(AbstractLogMinerTransactionCache.class);
        LogMinerEvent[] events = new LogMinerEvent[]{
                event(EventType.INSERT, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.UPDATE, 0, "BBBBBBBBBBBBBBBBBB", "2"),
                event(EventType.SELECT_LOB_LOCATOR, 0, "BBBBBBBBBBBBBBBBBB", "3"),
                event(EventType.LOB_WRITE, 0, "BBBBBBBBBBBBBBBBBB", "4"),
                event(EventType.INTERNAL, 0, "BBBBBBBBBBBBBBBBBB", "5"),
                event(EventType.UPDATE, 1, "BBBBBBBBBBBBBBBBBB", "6"), };
        LogMinerEvent[] expected = new LogMinerEvent[]{
                event(EventType.INSERT, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.UPDATE, 0, "BBBBBBBBBBBBBBBBBB", "2"),
                event(EventType.UPDATE, 1, "BBBBBBBBBBBBBBBBBB", "6"), };
        assertThat(cache(events)).isEqualTo(expected);
        assertThat(logInterceptor.containsWarnMessage("Manual investigation is required")).isFalse();
        assertThat(logInterceptor.containsWarnMessage("Please enable 'log.mining.include.internal.events'")).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void testRollbackLobTrim() throws Exception {
        // CREATE TABLE DBZ1960_16(ID NUMERIC(9,0) PRIMARY KEY, LOB0 CLOB);
        // INSERT INTO DBZ1960_16 (ID, LOB0) VALUES (1, 'LOB0-1-00');
        // SAVEPOINT s1;
        // DECLARE
        // loc CLOB;
        // BEGIN
        // SELECT LOB0 INTO loc FROM DBZ1960_16 WHERE ID = 1 FOR UPDATE;
        // DBMS_LOB.TRIM(loc, 8);
        // END;
        // ROLLBACK TO SAVEPOINT s1;
        // Version: 26ai Free Release 23.26.2.0.0
        LogInterceptor logInterceptor = new LogInterceptor(AbstractLogMinerTransactionCache.class);
        LogMinerEvent[] events = new LogMinerEvent[]{
                event(EventType.INSERT, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.UPDATE, 0, "BBBBBBBBBBBBBBBBBB", "2"),
                event(EventType.SELECT_LOB_LOCATOR, 0, "BBBBBBBBBBBBBBBBBB", "3"),
                // event(EventType.LOB_TRIM, 0, "AAAAAAAAAAAAAAAAAA", "4"),
                event(EventType.INTERNAL, 0, "BBBBBBBBBBBBBBBBBB", "5"),
                event(EventType.UPDATE, 1, "BBBBBBBBBBBBBBBBBB", "6"), };
        LogMinerEvent[] expected = new LogMinerEvent[]{
                event(EventType.INSERT, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.UPDATE, 0, "BBBBBBBBBBBBBBBBBB", "2"),
                event(EventType.UPDATE, 1, "BBBBBBBBBBBBBBBBBB", "6"), };
        assertThat(cache(events)).isEqualTo(expected);
        assertThat(logInterceptor.containsWarnMessage("Manual investigation is required")).isFalse();
        assertThat(logInterceptor.containsWarnMessage("Please enable 'log.mining.include.internal.events'")).isFalse();
    }

    // DELETE

    @Test
    @FixFor("debezium/dbz#1960")
    public void testRollbackDelete() throws Exception {
        // CREATE TABLE DBZ1960_17(ID NUMERIC(9,0) PRIMARY KEY, STR0 VARCHAR2(50), LOB0 CLOB, XML0 XMLTYPE, LOB1 CLOB, EXT0 VARCHAR2(8000));
        // INSERT INTO DBZ1960_17 (ID, STR0, LOB0, XML0, EXT0, LOB1) VALUES (1, 'STR0-1-0', RPAD('LOB0-1-', 1985, '0'), XMLTYPE('<XML0><ID>1</ID><V>0</V></XML0>'), RPAD('EXT0-1-', 4000, '0'), 'LOB1-1-0');
        // SAVEPOINT s1;
        // DELETE FROM DBZ1960_17 WHERE ID = 1;
        // ROLLBACK TO SAVEPOINT s1;
        // Version: 26ai Free Release 23.26.2.0.0
        LogInterceptor logInterceptor = new LogInterceptor(AbstractLogMinerTransactionCache.class);
        LogMinerEvent[] events = new LogMinerEvent[]{
                event(EventType.INSERT, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.SELECT_LOB_LOCATOR, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.LOB_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "2"),
                event(EventType.EXTENDED_STRING_BEGIN, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.EXTENDED_STRING_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "3"),
                event(EventType.XML_BEGIN, 0, "AAAAAAAAAAAAAAAAAA", "4"),
                event(EventType.XML_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "4"),
                event(EventType.XML_END, 0, "AAAAAAAAAAAAAAAAAA", "4"),
                event(EventType.UPDATE, 0, "BBBBBBBBBBBBBBBBBB", "5"),
                event(EventType.DELETE, 0, "BBBBBBBBBBBBBBBBBB", "6"),
                event(EventType.INSERT, 1, "BBBBBBBBBBBBBBBBBB", "7"), };
        LogMinerEvent[] expected = new LogMinerEvent[]{
                event(EventType.INSERT, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.SELECT_LOB_LOCATOR, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.LOB_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "2"),
                event(EventType.EXTENDED_STRING_BEGIN, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.EXTENDED_STRING_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "3"),
                event(EventType.XML_BEGIN, 0, "AAAAAAAAAAAAAAAAAA", "4"),
                event(EventType.XML_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "4"),
                event(EventType.XML_END, 0, "AAAAAAAAAAAAAAAAAA", "4"),
                event(EventType.UPDATE, 0, "BBBBBBBBBBBBBBBBBB", "5"),
                event(EventType.INSERT, 1, "BBBBBBBBBBBBBBBBBB", "7"), };
        assertThat(cache(events)).isEqualTo(expected);
        assertThat(logInterceptor.containsWarnMessage("Manual investigation is required")).isFalse();
        assertThat(logInterceptor.containsWarnMessage("Please enable 'log.mining.include.internal.events'")).isFalse();
    }

    // Rollback multiple operations

    @Test
    @FixFor("debezium/dbz#1960")
    public void testRollbacks() throws Exception {
        // CREATE TABLE DBZ1960_18(ID NUMERIC(9,0) PRIMARY KEY, STR0 VARCHAR2(50));
        // INSERT INTO DBZ1960_18 (ID, STR0) VALUES (1, 'STR0-1-0');
        // INSERT INTO DBZ1960_18 (ID, STR0) VALUES (2, 'STR0-2-0');
        // SAVEPOINT s1;
        // INSERT INTO DBZ1960_18 (ID, STR0) VALUES (3, 'STR0-3-0');
        // UPDATE DBZ1960_18 SET STR0 = 'STR0-2-1' WHERE ID = 2;
        // DELETE FROM DBZ1960_18 WHERE ID = 1;
        // ROLLBACK TO SAVEPOINT s1;
        // Version: 26ai Free Release 23.26.2.0.0
        LogInterceptor logInterceptor = new LogInterceptor(AbstractLogMinerTransactionCache.class);
        LogMinerEvent[] events = new LogMinerEvent[]{
                event(EventType.INSERT, 0, "BBBBBBBBBBBBBBBBBB", "1"),
                event(EventType.INSERT, 0, "CCCCCCCCCCCCCCCCCC", "2"),
                event(EventType.INSERT, 0, "DDDDDDDDDDDDDDDDDD", "3"),
                event(EventType.UPDATE, 0, "CCCCCCCCCCCCCCCCCC", "4"),
                event(EventType.DELETE, 0, "BBBBBBBBBBBBBBBBBB", "5"),
                event(EventType.INSERT, 1, "BBBBBBBBBBBBBBBBBB", "6"),
                event(EventType.UPDATE, 1, "CCCCCCCCCCCCCCCCCC", "7"),
                event(EventType.DELETE, 1, "DDDDDDDDDDDDDDDDDD", "8"), };
        LogMinerEvent[] expected = new LogMinerEvent[]{
                event(EventType.INSERT, 0, "BBBBBBBBBBBBBBBBBB", "1"),
                event(EventType.INSERT, 0, "CCCCCCCCCCCCCCCCCC", "2"),
                event(EventType.INSERT, 1, "BBBBBBBBBBBBBBBBBB", "6"),
                event(EventType.UPDATE, 1, "CCCCCCCCCCCCCCCCCC", "7"),
                event(EventType.DELETE, 1, "DDDDDDDDDDDDDDDDDD", "8"), };
        assertThat(cache(events)).isEqualTo(expected);
        assertThat(logInterceptor.containsWarnMessage("Manual investigation is required")).isFalse();
        assertThat(logInterceptor.containsWarnMessage("Please enable 'log.mining.include.internal.events'")).isFalse();
    }

    // Supported without INTERNAL

    @Test
    @FixFor("debezium/dbz#1960")
    public void testRollbackUpdateOutOfLineWithoutInternal() throws Exception {
        // CREATE TABLE DBZ1960_19(ID NUMERIC(9,0) PRIMARY KEY, LOB0 CLOB);
        // INSERT INTO DBZ1960_19 (ID, LOB0) VALUES (1, 'LOB0-1-0');
        // SAVEPOINT s1;
        // UPDATE DBZ1960_19 SET LOB0 = RPAD('LOB0-1-', 1985, '0') WHERE ID = 1;
        // ROLLBACK TO SAVEPOINT s1;
        // Version: 26ai Free Release 23.26.2.0.0
        LogInterceptor logInterceptor = new LogInterceptor(AbstractLogMinerTransactionCache.class);
        LogMinerEvent[] events = new LogMinerEvent[]{
                event(EventType.INSERT, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.UPDATE, 0, "BBBBBBBBBBBBBBBBBB", "2"),
                event(EventType.SELECT_LOB_LOCATOR, 0, "AAAAAAAAAAAAAAAAAA", "3"),
                event(EventType.LOB_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "4"),
                // event(EventType.INTERNAL, 0, "BBBBBBBBBBBBBBBBBB", "5"),
                event(EventType.UPDATE, 1, "BBBBBBBBBBBBBBBBBB", "6"), };
        LogMinerEvent[] expected = new LogMinerEvent[]{
                event(EventType.INSERT, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.UPDATE, 0, "BBBBBBBBBBBBBBBBBB", "2"),
                event(EventType.UPDATE, 1, "BBBBBBBBBBBBBBBBBB", "6"), };
        assertThat(cache(events)).isEqualTo(expected);
        assertThat(logInterceptor.containsWarnMessage("Manual investigation is required")).isFalse();
        assertThat(logInterceptor.containsWarnMessage("Please enable 'log.mining.include.internal.events'")).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void testInsertEmptyWithoutInternalAndRollbackUpdateScalar() throws Exception {
        // CREATE TABLE DBZ1960_20(ID NUMERIC(9,0) PRIMARY KEY, STR0 VARCHAR2(50), LOB0 CLOB);
        // INSERT INTO DBZ1960_20 (ID, STR0, LOB0) VALUES (1, 'STR0-1-0', EMPTY_CLOB());
        // SAVEPOINT s1;
        // UPDATE DBZ1960_20 SET STR0 = 'STR0-1-1' WHERE ID = 1;
        // ROLLBACK TO SAVEPOINT s1;
        // Version: 26ai Free Release 23.26.2.0.0
        LogInterceptor logInterceptor = new LogInterceptor(AbstractLogMinerTransactionCache.class);
        LogMinerEvent[] events = new LogMinerEvent[]{
                event(EventType.INSERT, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                // event(EventType.INTERNAL, 0, "BBBBBBBBBBBBBBBBBB", "2"),
                event(EventType.UPDATE, 0, "BBBBBBBBBBBBBBBBBB", "3"),
                event(EventType.UPDATE, 1, "BBBBBBBBBBBBBBBBBB", "4"), };
        LogMinerEvent[] expected = new LogMinerEvent[]{
                event(EventType.INSERT, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.UPDATE, 1, "BBBBBBBBBBBBBBBBBB", "4"), };
        assertThat(cache(events)).isEqualTo(expected);
        assertThat(logInterceptor.containsWarnMessage("Manual investigation is required")).isFalse();
        assertThat(logInterceptor.containsWarnMessage("Please enable 'log.mining.include.internal.events'")).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void testUpdateEmptyWithoutInternalAndRollbackUpdateInlineAndOutOfLine() throws Exception {
        // CREATE TABLE DBZ1960_21(ID NUMERIC(9,0) PRIMARY KEY, LOB0 CLOB, LOB1 CLOB);
        // INSERT INTO DBZ1960_21 (ID, LOB0, LOB1) VALUES (1, NULL, NULL);
        // UPDATE DBZ1960_21 SET LOB0 = EMPTY_CLOB(), LOB1 = EMPTY_CLOB() WHERE ID = 1;
        // SAVEPOINT s1;
        // UPDATE DBZ1960_21 SET LOB0 = RPAD('LOB0-1-', 1985, '2'), LOB1 = 'LOB1-1-2' WHERE ID = 1;
        // ROLLBACK TO SAVEPOINT s1;
        // Version: 26ai Free Release 23.26.2.0.0
        LogInterceptor logInterceptor = new LogInterceptor(AbstractLogMinerTransactionCache.class);
        LogMinerEvent[] events = new LogMinerEvent[]{
                event(EventType.INSERT, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.UPDATE, 0, "BBBBBBBBBBBBBBBBBB", "2"),
                event(EventType.UPDATE, 0, "AAAAAAAAAAAAAAAAAA", "3"),
                // event(EventType.INTERNAL, 0, "BBBBBBBBBBBBBBBBBB", "4"),
                event(EventType.SELECT_LOB_LOCATOR, 0, "AAAAAAAAAAAAAAAAAA", "5"),
                event(EventType.LOB_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "6"),
                event(EventType.UPDATE, 0, "BBBBBBBBBBBBBBBBBB", "7"),
                event(EventType.UPDATE, 1, "BBBBBBBBBBBBBBBBBB", "8"), };
        LogMinerEvent[] expected = new LogMinerEvent[]{
                event(EventType.INSERT, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.UPDATE, 0, "BBBBBBBBBBBBBBBBBB", "2"),
                event(EventType.UPDATE, 0, "AAAAAAAAAAAAAAAAAA", "3"),
                event(EventType.UPDATE, 1, "BBBBBBBBBBBBBBBBBB", "8"), };
        assertThat(cache(events)).isEqualTo(expected);
        assertThat(logInterceptor.containsWarnMessage("Manual investigation is required")).isFalse();
        assertThat(logInterceptor.containsWarnMessage("Please enable 'log.mining.include.internal.events'")).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void testUpdateOutOfLineWithoutInternalAndRollbackUpdateInlineAndOutOfLine() throws Exception {
        // CREATE TABLE DBZ1960_22(ID NUMERIC(9,0) PRIMARY KEY, LOB0 CLOB, EXT0 VARCHAR2(8000));
        // INSERT INTO DBZ1960_22 (ID, LOB0, EXT0) VALUES (1, 'LOB0-1-0', 'EXT0-1-0');
        // UPDATE DBZ1960_22 SET LOB0 = RPAD('LOB0-1-', 1985, '1') WHERE ID = 1
        // SAVEPOINT s1;
        // UPDATE DBZ1960_22 SET LOB0 = 'LOB0-1-2', EXT0 = RPAD('EXT0-1-', 4000, '2') WHERE ID = 1;
        // ROLLBACK TO SAVEPOINT s1;
        // Version: 26ai Free Release 23.26.2.0.0
        LogInterceptor logInterceptor = new LogInterceptor(AbstractLogMinerTransactionCache.class);
        LogMinerEvent[] events = new LogMinerEvent[]{
                event(EventType.INSERT, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.UPDATE, 0, "BBBBBBBBBBBBBBBBBB", "2"),
                event(EventType.SELECT_LOB_LOCATOR, 0, "AAAAAAAAAAAAAAAAAA", "3"),
                event(EventType.LOB_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "4"),
                // event(EventType.INTERNAL, 0, "BBBBBBBBBBBBBBBBBB", "5"),
                event(EventType.EXTENDED_STRING_BEGIN, 0, "AAAAAAAAAAAAAAAAAA", "6"),
                event(EventType.EXTENDED_STRING_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "7"),
                event(EventType.UPDATE, 0, "BBBBBBBBBBBBBBBBBB", "8"),
                event(EventType.UPDATE, 1, "BBBBBBBBBBBBBBBBBB", "9"), };
        LogMinerEvent[] expected = new LogMinerEvent[]{
                event(EventType.INSERT, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.UPDATE, 0, "BBBBBBBBBBBBBBBBBB", "2"),
                event(EventType.SELECT_LOB_LOCATOR, 0, "AAAAAAAAAAAAAAAAAA", "3"),
                event(EventType.LOB_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "4"),
                event(EventType.UPDATE, 1, "BBBBBBBBBBBBBBBBBB", "9"), };
        assertThat(cache(events)).isEqualTo(expected);
        assertThat(logInterceptor.containsWarnMessage("Manual investigation is required")).isFalse();
        assertThat(logInterceptor.containsWarnMessage("Please enable 'log.mining.include.internal.events'")).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void testInsertOutOfLineWithoutInternalAndRollbackUpdateScalar() throws Exception {
        // CREATE TABLE DBZ1960_23(ID NUMERIC(9,0) PRIMARY KEY, STR0 VARCHAR2(50), LOB0 CLOB);
        // INSERT INTO DBZ1960_23 (ID, STR0, LOB0) VALUES (1, 'STR0-1-0', RPAD('LOB0-1-', 1985, '0'));
        // SAVEPOINT s1;
        // UPDATE DBZ1960_23 SET STR0 = 'STR0-1-1' WHERE ID = 1;
        // ROLLBACK TO SAVEPOINT s1;
        // Version: 26ai Free Release 23.26.2.0.0
        LogInterceptor logInterceptor = new LogInterceptor(AbstractLogMinerTransactionCache.class);
        LogMinerEvent[] events = new LogMinerEvent[]{
                event(EventType.INSERT, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.SELECT_LOB_LOCATOR, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.LOB_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "2"),
                // event(EventType.INTERNAL, 0, "BBBBBBBBBBBBBBBBBB", "3"),
                event(EventType.UPDATE, 0, "BBBBBBBBBBBBBBBBBB", "4"),
                event(EventType.UPDATE, 1, "BBBBBBBBBBBBBBBBBB", "5"), };
        LogMinerEvent[] expected = new LogMinerEvent[]{
                event(EventType.INSERT, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.SELECT_LOB_LOCATOR, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.LOB_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "2"),
                event(EventType.UPDATE, 1, "BBBBBBBBBBBBBBBBBB", "5"), };
        assertThat(cache(events)).isEqualTo(expected);
        assertThat(logInterceptor.containsWarnMessage("Manual investigation is required")).isFalse();
        assertThat(logInterceptor.containsWarnMessage("Please enable 'log.mining.include.internal.events'")).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void testUpdateXmlWithoutInternalAndRollbackUpdateInlineAndOutOfLine() throws Exception {
        // CREATE TABLE DBZ1960_24(ID NUMERIC(9,0) PRIMARY KEY, XML0 XMLTYPE, EXT0 VARCHAR2(8000), LOB0 CLOB);
        // INSERT INTO DBZ1960_24 (ID, XML0, EXT0, LOB0) VALUES (1, XMLTYPE('<XML0><ID>1</ID><V>0</V></XML0>'), 'EXT0-1-0', 'LOB0-1-0');
        // UPDATE DBZ1960_24 SET XML0 = XMLTYPE('<XML0><ID>1</ID><V>1</V></XML0>') WHERE ID = 1;
        // SAVEPOINT s1;
        // UPDATE DBZ1960_24 SET EXT0 = 'EXT0-1-2', LOB0 = RPAD('LOB0-1-', 1985, '2') WHERE ID = 1;
        // ROLLBACK TO SAVEPOINT s1;
        // Version: 26ai Free Release 23.26.2.0.0
        LogInterceptor logInterceptor = new LogInterceptor(AbstractLogMinerTransactionCache.class);
        LogMinerEvent[] events = new LogMinerEvent[]{
                event(EventType.INSERT, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.XML_BEGIN, 0, "AAAAAAAAAAAAAAAAAA", "2"),
                event(EventType.XML_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "2"),
                event(EventType.XML_END, 0, "AAAAAAAAAAAAAAAAAA", "2"),
                event(EventType.UPDATE, 0, "BBBBBBBBBBBBBBBBBB", "3"),
                event(EventType.XML_BEGIN, 0, "AAAAAAAAAAAAAAAAAA", "4"),
                event(EventType.XML_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "4"),
                event(EventType.XML_END, 0, "AAAAAAAAAAAAAAAAAA", "4"),
                // event(EventType.INTERNAL, 0, "BBBBBBBBBBBBBBBBBB", "5"),
                event(EventType.SELECT_LOB_LOCATOR, 0, "AAAAAAAAAAAAAAAAAA", "6"),
                event(EventType.LOB_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "7"),
                event(EventType.UPDATE, 0, "BBBBBBBBBBBBBBBBBB", "8"),
                event(EventType.UPDATE, 1, "BBBBBBBBBBBBBBBBBB", "9"), };
        LogMinerEvent[] expected = new LogMinerEvent[]{
                event(EventType.INSERT, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.XML_BEGIN, 0, "AAAAAAAAAAAAAAAAAA", "2"),
                event(EventType.XML_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "2"),
                event(EventType.XML_END, 0, "AAAAAAAAAAAAAAAAAA", "2"),
                event(EventType.UPDATE, 0, "BBBBBBBBBBBBBBBBBB", "3"),
                event(EventType.XML_BEGIN, 0, "AAAAAAAAAAAAAAAAAA", "4"),
                event(EventType.XML_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "4"),
                event(EventType.XML_END, 0, "AAAAAAAAAAAAAAAAAA", "4"),
                event(EventType.UPDATE, 1, "BBBBBBBBBBBBBBBBBB", "9"), };
        assertThat(cache(events)).isEqualTo(expected);
        assertThat(logInterceptor.containsWarnMessage("Manual investigation is required")).isFalse();
        assertThat(logInterceptor.containsWarnMessage("Please enable 'log.mining.include.internal.events'")).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void testUpdateOutOfLineAndXmlWithoutInternalAndRollbackUpdateInlineAndXml() throws Exception {
        // CREATE TABLE DBZ1960_25(ID NUMERIC(9,0) PRIMARY KEY, EXT0 VARCHAR2(8000), XML0 XMLTYPE);
        // INSERT INTO DBZ1960_25 (ID, EXT0, XML0) VALUES (1, 'EXT0-1-0', XMLTYPE('<XML0><ID>1</ID><V>0</V></XML0>'));
        // UPDATE DBZ1960_25 SET EXT0 = RPAD('EXT0-1-', 4000, '1'), XML0 = XMLTYPE('<XML0><ID>1</ID><V>1</V></XML0>') WHERE ID = 1;
        // SAVEPOINT s1;
        // UPDATE DBZ1960_25 SET EXT0 = 'EXT0-1-2', XML0 = XMLTYPE('<XML0><ID>1</ID><V>2</V></XML0>') WHERE ID = 1;
        // ROLLBACK TO SAVEPOINT s1;
        // Version: 26ai Free Release 23.26.2.0.0
        LogInterceptor logInterceptor = new LogInterceptor(AbstractLogMinerTransactionCache.class);
        LogMinerEvent[] events = new LogMinerEvent[]{
                event(EventType.INSERT, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.XML_BEGIN, 0, "AAAAAAAAAAAAAAAAAA", "2"),
                event(EventType.XML_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "2"),
                event(EventType.XML_END, 0, "AAAAAAAAAAAAAAAAAA", "2"),
                event(EventType.UPDATE, 0, "BBBBBBBBBBBBBBBBBB", "3"),
                event(EventType.EXTENDED_STRING_BEGIN, 0, "AAAAAAAAAAAAAAAAAA", "4"),
                event(EventType.EXTENDED_STRING_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "5"),
                event(EventType.XML_BEGIN, 0, "AAAAAAAAAAAAAAAAAA", "6"),
                event(EventType.XML_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "6"),
                event(EventType.XML_END, 0, "AAAAAAAAAAAAAAAAAA", "6"),
                // event(EventType.INTERNAL, 0, "BBBBBBBBBBBBBBBBBB", "7"), // SEQUENCE#=1
                event(EventType.INTERNAL, 0, "AAAAAAAAAAAAAAAAAA", "6"), // added by Debezium
                event(EventType.XML_BEGIN, 0, "AAAAAAAAAAAAAAAAAA", "8"),
                event(EventType.XML_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "8"),
                event(EventType.XML_END, 0, "AAAAAAAAAAAAAAAAAA", "8"),
                event(EventType.UPDATE, 0, "BBBBBBBBBBBBBBBBBB", "9"),
                event(EventType.UPDATE, 1, "BBBBBBBBBBBBBBBBBB", "10"), };
        LogMinerEvent[] expected = new LogMinerEvent[]{
                event(EventType.INSERT, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.XML_BEGIN, 0, "AAAAAAAAAAAAAAAAAA", "2"),
                event(EventType.XML_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "2"),
                event(EventType.XML_END, 0, "AAAAAAAAAAAAAAAAAA", "2"),
                event(EventType.UPDATE, 0, "BBBBBBBBBBBBBBBBBB", "3"),
                event(EventType.EXTENDED_STRING_BEGIN, 0, "AAAAAAAAAAAAAAAAAA", "4"),
                event(EventType.EXTENDED_STRING_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "5"),
                event(EventType.XML_BEGIN, 0, "AAAAAAAAAAAAAAAAAA", "6"),
                event(EventType.XML_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "6"),
                event(EventType.XML_END, 0, "AAAAAAAAAAAAAAAAAA", "6"),
                event(EventType.INTERNAL, 0, "AAAAAAAAAAAAAAAAAA", "6"),
                event(EventType.UPDATE, 1, "BBBBBBBBBBBBBBBBBB", "10"), };
        assertThat(cache(events)).isEqualTo(expected);
        assertThat(logInterceptor.containsWarnMessage("Manual investigation is required")).isFalse();
        assertThat(logInterceptor.containsWarnMessage("Please enable 'log.mining.include.internal.events'")).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void testRollbackLobWriteWithoutInternal() throws Exception {
        // CREATE TABLE DBZ1960_26(ID NUMERIC(9,0) PRIMARY KEY, LOB0 CLOB);
        // INSERT INTO DBZ1960_26 (ID, LOB0) VALUES (1, 'LOB0-1-0');
        // SAVEPOINT s1;
        // DECLARE
        // loc CLOB;
        // BEGIN
        // SELECT LOB0 INTO loc FROM DBZ1960_26 WHERE ID = 1 FOR UPDATE;
        // DBMS_LOB.WRITE(loc, 1985, 1, RPAD('LOB0-1-', 1985, '1'));
        // END;
        // ROLLBACK TO SAVEPOINT s1;
        // Version: 26ai Free Release 23.26.2.0.0
        LogInterceptor logInterceptor = new LogInterceptor(AbstractLogMinerTransactionCache.class);
        LogMinerEvent[] events = new LogMinerEvent[]{
                event(EventType.INSERT, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.UPDATE, 0, "BBBBBBBBBBBBBBBBBB", "2"),
                event(EventType.SELECT_LOB_LOCATOR, 0, "BBBBBBBBBBBBBBBBBB", "3"),
                event(EventType.LOB_WRITE, 0, "BBBBBBBBBBBBBBBBBB", "4"),
                // event(EventType.INTERNAL, 0, "BBBBBBBBBBBBBBBBBB", "5"),
                event(EventType.UPDATE, 1, "BBBBBBBBBBBBBBBBBB", "6"), };
        LogMinerEvent[] expected = new LogMinerEvent[]{
                event(EventType.INSERT, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.UPDATE, 0, "BBBBBBBBBBBBBBBBBB", "2"),
                event(EventType.UPDATE, 1, "BBBBBBBBBBBBBBBBBB", "6"), };
        assertThat(cache(events)).isEqualTo(expected);
        assertThat(logInterceptor.containsWarnMessage("Manual investigation is required")).isFalse();
        assertThat(logInterceptor.containsWarnMessage("Please enable 'log.mining.include.internal.events'")).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void testRollbackLobTrimWithoutInternal() throws Exception {
        // CREATE TABLE DBZ1960_27(ID NUMERIC(9,0) PRIMARY KEY, LOB0 CLOB);
        // INSERT INTO DBZ1960_27 (ID, LOB0) VALUES (1, 'LOB0-1-00');
        // SAVEPOINT s1;
        // DECLARE
        // loc CLOB;
        // BEGIN
        // SELECT LOB0 INTO loc FROM DBZ1960_27 WHERE ID = 1 FOR UPDATE;
        // DBMS_LOB.TRIM(loc, 8);
        // END;
        // ROLLBACK TO SAVEPOINT s1;
        // Version: 26ai Free Release 23.26.2.0.0
        LogInterceptor logInterceptor = new LogInterceptor(AbstractLogMinerTransactionCache.class);
        LogMinerEvent[] events = new LogMinerEvent[]{
                event(EventType.INSERT, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.UPDATE, 0, "BBBBBBBBBBBBBBBBBB", "2"),
                event(EventType.SELECT_LOB_LOCATOR, 0, "BBBBBBBBBBBBBBBBBB", "3"),
                // event(EventType.LOB_TRIM, 0, "AAAAAAAAAAAAAAAAAA", "4"),
                // event(EventType.INTERNAL, 0, "BBBBBBBBBBBBBBBBBB", "5"),
                event(EventType.UPDATE, 1, "BBBBBBBBBBBBBBBBBB", "6"), };
        LogMinerEvent[] expected = new LogMinerEvent[]{
                event(EventType.INSERT, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.UPDATE, 0, "BBBBBBBBBBBBBBBBBB", "2"),
                event(EventType.UPDATE, 1, "BBBBBBBBBBBBBBBBBB", "6"), };
        assertThat(cache(events)).isEqualTo(expected);
        assertThat(logInterceptor.containsWarnMessage("Manual investigation is required")).isFalse();
        assertThat(logInterceptor.containsWarnMessage("Please enable 'log.mining.include.internal.events'")).isTrue();
    }

    // Not supported without INTERNAL: silently remove valid events because the event sequence is ambiguous

    @Test
    @FixFor("debezium/dbz#1960")
    public void testUpdateEmptyWithoutInternalAndRollbackUpdateScalar() throws Exception {
        // CREATE TABLE DBZ1960_28(ID NUMERIC(9,0) PRIMARY KEY, STR0 VARCHAR2(50), LOB0 CLOB);
        // INSERT INTO DBZ1960_28 (ID, STR0) VALUES (1, 'STR0-1-0');
        //
        // With INTERNAL:
        // UPDATE DBZ1960_28 SET STR0 = 'STR0-1-1', LOB0 = EMPTY_CLOB() WHERE ID = 1;
        // SAVEPOINT s1;
        // UPDATE DBZ1960_28 SET STR0 = 'STR0-1-2' WHERE ID = 1;
        // ROLLBACK TO SAVEPOINT s1;
        //
        // Without INTERNAL:
        // SAVEPOINT s1;
        // UPDATE DBZ1960_28 SET STR0 = 'STR0-1-1', LOB0 = 'LOB0-1-1' WHERE ID = 1;
        // ROLLBACK TO SAVEPOINT s1;
        // Version: 26ai Free Release 23.26.2.0.0
        LogInterceptor logInterceptor = new LogInterceptor(AbstractLogMinerTransactionCache.class);
        LogMinerEvent[] events = new LogMinerEvent[]{
                event(EventType.UPDATE, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                // event(EventType.INTERNAL, 0, "BBBBBBBBBBBBBBBBBB", "2"),
                event(EventType.UPDATE, 0, "BBBBBBBBBBBBBBBBBB", "3"),
                event(EventType.UPDATE, 1, "BBBBBBBBBBBBBBBBBB", "4"), };
        LogMinerEvent[] expected = new LogMinerEvent[]{
                // event(EventType.UPDATE, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.UPDATE, 1, "BBBBBBBBBBBBBBBBBB", "4"), };
        assertThat(cache(events)).isEqualTo(expected);
        assertThat(logInterceptor.containsWarnMessage("Manual investigation is required")).isFalse();
        assertThat(logInterceptor.containsWarnMessage("Please enable 'log.mining.include.internal.events'")).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void testUpdateOutOfLineWithoutInternalAndRollbackUpdateScalar() throws Exception {
        // CREATE TABLE DBZ1960_29(ID NUMERIC(9,0) PRIMARY KEY, STR0 VARCHAR2(50), LOB0 CLOB, EXT0 VARCHAR2(8000));
        // INSERT INTO DBZ1960_29 (ID, STR0, LOB0) VALUES (1, 'STR0-1-0', EMPTY_CLOB());
        //
        // With INTERNAL:
        // UPDATE DBZ1960_29 SET EXT0 = RPAD('EXT0-1-', 4000, '1') WHERE ID = 1;
        // SAVEPOINT s1;
        // UPDATE DBZ1960_29 SET STR0 = 'STR0-1-2' WHERE ID = 1;
        // ROLLBACK TO SAVEPOINT s1;
        //
        // Without INTERNAL:
        // SAVEPOINT s1;
        // UPDATE DBZ1960_29 SET LOB0 = 'LOB0-1-1', EXT0 = RPAD('EXT0-1-', 4000, '1') WHERE ID = 1;
        // ROLLBACK TO SAVEPOINT s1;
        // Version: 26ai Free Release 23.26.2.0.0
        LogInterceptor logInterceptor = new LogInterceptor(AbstractLogMinerTransactionCache.class);
        LogMinerEvent[] events = new LogMinerEvent[]{
                event(EventType.EXTENDED_STRING_BEGIN, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.EXTENDED_STRING_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "2"),
                // event(EventType.INTERNAL, 0, "BBBBBBBBBBBBBBBBBB", "3"),
                event(EventType.UPDATE, 0, "BBBBBBBBBBBBBBBBBB", "4"),
                event(EventType.UPDATE, 1, "BBBBBBBBBBBBBBBBBB", "5"), };
        LogMinerEvent[] expected = new LogMinerEvent[]{
                // event(EventType.EXTENDED_STRING_BEGIN, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                // event(EventType.EXTENDED_STRING_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "2"),
                event(EventType.UPDATE, 1, "BBBBBBBBBBBBBBBBBB", "5"), };
        assertThat(cache(events)).isEqualTo(expected);
        assertThat(logInterceptor.containsWarnMessage("Manual investigation is required")).isFalse();
        assertThat(logInterceptor.containsWarnMessage("Please enable 'log.mining.include.internal.events'")).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void testUpdateEmptyWithoutInternalAndRollbackUpdateInlineAndXml() throws Exception {
        // CREATE TABLE DBZ1960_30(ID NUMERIC(9,0) PRIMARY KEY, STR0 VARCHAR2(50), LOB0 CLOB, XML0 XMLTYPE);
        // INSERT INTO DBZ1960_30 (ID, STR0, XML0) VALUES (1, 'STR0-1-0', XMLTYPE('<XML0><ID>1</ID><V>0</V></XML0>'));
        //
        // With INTERNAL:
        // UPDATE DBZ1960_30 SET STR0 = 'STR0-1-1', LOB0 = EMPTY_CLOB() WHERE ID = 1;
        // SAVEPOINT s1;
        // UPDATE DBZ1960_30 SET LOB0 = 'LOB0-1-2', XML0 = XMLTYPE('<XML0><ID>1</ID><V>2</V></XML0>') WHERE ID = 1;
        // ROLLBACK TO SAVEPOINT s1;
        //
        // Without INTERNAL:
        // SAVEPOINT s1;
        // UPDATE DBZ1960_30 SET STR0 = 'STR0-1-1', LOB0 = 'LOB0-1-1', XML0 = XMLTYPE('<XML0><ID>1</ID><V>1</V></XML0>') WHERE ID = 1;
        // ROLLBACK TO SAVEPOINT s1;
        // Version: 26ai Free Release 23.26.2.0.0
        LogInterceptor logInterceptor = new LogInterceptor(AbstractLogMinerTransactionCache.class);
        LogMinerEvent[] events = new LogMinerEvent[]{
                event(EventType.UPDATE, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                // event(EventType.INTERNAL, 0, "BBBBBBBBBBBBBBBBBB", "2"),
                event(EventType.XML_BEGIN, 0, "AAAAAAAAAAAAAAAAAA", "3"),
                event(EventType.XML_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "3"),
                event(EventType.XML_END, 0, "AAAAAAAAAAAAAAAAAA", "3"),
                event(EventType.UPDATE, 0, "BBBBBBBBBBBBBBBBBB", "4"),
                event(EventType.UPDATE, 1, "BBBBBBBBBBBBBBBBBB", "5"), };
        LogMinerEvent[] expected = new LogMinerEvent[]{
                // event(EventType.UPDATE, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.UPDATE, 1, "BBBBBBBBBBBBBBBBBB", "5"), };
        assertThat(cache(events)).isEqualTo(expected);
        assertThat(logInterceptor.containsWarnMessage("Manual investigation is required")).isFalse();
        assertThat(logInterceptor.containsWarnMessage("Please enable 'log.mining.include.internal.events'")).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void testUpdateXmlWithoutInternalAndRollbackUpdateInlineAndXml() throws Exception {
        // CREATE TABLE DBZ1960_31(ID NUMERIC(9,0) PRIMARY KEY, XML0 XMLTYPE, XML1 XMLTYPE, LOB0 CLOB);
        // INSERT INTO DBZ1960_31 (ID, XML0, XML1, LOB0) VALUES (1, XMLTYPE('<XML0><ID>1</ID><V>0</V></XML0>'), XMLTYPE('<XML1><ID>1</ID><V>0</V></XML1>'), 'LOB0-1-0');
        //
        // With INTERNAL:
        // UPDATE DBZ1960_31 SET XML0 = XMLTYPE('<XML0><ID>1</ID><V>1</V></XML0>') WHERE ID = 1;
        // SAVEPOINT s1;
        // UPDATE DBZ1960_31 SET XML1 = XMLTYPE('<XML1><ID>1</ID><V>2</V></XML1>'), LOB0 = 'LOB0-1-2' WHERE ID = 1;
        // ROLLBACK TO SAVEPOINT s1;
        //
        // Without INTERNAL:
        // SAVEPOINT s1;
        // UPDATE DBZ1960_31 SET XML0 = XMLTYPE('<XML0><ID>1</ID><V>1</V></XML0>'), XML1 = XMLTYPE('<XML1><ID>1</ID><V>1</V></XML1>'), LOB0 = 'LOB0-1-1' WHERE ID = 1;
        // ROLLBACK TO SAVEPOINT s1;
        // Version: 26ai Free Release 23.26.2.0.0
        LogInterceptor logInterceptor = new LogInterceptor(AbstractLogMinerTransactionCache.class);
        LogMinerEvent[] events = new LogMinerEvent[]{
                event(EventType.XML_BEGIN, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.XML_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.XML_END, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                // event(EventType.INTERNAL, 0, "BBBBBBBBBBBBBBBBBB", "2"),
                event(EventType.XML_BEGIN, 0, "AAAAAAAAAAAAAAAAAA", "3"),
                event(EventType.XML_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "3"),
                event(EventType.XML_END, 0, "AAAAAAAAAAAAAAAAAA", "3"),
                event(EventType.UPDATE, 0, "BBBBBBBBBBBBBBBBBB", "4"),
                event(EventType.UPDATE, 1, "BBBBBBBBBBBBBBBBBB", "5"), };
        LogMinerEvent[] expected = new LogMinerEvent[]{
                // event(EventType.XML_BEGIN, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                // event(EventType.XML_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                // event(EventType.XML_END, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.UPDATE, 1, "BBBBBBBBBBBBBBBBBB", "5"), };
        assertThat(cache(events)).isEqualTo(expected);
        assertThat(logInterceptor.containsWarnMessage("Manual investigation is required")).isFalse();
        assertThat(logInterceptor.containsWarnMessage("Please enable 'log.mining.include.internal.events'")).isFalse();
    }

    // Manual investigation is required

    @Test
    @FixFor("debezium/dbz#1960")
    public void testRollbackInsertScalarWithUnexpectedOperation() throws Exception {
        LogInterceptor logInterceptor = new LogInterceptor(AbstractLogMinerTransactionCache.class);
        LogMinerEvent[] events = new LogMinerEvent[]{
                event(EventType.DELETE, 0, "BBBBBBBBBBBBBBBBBB", "1"),
                event(EventType.INSERT, 0, "BBBBBBBBBBBBBBBBBB", "2"),
                event(EventType.INSERT, 1, "BBBBBBBBBBBBBBBBBB", "3"), };
        LogMinerEvent[] expected = new LogMinerEvent[]{
                event(EventType.DELETE, 0, "BBBBBBBBBBBBBBBBBB", "1"),
                event(EventType.INSERT, 0, "BBBBBBBBBBBBBBBBBB", "2"),
                event(EventType.INSERT, 1, "BBBBBBBBBBBBBBBBBB", "3"), };
        assertThat(cache(events)).isEqualTo(expected);
        assertThat(logInterceptor.containsWarnMessage("Manual investigation is required")).isTrue();
        assertThat(logInterceptor.containsWarnMessage("Please enable 'log.mining.include.internal.events'")).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void testRollbackInsertEmptyWithUnexpectedOperation() throws Exception {
        LogInterceptor logInterceptor = new LogInterceptor(AbstractLogMinerTransactionCache.class);
        LogMinerEvent[] events = new LogMinerEvent[]{
                event(EventType.DELETE, 0, "BBBBBBBBBBBBBBBBBB", "1"),
                event(EventType.INSERT, 0, "AAAAAAAAAAAAAAAAAA", "2"),
                // event(EventType.INTERNAL, 0, "BBBBBBBBBBBBBBBBBB", "3"),
                event(EventType.INSERT, 1, "BBBBBBBBBBBBBBBBBB", "4"), };
        LogMinerEvent[] expected = new LogMinerEvent[]{
                event(EventType.DELETE, 0, "BBBBBBBBBBBBBBBBBB", "1"),
                event(EventType.INSERT, 0, "AAAAAAAAAAAAAAAAAA", "2"),
                event(EventType.INSERT, 1, "BBBBBBBBBBBBBBBBBB", "4"), };
        assertThat(cache(events)).isEqualTo(expected);
        assertThat(logInterceptor.containsWarnMessage("Manual investigation is required")).isTrue();
        assertThat(logInterceptor.containsWarnMessage("Please enable 'log.mining.include.internal.events'")).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void testRollbackUpdateScalarWithUnexpectedOperation() throws Exception {
        LogInterceptor logInterceptor = new LogInterceptor(AbstractLogMinerTransactionCache.class);
        LogMinerEvent[] events = new LogMinerEvent[]{
                event(EventType.INSERT, 0, "BBBBBBBBBBBBBBBBBB", "1"),
                event(EventType.UPDATE, 0, "BBBBBBBBBBBBBBBBBB", "2"),
                event(EventType.DELETE, 1, "BBBBBBBBBBBBBBBBBB", "3"), };
        LogMinerEvent[] expected = new LogMinerEvent[]{
                event(EventType.INSERT, 0, "BBBBBBBBBBBBBBBBBB", "1"),
                event(EventType.UPDATE, 0, "BBBBBBBBBBBBBBBBBB", "2"),
                event(EventType.DELETE, 1, "BBBBBBBBBBBBBBBBBB", "3"), };
        assertThat(cache(events)).isEqualTo(expected);
        assertThat(logInterceptor.containsWarnMessage("Manual investigation is required")).isTrue();
        assertThat(logInterceptor.containsWarnMessage("Please enable 'log.mining.include.internal.events'")).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void testRollbackUpdateEmptyWithUnexpectedOperation() throws Exception {
        LogInterceptor logInterceptor = new LogInterceptor(AbstractLogMinerTransactionCache.class);
        LogMinerEvent[] events = new LogMinerEvent[]{
                event(EventType.DELETE, 0, "BBBBBBBBBBBBBBBBBB", "1"),
                event(EventType.UPDATE, 0, "AAAAAAAAAAAAAAAAAA", "2"),
                // event(EventType.INTERNAL, 0, "BBBBBBBBBBBBBBBBBB", "3"),
                event(EventType.INSERT, 1, "BBBBBBBBBBBBBBBBBB", "4"), };
        LogMinerEvent[] expected = new LogMinerEvent[]{
                event(EventType.DELETE, 0, "BBBBBBBBBBBBBBBBBB", "1"),
                event(EventType.UPDATE, 0, "AAAAAAAAAAAAAAAAAA", "2"),
                event(EventType.INSERT, 1, "BBBBBBBBBBBBBBBBBB", "4"), };
        assertThat(cache(events)).isEqualTo(expected);
        assertThat(logInterceptor.containsWarnMessage("Manual investigation is required")).isTrue();
        assertThat(logInterceptor.containsWarnMessage("Please enable 'log.mining.include.internal.events'")).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void testRollbackUpdateInlineWithUnexpectedOperation() throws Exception {
        LogInterceptor logInterceptor = new LogInterceptor(AbstractLogMinerTransactionCache.class);
        LogMinerEvent[] events = new LogMinerEvent[]{
                event(EventType.UPDATE, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.UPDATE, 0, "BBBBBBBBBBBBBBBBBB", "2"),
                event(EventType.DELETE, 1, "BBBBBBBBBBBBBBBBBB", "3"), };
        LogMinerEvent[] expected = new LogMinerEvent[]{
                event(EventType.UPDATE, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.UPDATE, 0, "BBBBBBBBBBBBBBBBBB", "2"),
                event(EventType.DELETE, 1, "BBBBBBBBBBBBBBBBBB", "3"), };
        assertThat(cache(events)).isEqualTo(expected);
        assertThat(logInterceptor.containsWarnMessage("Manual investigation is required")).isTrue();
        assertThat(logInterceptor.containsWarnMessage("Please enable 'log.mining.include.internal.events'")).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void testRollbackUpdateOutOfLineWithUnexpectedOperation() throws Exception {
        LogInterceptor logInterceptor = new LogInterceptor(AbstractLogMinerTransactionCache.class);
        LogMinerEvent[] events = new LogMinerEvent[]{
                event(EventType.SELECT_LOB_LOCATOR, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.LOB_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "2"),
                event(EventType.INTERNAL, 0, "BBBBBBBBBBBBBBBBBB", "3"),
                event(EventType.DELETE, 1, "BBBBBBBBBBBBBBBBBB", "4"), };
        LogMinerEvent[] expected = new LogMinerEvent[]{
                event(EventType.SELECT_LOB_LOCATOR, 0, "AAAAAAAAAAAAAAAAAA", "1"),
                event(EventType.LOB_WRITE, 0, "AAAAAAAAAAAAAAAAAA", "2"),
                event(EventType.INTERNAL, 0, "BBBBBBBBBBBBBBBBBB", "3"),
                event(EventType.DELETE, 1, "BBBBBBBBBBBBBBBBBB", "4"), };
        assertThat(cache(events)).isEqualTo(expected);
        assertThat(logInterceptor.containsWarnMessage("Manual investigation is required")).isTrue();
        assertThat(logInterceptor.containsWarnMessage("Please enable 'log.mining.include.internal.events'")).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void testRollbackInsertScalarWithUnexpectedRowId() throws Exception {
        LogInterceptor logInterceptor = new LogInterceptor(AbstractLogMinerTransactionCache.class);
        LogMinerEvent[] events = new LogMinerEvent[]{
                event(EventType.DELETE, 0, "BBBBBBBBBBBBBBBBBB", "1"),
                event(EventType.INSERT, 0, "CCCCCCCCCCCCCCCCCC", "2"),
                event(EventType.INSERT, 1, "BBBBBBBBBBBBBBBBBB", "3"), };
        LogMinerEvent[] expected = new LogMinerEvent[]{
                event(EventType.DELETE, 0, "BBBBBBBBBBBBBBBBBB", "1"),
                event(EventType.INSERT, 0, "CCCCCCCCCCCCCCCCCC", "2"),
                event(EventType.INSERT, 1, "BBBBBBBBBBBBBBBBBB", "3"), };
        assertThat(cache(events)).isEqualTo(expected);
        assertThat(logInterceptor.containsWarnMessage("Manual investigation is required")).isTrue();
        assertThat(logInterceptor.containsWarnMessage("Please enable 'log.mining.include.internal.events'")).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void testRollbackInsertScalarWithUnexpectedTable() throws Exception {
        LogInterceptor logInterceptor = new LogInterceptor(AbstractLogMinerTransactionCache.class);
        // The event immediately preceding the undo belongs to another table, so the undo cannot be attributed
        LogMinerEvent[] events = new LogMinerEvent[]{
                event(EventType.INSERT, 0, "BBBBBBBBBBBBBBBBBB", "1"),
                event(EventType.INSERT, 0, "CCCCCCCCCCCCCCCCCC", "2", OTHER_TABLE),
                event(EventType.DELETE, 1, "BBBBBBBBBBBBBBBBBB", "3"), };
        LogMinerEvent[] expected = new LogMinerEvent[]{
                event(EventType.INSERT, 0, "BBBBBBBBBBBBBBBBBB", "1"),
                event(EventType.INSERT, 0, "CCCCCCCCCCCCCCCCCC", "2", OTHER_TABLE),
                event(EventType.DELETE, 1, "BBBBBBBBBBBBBBBBBB", "3"), };
        assertThat(cache(events)).isEqualTo(expected);
        assertThat(logInterceptor.containsWarnMessage("has a different TABLE_NAME '" + OTHER_TABLE + "'")).isTrue();
        assertThat(logInterceptor.containsWarnMessage("Manual investigation is required")).isTrue();
        assertThat(logInterceptor.containsWarnMessage("Please enable 'log.mining.include.internal.events'")).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#1960")
    public void testRollbackUpdateOutOfLineWithUnexpectedTable() throws Exception {
        LogInterceptor logInterceptor = new LogInterceptor(AbstractLogMinerTransactionCache.class);
        // The INTERNAL event is matched, but the empty-ROW_ID event before it belongs to another table. Without the
        // guard it would be taken as the inline part of the same statement and removed along with the INTERNAL event.
        LogMinerEvent[] events = new LogMinerEvent[]{
                event(EventType.UPDATE, 0, "AAAAAAAAAAAAAAAAAA", "1", OTHER_TABLE),
                event(EventType.INTERNAL, 0, "BBBBBBBBBBBBBBBBBB", "2"),
                event(EventType.UPDATE, 1, "BBBBBBBBBBBBBBBBBB", "3"), };
        LogMinerEvent[] expected = new LogMinerEvent[]{
                event(EventType.UPDATE, 0, "AAAAAAAAAAAAAAAAAA", "1", OTHER_TABLE),
                event(EventType.UPDATE, 1, "BBBBBBBBBBBBBBBBBB", "3"), };
        assertThat(cache(events)).isEqualTo(expected);
        assertThat(logInterceptor.containsWarnMessage("unexpected TABLE_NAME '" + OTHER_TABLE + "'")).isTrue();
        assertThat(logInterceptor.containsWarnMessage("Please enable 'log.mining.include.internal.events'")).isTrue();
        assertThat(logInterceptor.containsWarnMessage("Manual investigation is required")).isFalse();
    }

    private LogMinerEvent event(EventType eventType, int rollback, String rowId, String rsId) {
        return event(eventType, rollback, rowId, rsId, TABLE);
    }

    private LogMinerEvent event(EventType eventType, int rollback, String rowId, String rsId, TableId tableId) {
        return rollback == 0 ? new LogMinerEvent(eventType, Scn.ONE, tableId, rowId, rsId, CHANGE_TIME)
                : new RollbackToSavepointEvent(eventType, Scn.ONE, tableId, rowId, rsId, CHANGE_TIME);
    }

    private LogMinerEvent[] cache(LogMinerEvent[] events) throws InterruptedException {
        LogMinerTransactionCache<T> cache = cacheProvider.getTransactionCache();
        T transaction = transactionFactory.createTransaction("1", Scn.ONE, CHANGE_TIME, "userName", 1, "clientId");
        cache.addTransaction(transaction);
        for (LogMinerEvent event : events) {
            cache.addTransactionEvent(transaction, transaction.getNextEventId(), event);
        }
        List<LogMinerEvent> result = new ArrayList<>(transaction.getNumberOfEvents());
        cache.forEachEvent(transaction, result::add);
        return result.toArray(new LogMinerEvent[result.size()]);
    }

    protected abstract CacheProvider<T> getCacheProvider();

    protected abstract TransactionFactory<T> getTransactionFactory();
}
