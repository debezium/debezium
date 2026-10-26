/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import static io.debezium.connector.oracle.logminer.buffered.LogMinerRedoEventFixtures.CHANGE_TIME;
import static io.debezium.connector.oracle.logminer.buffered.LogMinerRedoEventFixtures.ROW_ID;
import static io.debezium.connector.oracle.logminer.buffered.LogMinerRedoEventFixtures.RS_ID;
import static io.debezium.connector.oracle.logminer.buffered.LogMinerRedoEventFixtures.SCN;
import static io.debezium.connector.oracle.logminer.buffered.LogMinerRedoEventFixtures.TABLE_ID;

import java.util.ArrayList;
import java.util.List;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.buffered.memory.MemoryCacheProvider;
import io.debezium.connector.oracle.logminer.buffered.memory.MemoryTransaction;
import io.debezium.connector.oracle.logminer.buffered.memory.MemoryTransactionFactory;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.LogMinerRedoEvent;

/**
 * @author Chris Cranford
 */
public class MemoryLogMinerRedoEventRoundTripTest extends AbstractLogMinerEventRoundTripTest<MemoryTransaction> {

    @Override
    protected CacheProvider<MemoryTransaction> getCacheProvider() {
        return new MemoryCacheProvider(new OracleConnectorConfig(Configuration.empty()));
    }

    @Override
    protected TransactionFactory<MemoryTransaction> getTransactionFactory() {
        return new MemoryTransactionFactory();
    }

    @Override
    protected List<LogMinerRedoEvent> sampleEvents() {
        // The heap buffer also keeps what the persistent buffers cannot store: a null table, a null or unpacked rs id,
        // and an SCN before the transaction's start
        final List<LogMinerRedoEvent> samples = new ArrayList<>(LogMinerRedoEventFixtures.allEvents());
        samples.add(new LogMinerRedoEvent(EventType.INTERNAL, false, SCN, null, ROW_ID, RS_ID, CHANGE_TIME, null, 0, null, 1L, 0L, 0L));
        samples.add(new LogMinerRedoEvent(EventType.INSERT, false, SCN, TABLE_ID, ROW_ID, null, CHANGE_TIME, "insert", 0, null, 0L, 1L, 1L));
        samples.add(new LogMinerRedoEvent(EventType.INSERT, false, SCN, TABLE_ID, ROW_ID, "1", CHANGE_TIME, "insert", 0, null, 0L, 1L, 1L));
        samples.add(new LogMinerRedoEvent(EventType.INSERT, false, Scn.ONE, TABLE_ID, ROW_ID, RS_ID, CHANGE_TIME, "insert", 0, null, 0L, 1L, 1L));
        return samples;
    }

    @Override
    protected void assertSameEvent(LogMinerRedoEvent expected, LogMinerRedoEvent actual) {
        LogMinerRedoEventFixtures.assertSameEvent(expected, actual);
    }
}
