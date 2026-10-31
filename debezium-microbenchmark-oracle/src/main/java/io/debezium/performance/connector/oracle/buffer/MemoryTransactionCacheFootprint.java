/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.performance.connector.oracle.buffer;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.time.Instant;
import java.util.Arrays;
import java.util.Locale;
import java.util.Random;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.buffered.LogMinerTransactionCache;
import io.debezium.connector.oracle.logminer.buffered.memory.MemoryCacheProvider;
import io.debezium.connector.oracle.logminer.buffered.memory.MemoryTransaction;
import io.debezium.connector.oracle.logminer.buffered.memory.MemoryTransactionFactory;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.LogMinerRedoEvent;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlParser;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;

/**
 * Measures the heap retained per event by the memory transaction buffer.
 * <p>
 * The workload is deterministic: {@code transactions} transactions of {@code events} rows each, cycling through
 * INSERT, UPDATE and DELETE redo statements against a table with {@code columns} columns, every fifth column
 * {@code NULL}. Each statement is stored as a {@link LogMinerRedoEvent}, the row's metadata plus its redo SQL,
 * the way the buffered connector stores it until the transaction commits. The heap in use is sampled after
 * forced garbage collections before and after the buffer is filled, over {@code rounds} rounds. The lowest
 * empty-heap sample of the run is the baseline, and the median bytes per event is the figure to compare between
 * builds. Run both builds with the same arguments and JVM settings:
 *
 * <pre>
 * java -Xms2g -Xmx2g -XX:+UseSerialGC -cp target/debezium-microbenchmark-oracle.jar \
 *     io.debezium.performance.connector.oracle.buffer.MemoryTransactionCacheFootprint \
 *     --transactions 100 --events 1000 --columns 10 --rounds 5 --compression true
 * </pre>
 *
 * The average redo statement length is reported too, since a build that stores the statement text instead of
 * the parsed values should be read against it. The time {@link LogMinerDmlParser} needs per statement is measured
 * last, over the same statements: with commit-time processing it is the parsing cost a commit pays per row.
 *
 * @author Chris Cranford
 */
public class MemoryTransactionCacheFootprint {

    private static final Instant CHANGE_TIME = Instant.parse("2026-10-24T12:00:00Z");
    private static final TableId TABLE_ID = TableId.parse("ORCLPDB1.DEBEZIUM.CUSTOMERS");
    private static final String CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz";
    private static final int VALUE_LENGTH = 12;
    private static final long SEED = 42L;

    private final int transactions;
    private final int eventsPerTransaction;
    private final int columns;
    private final int rounds;
    private final boolean compression;
    private final Table table;
    private final LogMinerDmlParser parser;

    private long statementChars;

    public static void main(String[] args) throws Exception {
        int transactions = 100;
        int events = 1000;
        int columns = 10;
        int rounds = 5;
        boolean compression = true;
        for (int i = 0; i + 1 < args.length; i += 2) {
            switch (args[i]) {
                case "--transactions" -> transactions = Integer.parseInt(args[i + 1]);
                case "--events" -> events = Integer.parseInt(args[i + 1]);
                case "--columns" -> columns = Integer.parseInt(args[i + 1]);
                case "--rounds" -> rounds = Integer.parseInt(args[i + 1]);
                case "--compression" -> compression = Boolean.parseBoolean(args[i + 1]);
                default -> throw new IllegalArgumentException("Unknown argument: " + args[i]);
            }
        }
        new MemoryTransactionCacheFootprint(transactions, events, columns, rounds, compression).run();
    }

    public MemoryTransactionCacheFootprint(int transactions, int eventsPerTransaction, int columns, int rounds, boolean compression) {
        this.transactions = transactions;
        this.eventsPerTransaction = eventsPerTransaction;
        this.columns = columns;
        this.rounds = rounds;
        this.compression = compression;
        this.table = createTable();
        this.parser = new LogMinerDmlParser(new OracleConnectorConfig(Configuration.empty()));
    }

    private void run() throws Exception {
        final long totalEvents = (long) transactions * eventsPerTransaction;
        final long[] before = new long[rounds];
        final long[] after = new long[rounds];

        MemoryCacheProvider provider = null;
        for (int round = 0; round < rounds; round++) {
            before[round] = usedHeapAfterGc();
            provider = fill();
            after[round] = usedHeapAfterGc();
            print(String.format(Locale.ROOT, "round %d: heap before=%,d after=%,d", round + 1, before[round], after[round]));

            // Drop the reference before the next round's baseline sample so the previous buffer is collected
            provider.close();
            provider = null;
        }

        // The filled heap is the same every round; the empty baseline occasionally keeps a few megabytes of
        // garbage the forced collections did not reclaim yet, so the lowest baseline of the run is the floor.
        final long baseline = Arrays.stream(before).min().orElseThrow();
        final long[] bytesPerEvent = new long[rounds];
        for (int round = 0; round < rounds; round++) {
            bytesPerEvent[round] = (after[round] - baseline) / totalEvents;
        }

        Arrays.sort(bytesPerEvent);
        print(String.format(Locale.ROOT,
                "footprint transactions=%d events=%d columns=%d rounds=%d compression=%s baseline=%d statementChars/event=%d bytes/event min=%d median=%d max=%d",
                transactions, eventsPerTransaction, columns, rounds, compression, baseline, statementChars / totalEvents,
                bytesPerEvent[0], bytesPerEvent[rounds / 2], bytesPerEvent[rounds - 1]));

        measureParseTime(totalEvents);
    }

    /**
     * Parses every statement of the workload once, after a warm-up pass, and reports the time per statement.
     */
    private void measureParseTime(long totalEvents) {
        parseAll();
        final long start = System.nanoTime();
        parseAll();
        final long elapsedNanos = System.nanoTime() - start;
        print(String.format(Locale.ROOT, "parse statements=%d elapsedMs=%d micros/statement=%.2f",
                totalEvents, elapsedNanos / 1_000_000, elapsedNanos / 1_000.0 / totalEvents));
    }

    private void parseAll() {
        final Random random = new Random(SEED);
        for (int t = 0; t < transactions; t++) {
            for (int e = 0; e < eventsPerTransaction; e++) {
                parser.parse(statement(e % 3, (long) t * eventsPerTransaction + e, random), table);
            }
        }
    }

    private MemoryCacheProvider fill() {
        final MemoryCacheProvider provider = new MemoryCacheProvider(new OracleConnectorConfig(Configuration.create()
                .with(OracleConnectorConfig.LOG_MINING_BUFFER_MEMORY_COMPRESSION, compression)
                .build()));
        final LogMinerTransactionCache<MemoryTransaction> cache = provider.getTransactionCache();
        final MemoryTransactionFactory factory = new MemoryTransactionFactory();
        final Random random = new Random(SEED);

        statementChars = 0;
        long scn = 8_800_000L;
        for (int t = 0; t < transactions; t++) {
            final String transactionId = String.format(Locale.ROOT, "%016x", t + 1);
            final MemoryTransaction transaction = factory.createTransaction(transactionId, Scn.valueOf(scn), CHANGE_TIME, "DEBEZIUM", 1, null);
            cache.addTransaction(transaction);

            for (int e = 0; e < eventsPerTransaction; e++) {
                final long rowNumber = (long) t * eventsPerTransaction + e;
                final String sql = statement(e % 3, rowNumber, random);
                statementChars += sql.length();

                final LogMinerRedoEvent event = new LogMinerRedoEvent(eventType(e % 3), false, Scn.valueOf(++scn), TABLE_ID, rowId(rowNumber),
                        rsId(rowNumber), CHANGE_TIME, sql, 0, null, 0L, 70_123L, 70_124L);
                cache.addTransactionEvent(transaction, transaction.getNextEventId(), event);
            }
        }
        return provider;
    }

    private Table createTable() {
        final TableEditor editor = Table.editor()
                .tableId(TABLE_ID)
                .addColumn(Column.editor().name("ID").create());
        for (int i = 0; i < columns; ++i) {
            editor.addColumn(Column.editor().name("COL" + i).create());
        }
        return editor.create();
    }

    private static EventType eventType(int kind) {
        return switch (kind) {
            case 0 -> EventType.INSERT;
            case 1 -> EventType.UPDATE;
            default -> EventType.DELETE;
        };
    }

    private String statement(int kind, long rowNumber, Random random) {
        return switch (kind) {
            case 0 -> insertStatement(rowNumber, random);
            case 1 -> updateStatement(rowNumber, random);
            default -> deleteStatement(rowNumber, random);
        };
    }

    private String insertStatement(long rowNumber, Random random) {
        final StringBuilder sb = new StringBuilder("insert into \"DEBEZIUM\".\"CUSTOMERS\"(\"ID\"");
        for (int i = 0; i < columns; ++i) {
            sb.append(",\"COL").append(i).append('"');
        }
        sb.append(") values ('").append(rowNumber).append('\'');
        for (int i = 0; i < columns; ++i) {
            sb.append(',').append(value(i, random));
        }
        return sb.append(");").toString();
    }

    private String updateStatement(long rowNumber, Random random) {
        final StringBuilder sb = new StringBuilder("update \"DEBEZIUM\".\"CUSTOMERS\" set ");
        for (int i = 0; i < columns; ++i) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append("\"COL").append(i).append("\" = ").append(value(i, random));
        }
        sb.append(" where \"ID\" = '").append(rowNumber).append('\'');
        appendWhereColumns(sb, random);
        return sb.append(';').toString();
    }

    private String deleteStatement(long rowNumber, Random random) {
        final StringBuilder sb = new StringBuilder("delete from \"DEBEZIUM\".\"CUSTOMERS\" where \"ID\" = '").append(rowNumber).append('\'');
        appendWhereColumns(sb, random);
        return sb.append(';').toString();
    }

    private void appendWhereColumns(StringBuilder sb, Random random) {
        for (int i = 0; i < columns; ++i) {
            sb.append(" and \"COL").append(i).append('"');
            if (isNullColumn(i)) {
                sb.append(" IS NULL");
            }
            else {
                sb.append(" = ").append(value(i, random));
            }
        }
    }

    private String value(int column, Random random) {
        if (isNullColumn(column)) {
            return "NULL";
        }
        final StringBuilder sb = new StringBuilder(VALUE_LENGTH + 2).append('\'');
        for (int i = 0; i < VALUE_LENGTH; ++i) {
            sb.append(CHARS.charAt(random.nextInt(CHARS.length())));
        }
        return sb.append('\'').toString();
    }

    private static void print(String line) {
        // A command line tool; the shaded jar has no logging backend
        // CHECKSTYLE:OFF
        System.out.println(line);
        // CHECKSTYLE:ON
    }

    private static boolean isNullColumn(int column) {
        return column % 5 == 4;
    }

    private static String rowId(long rowNumber) {
        return String.format(Locale.ROOT, "AAAVn/AAEAAA%06d", rowNumber % 1_000_000);
    }

    private static String rsId(long rowNumber) {
        return String.format(Locale.ROOT, "0x%06x.%08x.%04x", 19 + rowNumber / 1000, rowNumber % 100_000, (rowNumber % 16) * 16);
    }

    /**
     * Heap in use right after a full collection, summed over the heap pools from their collection usage, which
     * excludes space that threads have reserved but not filled since the collection.
     */
    private static long usedHeapAfterGc() throws InterruptedException {
        for (int i = 0; i < 3; i++) {
            System.gc();
            Thread.sleep(100);
        }
        long used = 0;
        for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
            if (pool.getType() == MemoryType.HEAP && pool.getCollectionUsage() != null) {
                used += pool.getCollectionUsage().getUsed();
            }
        }
        return used;
    }
}
