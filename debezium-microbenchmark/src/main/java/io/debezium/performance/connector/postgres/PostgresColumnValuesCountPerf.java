/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.performance.connector.postgres;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import io.debezium.connector.postgresql.PostgresStreamingChangeEventSource.PgConnectionSupplier;
import io.debezium.connector.postgresql.PostgresType;
import io.debezium.connector.postgresql.connection.ReplicationMessage;
import io.debezium.function.Predicates;

/**
 * Compares the non-toasted column counting used by
 * {@code PostgresChangeRecordEmitter#columnValues()} to size the values array.
 * <p>
 * The current implementation materializes a full {@code List} via
 * {@code stream().filter().collect(Collectors.toList())} only to read its
 * {@code size()}; the list itself is never iterated (the surrounding loop walks
 * the original column list). The proposed implementation replaces this with a
 * plain counting loop that allocates nothing.
 * <p>
 * {@code columnValues()} runs once per INSERT/DELETE and twice per UPDATE
 * (old + new tuple), for every captured row, so this sits on the streaming hot path.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
public class PostgresColumnValuesCountPerf {

    /** Number of columns in the replicated row. */
    @Param({ "4", "16", "64", "256" })
    private int columnCount;

    /** Percentage of columns delivered as toasted (unchanged) placeholders. */
    @Param({ "0", "25" })
    private int toastedPercent;

    private List<ReplicationMessage.Column> columns;

    @Setup(Level.Trial)
    public void setup() {
        columns = createColumns(columnCount, toastedPercent);
    }

    @Benchmark
    public int stream() {
        return streamCount(columns);
    }

    @Benchmark
    public int loop() {
        return loopCount(columns);
    }

    private static List<ReplicationMessage.Column> createColumns(int columnCount, int toastedPercent) {
        final List<ReplicationMessage.Column> result = new ArrayList<>(columnCount);
        for (int i = 0; i < columnCount; i++) {
            // Spread the toasted columns evenly so the ratio holds even for small columnCount.
            final boolean toasted = (i * 100 / columnCount) < toastedPercent;
            result.add(new DummyColumn("col_" + i, toasted));
        }
        return List.copyOf(result);
    }

    /** Current implementation: materializes a list purely to read its size. */
    private static int streamCount(List<ReplicationMessage.Column> columns) {
        List<ReplicationMessage.Column> columnsWithoutToasted = columns.stream()
                .filter(Predicates.not(ReplicationMessage.Column::isToastedColumn))
                .collect(Collectors.toList());
        return columnsWithoutToasted.size();
    }

    /** Proposed implementation: counting loop, no allocation. */
    private static int loopCount(List<ReplicationMessage.Column> columns) {
        int nonToastedCount = 0;
        for (ReplicationMessage.Column column : columns) {
            if (!column.isToastedColumn()) {
                nonToastedCount++;
            }
        }
        return nonToastedCount;
    }

    @State(Scope.Thread)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    @Fork(1)
    @Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
    public static class Sustained {

        private static final int ROWS_PER_INVOCATION = 1_000;

        @Param({ "16", "64", "256" })
        private int columnCount;

        @Param({ "0", "25" })
        private int toastedPercent;

        private List<ReplicationMessage.Column> columns;

        @Setup(Level.Trial)
        public void setup() {
            columns = createColumns(columnCount, toastedPercent);
        }

        @Benchmark
        @OperationsPerInvocation(ROWS_PER_INVOCATION)
        public void stream(Blackhole blackhole) {
            for (int i = 0; i < ROWS_PER_INVOCATION; i++) {
                blackhole.consume(streamCount(columns));
            }
        }

        @Benchmark
        @OperationsPerInvocation(ROWS_PER_INVOCATION)
        public void loop(Blackhole blackhole) {
            for (int i = 0; i < ROWS_PER_INVOCATION; i++) {
                blackhole.consume(loopCount(columns));
            }
        }
    }

    /**
     * Minimal {@link ReplicationMessage.Column} whose only benchmark-relevant
     * behavior is {@link #isToastedColumn()}.
     */
    private static final class DummyColumn implements ReplicationMessage.Column {

        private final String name;
        private final boolean toasted;

        private DummyColumn(String name, boolean toasted) {
            this.name = name;
            this.toasted = toasted;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public PostgresType getType() {
            return null;
        }

        @Override
        public ReplicationMessage.ColumnTypeMetadata getTypeMetadata() {
            return null;
        }

        @Override
        public Object getValue(PgConnectionSupplier connection, boolean includeUnknownDatatypes) {
            return null;
        }

        @Override
        public boolean isOptional() {
            return false;
        }

        @Override
        public boolean isToastedColumn() {
            return toasted;
        }
    }
}
