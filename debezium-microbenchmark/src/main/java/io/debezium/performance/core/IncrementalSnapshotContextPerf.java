/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.performance.core;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import io.debezium.pipeline.signal.actions.snapshotting.AdditionalCondition;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.relational.TableId;

@Fork(1)
@State(Scope.Thread)
@Warmup(iterations = 2, time = 5)
@Measurement(iterations = 2, time = 5)
@OutputTimeUnit(TimeUnit.SECONDS)
@BenchmarkMode({ Mode.Throughput })
public class IncrementalSnapshotContextPerf {

    private SignalBasedIncrementalSnapshotContext<TableId> snapshotContext;

    @Param({ "1", "5", "10", "50", "100" })
    private int numberOfTableToSnapshot;

    @Setup(Level.Trial)
    public void setUp() {
        snapshotContext = new SignalBasedIncrementalSnapshotContext<>(false);
        List<String> dataCollectionIds = generateDataCollectionIds(numberOfTableToSnapshot);
        snapshotContext.addDataCollectionNamesToSnapshot("1",
                dataCollectionIds,
                dataCollectionIds.stream().map(id -> AdditionalCondition.AdditionalConditionBuilder.builder()
                        .dataCollection(Pattern.compile(id))
                        .filter("color='blue'")
                        .build()).collect(Collectors.toList()),
                "");
    }

    private List<String> generateDataCollectionIds(int number) {

        return IntStream.rangeClosed(1, number)
                .mapToObj(i -> IntStream.rangeClosed(1, number)
                        .mapToObj(j -> String.format("%s.%s", "db" + i, "table" + j))
                        .collect(Collectors.toList()))
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    @Benchmark
    public void store() {
        Map<String, Object> offset = new HashMap<>();
        snapshotContext.store(offset);
    }
}
