/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.azure.blob.history;

import java.io.BufferedWriter;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Types;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.azure.storage.blob.BlobServiceClientBuilder;

import io.debezium.config.Configuration;
import io.debezium.document.Document;
import io.debezium.document.DocumentWriter;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.relational.history.SchemaHistoryListener;
import io.debezium.relational.history.TableChanges;

/**
 * Runs the legacy and streaming readers in separate, identical Kubernetes JVMs.
 * Uses only generated data and reports heap, allocation, CPU and recovered schema.
 */
public class StreamingAzureHistoryBenchmark {

    public static void main(String[] args) throws Exception {
        final String connection = System.getenv("BENCHMARK_AZURE_CONNECTION");
        final String name = args[1];
        if ("seed".equals(args[0])) {
            seed(connection, name, Integer.parseInt(args[2]));
            return;
        }
        final boolean streaming = "streaming".equals(args[0]);
        final SchemaHistory history = streaming ? new StreamingAzureBlobSchemaHistory() : new AzureBlobSchemaHistory();
        final AtomicLong recovered = new AtomicLong();
        final SchemaHistoryListener listener = new SchemaHistoryListener() {
            @Override
            public void started() {
            }

            @Override
            public void stopped() {
            }

            @Override
            public void recoveryStarted() {
            }

            @Override
            public void recoveryStopped() {
            }

            @Override
            public void onChangeFromHistory(HistoryRecord record) {
                recovered.incrementAndGet();
            }

            @Override
            public void onChangeApplied(HistoryRecord record) {
            }
        };
        history.configure(Configuration.create()
                .with(AzureBlobSchemaHistory.ACCOUNT_CONNECTION_STRING, connection)
                .with(AzureBlobSchemaHistory.CONTAINER_NAME, "benchmark")
                .with(AzureBlobSchemaHistory.BLOB_NAME, name).build(), null, listener, true);
        final var heap = ManagementFactory.getMemoryMXBean();
        final var thread = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        final var process = (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        System.gc();
        final long initialHeap = heap.getHeapMemoryUsage().getUsed();
        final long allocatedBefore = thread.getThreadAllocatedBytes(Thread.currentThread().getId());
        final long cpuBefore = process.getProcessCpuTime();
        final long started = System.nanoTime();
        final AtomicLong peakHeap = new AtomicLong(initialHeap);
        final var sampler = java.util.concurrent.Executors.newSingleThreadScheduledExecutor();
        sampler.scheduleAtFixedRate(() -> peakHeap.accumulateAndGet(heap.getHeapMemoryUsage().getUsed(), Math::max),
                0, 10, java.util.concurrent.TimeUnit.MILLISECONDS);
        final Tables schema = new Tables();
        try {
            history.start();
            history.recover(Map.of("server", "benchmark"), Map.of("position", Long.MAX_VALUE), schema, null);
            final long recoveryNanos = System.nanoTime() - started;
            final long recoveryCpuNanos = process.getProcessCpuTime() - cpuBefore;
            final long allocated = thread.getThreadAllocatedBytes(Thread.currentThread().getId()) - allocatedBefore;
            System.gc();
            Thread.sleep(500);
            final Document result = Document.create();
            result.setString("implementation", streaming ? "streaming" : "legacy");
            result.setString("blob", name);
            result.setNumber("recoveredRecords", recovered.get());
            result.setNumber("tableCount", schema.tableIds().size());
            result.setNumber("recoveryMillis", recoveryNanos / 1_000_000);
            result.setNumber("cpuMillis", recoveryCpuNanos / 1_000_000);
            result.setNumber("mainThreadAllocatedBytes", allocated);
            result.setNumber("initialHeapBytes", initialHeap);
            result.setNumber("peakHeapBytes", peakHeap.get());
            result.setNumber("retainedHeapBytes", heap.getHeapMemoryUsage().getUsed());
            System.out.println("BENCHMARK_RESULT " + DocumentWriter.defaultWriter().write(result));
            // Keep the history strongly reachable for an independent heap histogram.
            Thread.sleep(20000);
            System.out.println("BENCHMARK_ALIVE " + history.exists() + " " + schema.tableIds().size());
        }
        finally {
            sampler.shutdownNow();
            history.stop();
        }
    }

    private static void seed(String connection, String name, int records) throws Exception {
        final var container = new BlobServiceClientBuilder().connectionString(connection).buildClient()
                .getBlobContainerClient("benchmark");
        container.createIfNotExists();
        final var editor = Table.editor().tableId(new TableId("db", "dbo", "customers"));
        for (int i = 0; i < 8; ++i) {
            editor.addColumn(Column.editor().name("column" + i).position(i + 1)
                    .jdbcType(Types.VARCHAR).type("VARCHAR").length(255).optional(true).create());
        }
        final TableChanges changes = new TableChanges().create(editor.create());
        final Path file = Files.createTempFile("azure-history-fixture", ".jsonl");
        try {
            try (BufferedWriter output = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
                for (int i = 0; i < records; ++i) {
                    output.write('\n');
                    output.write(DocumentWriter.defaultWriter().write(new HistoryRecord(Map.of("server", "benchmark"),
                            Map.of("position", i), "db", "dbo", null, changes, null).document()));
                }
            }
            container.getBlobClient(name).uploadFromFile(file.toString(), false);
            System.out.println("FIXTURE " + name + " records=" + records + " bytes=" + Files.size(file));
        }
        finally {
            Files.deleteIfExists(file);
        }
    }
}
