/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.performance.engine;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import io.debezium.engine.DebeziumEngine;
import io.debezium.util.IoUtil;

/**
 * Base class for JMH benchmark focused on speed of record processing of given {@link DebeziumEngine} implementation.
 */
@State(Scope.Thread)
public abstract class AbstractDebeziumEnginePerf<R> {

    protected static final String OFFSET_FILE_NAME = "offsets.txt";

    private DebeziumEngine<R> engine;
    private ExecutorService executors;
    protected CountDownLatch finishLatch;

    @Param({ "100000", "1000000" })
    public int recordCount;

    public abstract DebeziumEngine createEngine();

    @Setup(Level.Iteration)
    public void doSetup() throws InterruptedException {
        delete(OFFSET_FILE_NAME);

        finishLatch = new CountDownLatch(recordCount);
        engine = createEngine();
        executors = Executors.newFixedThreadPool(1);
        executors.execute(engine);
    }

    @TearDown(Level.Iteration)
    public void doCleanup() throws IOException {
        try {
            if (engine != null) {
                engine.close();
            }
            if (executors != null) {
                executors.shutdown();
                try {
                    executors.awaitTermination(60, TimeUnit.SECONDS);
                }
                catch (InterruptedException e) {
                    executors.shutdownNow();
                }
            }
        }
        finally {
            engine = null;
            executors = null;
        }
    }

    protected Consumer<R> getRecordConsumer() {
        return record -> {
            if (record != null) {
                finishLatch.countDown();
            }
        };
    }

    protected Path getPath(String relativePath) {
        return Paths.get(resolveDataDir(), relativePath).toAbsolutePath();
    }

    private void delete(String relativePath) {
        Path history = getPath(relativePath).toAbsolutePath();
        if (history != null) {
            history = history.toAbsolutePath();
            if (inTestDataDir(history)) {
                try {
                    IoUtil.delete(history);
                }
                catch (IOException e) {
                    // ignored
                }
            }
        }
    }

    private boolean inTestDataDir(Path path) {
        Path target = FileSystems.getDefault().getPath(resolveDataDir()).toAbsolutePath();
        return path.toAbsolutePath().startsWith(target);
    }

    private String resolveDataDir() {
        String value = System.getProperty("dbz.test.data.dir");
        if (value != null && (value = value.trim()).length() > 0) {
            return value;
        }

        value = System.getenv("DBZ_TEST_DATA_DIR");
        if (value != null && (value = value.trim()).length() > 0) {
            return value;
        }

        return "/tmp";
    }
}
