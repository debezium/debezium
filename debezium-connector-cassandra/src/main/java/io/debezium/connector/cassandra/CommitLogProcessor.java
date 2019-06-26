/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import io.debezium.connector.cassandra.exceptions.CassandraConnectorTaskException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;


/**
 * The {@link CommitLogProcessor} is used to process CommitLog in CDC directory.
 * Upon readCommitLog, it processes the entire CommitLog specified in the {@link CassandraConnectorConfig}
 * and converts each row change in the commit log into a {@link Record},
 * and then emit the log via a {@link KafkaRecordEmitter}.
 */
public class CommitLogProcessor extends AbstractProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(CommitLogProcessor.class);

    private static final String NAME = "Commit Log Processor";

    private final org.apache.cassandra.db.commitlog.CommitLogReader commitLogReader;
    private final CommitLogReadHandlerImpl commitLogReadHandler;
    private final File cdcDir;
    private final AbstractDirectoryWatcher watcher;
    private final BlockingEventQueue<Event> queue;
    private final boolean latestOnly;
    private final CommitLogProcessorMetrics metrics = new CommitLogProcessorMetrics();

    public CommitLogProcessor(CassandraConnectorContext context, AtomicBoolean taskState) throws IOException {
        super(NAME, taskState);
        commitLogReader = new org.apache.cassandra.db.commitlog.CommitLogReader();
        queue = context.getQueue();
        commitLogReadHandler = new CommitLogReadHandlerImpl(
                context.getSchemaHolder(),
                context.getQueue(),
                context.getOffsetWriter(),
                new RecordMaker(context.getCassandraConnectorConfig().tombstonesOnDelete(), new Filters(context.getCassandraConnectorConfig().fieldBlacklist())),
                metrics);
        cdcDir = new File(DatabaseDescriptor.getCDCLogLocation());
        watcher = new AbstractDirectoryWatcher(cdcDir.toPath(), context.getCassandraConnectorConfig().cdcDirPollIntervalMs(), Collections.singleton(ENTRY_CREATE)) {
            @Override
            void handleEvent(WatchEvent event, Path path) throws IOException {
                processCommitLog(path.toFile());
            }
        };
        latestOnly = context.getCassandraConnectorConfig().latestCommitLogOnly();
    }

    @Override
    public void initialize() {
        metrics.registerMetrics();
    }

    @Override
    public void destroy() {
        metrics.unregisterMetrics();
    }

    @Override
    public void doStart() throws InterruptedException {
        try {
            if (latestOnly) {
                File lastModified = getLastModifiedCommitLog();
                if (lastModified != null) {
                    processCommitLog(lastModified);
                } else {
                    LOGGER.info("No commit logs found in {}", DatabaseDescriptor.getCommitLogLocation());
                }
            } else {
                processExistingCommitLogs();
                processWatchEventCommitLogs();
            }
        } catch (IOException e) {
            throw new CassandraConnectorTaskException(e);
        }
    }

    @Override
    public void doStop() { }

    private void processExistingCommitLogs() throws IOException {
        LOGGER.info("Reading existing commit logs in {}", cdcDir);
        File[] commitLogFiles = CommitLogUtil.getCommitLogs(cdcDir);
        Arrays.sort(commitLogFiles, CommitLogUtil::compareCommitLogs);

        for (File commitLogFile : commitLogFiles) {
            if (isTaskRunning()) {
                processCommitLog(commitLogFile);
            }
        }
    }

    private void processWatchEventCommitLogs() throws IOException, InterruptedException {
        while (isTaskRunning()) {
            watcher.poll();
        }
    }

    void processCommitLog(File file) throws IOException {
        if (file == null) {
            throw new IOException("Commit log is null");
        }
        if (!file.exists()) {
            throw new IOException("Commit log " + file.getName() + " does not exist");
        }
        try {
            LOGGER.info("Processing commit log {}", file.getName());
            metrics.setCommitLogFilename(file.getName());
            commitLogReader.readCommitLogSegment(commitLogReadHandler, file, false);
            queue.enqueue(new EOFEvent(file, true));
            LOGGER.info("Successfully processed commit log {}", file.getName());
        } catch (IOException e) {
            queue.enqueue(new EOFEvent(file, false));
            LOGGER.warn("Error occurred while processing commit log " + file.getName(), e);
        }
    }

    File getLastModifiedCommitLog() {
        LOGGER.warn("CommitLogProcessor will read the last modified commit log from the COMMIT LOG "
                + "DIRECTORY based on modified timestamp, NOT FROM THE CDC_RAW DIRECTORY. This method "
                + "should not be used in PRODUCTION!");
        File commitLogDir = new File(DatabaseDescriptor.getCommitLogLocation());
        File[] files = CommitLogUtil.getCommitLogs(commitLogDir);

        File commitLogFile = null;
        for (File file : files) {
            if (commitLogFile == null || commitLogFile.lastModified() < file.lastModified()) {
                commitLogFile = file;
            }
        }
        return commitLogFile;
    }
}
