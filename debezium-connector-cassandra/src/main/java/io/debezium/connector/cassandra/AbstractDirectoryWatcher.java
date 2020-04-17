/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper class around WatchService to make WatchService re-usable and avoid code repetition
 */
public abstract class AbstractDirectoryWatcher {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractDirectoryWatcher.class);

    private final WatchService watchService;
    private final Duration pollInterval;
    private final Path directory;
    private final Set<WatchEvent.Kind> kinds;

    public AbstractDirectoryWatcher(Path directory, Duration pollInterval, Set<WatchEvent.Kind> kinds) throws IOException {
        this(FileSystems.getDefault().newWatchService(), directory, pollInterval, kinds);
    }

    AbstractDirectoryWatcher(WatchService watchService, Path directory, Duration pollInterval, Set<WatchEvent.Kind> kinds) throws IOException {
        this.watchService = watchService;
        this.pollInterval = pollInterval;
        this.directory = directory;
        this.kinds = kinds;

        directory.register(watchService, kinds.toArray(new WatchEvent.Kind[kinds.size()]));
    }

    public void poll() throws InterruptedException, IOException {
        LOGGER.debug("Polling commitLogFiles from cdc_raw directory...");
        WatchKey key = watchService.poll(pollInterval.toMillis(), TimeUnit.MILLISECONDS);

        if (key != null) {
            LOGGER.debug("Detected new commitLogFiles in cdc_raw directory.");
            for (WatchEvent<?> event : key.pollEvents()) {
                Path relativePath = (Path) event.context();
                Path absolutePath = directory.resolve(relativePath);

                if (kinds.contains(event.kind())) {
                    handleEvent(event, absolutePath);
                }
            }
            key.reset();
        }
        else {
            LOGGER.debug("No commitLogFile is detected in cdc_raw directory.");
        }
    }

    abstract void handleEvent(WatchEvent<?> event, Path path) throws IOException;
}
