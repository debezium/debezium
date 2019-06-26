/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import io.debezium.connector.cassandra.exceptions.CassandraConnectorConfigException;
import io.debezium.connector.cassandra.exceptions.CassandraConnectorTaskException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Properties;

/**
 * A concrete implementation of {@link OffsetWriter} which tracks the progress of events
 * being processed by the {@link SnapshotProcessor} and {@link CommitLogProcessor} to
 * property files, snapshot_offset.properties and commitlog_offset.properties, respectively.
 *
 * The property key is the table for the offset, and is serialized in the format of <keyspace>.<table>
 * The property value is the offset position, and is serialized in the format of <file_name>:<file_position>.
 *
 * For snapshots, a table is either fully processed or not processed at all,
 * so offset is given a default value of ":-1" , where the filename is an empty
 * string, and file position is -1.
 *
 * For commit logs, the file_name represents the commit log file name and
 * file position represents bytes read in the commit log.
 */
public class FileOffsetWriter implements OffsetWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileOffsetWriter.class);

    public static final String SNAPSHOT_OFFSET_FILE = "snapshot_offset.properties";
    public static final String COMMITLOG_OFFSET_FILE = "commitlog_offset.properties";

    private final Properties snapshotProps = new Properties();
    private final Properties commitLogProps = new Properties();

    private final File offsetDir;

    private final File snapshotOffsetFile;
    private final File commitLogOffsetFile;

    private final FileLock snapshotOffsetFileLock;
    private final FileLock commitLogOffsetFileLock;

    public FileOffsetWriter(String offsetDir) throws IOException {
        if (offsetDir == null) {
            throw new CassandraConnectorConfigException("Offset file directory must be configured at the start");
        }

        this.offsetDir = new File(offsetDir);
        this.snapshotOffsetFile = Paths.get(this.offsetDir.getAbsolutePath(), SNAPSHOT_OFFSET_FILE).toFile();
        this.commitLogOffsetFile = Paths.get(this.offsetDir.getAbsolutePath(), COMMITLOG_OFFSET_FILE).toFile();

        snapshotOffsetFileLock = init(this.snapshotOffsetFile);
        commitLogOffsetFileLock = init(this.commitLogOffsetFile);

        loadOffset(this.snapshotOffsetFile, snapshotProps);
        loadOffset(this.commitLogOffsetFile, commitLogProps);
    }

    @Override
    public void markOffset(String sourceTable, String sourceOffset, boolean isSnapshot) {
        if (isSnapshot) {
            synchronized (snapshotOffsetFileLock) {
                if (!isOffsetProcessed(sourceTable, sourceOffset, isSnapshot)) {
                    snapshotProps.setProperty(sourceTable, sourceOffset);
                }
            }
        } else {
            synchronized (commitLogOffsetFileLock) {
                if (!isOffsetProcessed(sourceTable, sourceOffset, isSnapshot)) {
                    commitLogProps.setProperty(sourceTable, sourceOffset);
                }
            }
        }
    }

    @Override
    public boolean isOffsetProcessed(String sourceTable, String sourceOffset, boolean isSnapshot) {
        if (isSnapshot) {
            synchronized (snapshotOffsetFileLock) {
                return snapshotProps.containsKey(sourceTable);
            }
        } else {
            synchronized (commitLogOffsetFileLock) {
                OffsetPosition currentOffset = OffsetPosition.parse(sourceOffset);
                OffsetPosition recordedOffset = commitLogProps.containsKey(sourceTable) ? OffsetPosition.parse((String) commitLogProps.get(sourceTable)) : null;
                return recordedOffset != null && currentOffset.compareTo(recordedOffset) <= 0;
            }
        }
    }

    @Override
    public void flush() {
        try {
            synchronized (snapshotOffsetFileLock) {
                saveOffset(snapshotOffsetFile, snapshotProps);
            }
            synchronized (commitLogOffsetFileLock) {
                saveOffset(commitLogOffsetFile, commitLogProps);
            }
        } catch (IOException e) {
            LOGGER.error("Ignoring flush failure", e);
        }
    }

    public void close() {
        try {
            snapshotOffsetFileLock.release();
        } catch (IOException e) {
            LOGGER.error("Failed to release snapshot offset file lock");
        }

        try {
            commitLogOffsetFileLock.release();
        } catch (IOException e) {
            LOGGER.error("Failed to release commit log offset file lock");
        }
    }

    private static void saveOffset(File offsetFile, Properties props) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(offsetFile)) {
            props.store(fos, null);
        } catch (IOException e) {
            LOGGER.error("Failed to save offset for file " + offsetFile.getName(), e);
            throw e;
        }
    }

    private void loadOffset(File offsetFile, Properties props) throws IOException {
        try (FileInputStream fis = new FileInputStream(offsetFile)) {
            props.load(fis);
        } catch (IOException e) {
            LOGGER.error("Failed to load offset for file " + offsetFile.getName(), e);
            throw e;
        }
    }

    private FileLock init(File offsetFile) throws IOException {
        if (!offsetFile.exists()) {
            try {
                Files.createDirectories(offsetDir.toPath());
                Files.createFile(offsetFile.toPath());
            } catch (FileAlreadyExistsException e) {
                // do nothing
            }
        }

        try {
            FileChannel channel = FileChannel.open(offsetFile.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE);
            FileLock lock = channel.tryLock();
            if (lock == null) {
                throw new CassandraConnectorTaskException("Failed to acquire file lock on " + offsetFile.getName() + ". There might be another Cassandra Connector Task running");
            }
            return lock;
        } catch (OverlappingFileLockException e) {
            throw new CassandraConnectorTaskException("Failed to acquire file lock on " + offsetFile.getName() + ". There might be another thread running", e);
        }
    }
}
