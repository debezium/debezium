/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.signal;

import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.pipeline.source.snapshot.incremental.AbstractReadOnlyIncrementalSnapshotChangeEventSource;
import io.debezium.schema.DataCollectionId;

/**
 * The class responsible for processing of signals delivered to Debezium via a
 * file. The signal message must have the following structure:
 * <ul>
 * <li>{@code data STRING} - the data in JSON format that are passed to the
 * signal code
 * </ul>
 */
public class FileSignalThread<T extends DataCollectionId> extends AbstractExternalSignalThread<T> {

    public static final String CONFIGURATION_FIELD_PREFIX_STRING = "signal.";

    public static final Field SIGNAL_FILE = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "file")
            .withDisplayName("Signal file name").withType(ConfigDef.Type.STRING).withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("The name of the file for the signals to the connector").withValidation(Field::isRequired);

    private static final Logger LOGGER = LoggerFactory.getLogger(FileSignalThread.class);

    private final Path signalFilePath;

    public FileSignalThread(CommonConnectorConfig connectorConfig,
                            AbstractReadOnlyIncrementalSnapshotChangeEventSource<T> eventSource) {
        super(connectorConfig, eventSource);
        Configuration signalConfig = connectorConfig.getConfig().subset(CONFIGURATION_FIELD_PREFIX_STRING, false).edit()
                .build();
        this.signalFilePath = Paths.get(signalConfig.getString(SIGNAL_FILE));
        LOGGER.info("Using file '{}' for signals", signalFilePath.toAbsolutePath());
    }

    protected void monitorSignals() {
        while (true) {
            try {
                // unlike KafkaSignalThread we are not mathing signal key to connector's name.
                // processing all the values/signals.
                List<String> lines = Files.readAllLines(signalFilePath, StandardCharsets.UTF_8);
                lines.forEach(line -> {
                    try {
                        processSignal(reader.read(line), -1);
                    }
                    catch (final Exception e) {
                        LOGGER.error("Skipped signal due to an error '{}'", line, e);
                    }
                });
                // remove signals from file
                if (!lines.isEmpty()) {
                    new FileWriter(signalFilePath.toFile(), false).close();
                }
            }
            catch (Exception e) {
                throw new DebeziumException("Failed to read signal file " + signalFilePath, e);
            }
        }
    }
}