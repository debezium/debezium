/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.signal.channels;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.pipeline.signal.SignalRecord;

/**
 * The class responsible for processing of signals delivered to Debezium via a file.
 * The signal message must have the following structure, formatted as json line:
 * <ul>
 * <li>{@code id STRING} - the unique identifier of the signal sent, usually UUID, can be used for deduplication</li>
 * <li>{@code type STRING} - the unique logical name of the code executing the signal</li>
 * <li>{@code data STRING} - the data in JSON format that are passed to the signal code
 * </ul>
 *
 * @author Ismail Simsek
 *
 */
public class FileSignalChannel implements SignalChannelReader {

    public static final String CONFIGURATION_FIELD_PREFIX_STRING = "signal.";
    public static final Field SIGNAL_FILE = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "file")
            .withDisplayName("Signal file name")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("The name of the file for the signals to the connector")
            .withValidation(Field::isRequired);
    public static final String CHANNEL_NAME = "file";
    private static final Logger LOGGER = LoggerFactory.getLogger(FileSignalChannel.class);
    ObjectMapper mapper = new ObjectMapper();
    private File signalFile;

    @Override
    public String name() {
        return CHANNEL_NAME;
    }

    @Override
    public void init(CommonConnectorConfig connectorConfig) {

        Configuration signalConfig = connectorConfig.getConfig().subset(CONFIGURATION_FIELD_PREFIX_STRING, false)
                .edit()
                .withDefault(FileSignalChannel.SIGNAL_FILE, "file-signals.txt")
                .build();
        this.signalFile = new File(signalConfig.getString(SIGNAL_FILE));
        LOGGER.info("Reading '{}' file for signals", signalFile.getAbsolutePath());
    }

    @Override
    public void reset(Object reference) {

        try (FileChannel file = FileChannel.open(Paths.get(signalFile.getPath()), StandardOpenOption.WRITE)) {
            file.truncate(0);
        }
        catch (IOException e) {
            LOGGER.error("Unable to truncate file '{}'", signalFile.getAbsolutePath());
        }
    }

    @Override
    public List<SignalRecord> read() {
        List<SignalRecord> signals = new ArrayList<>();

        if (!signalFile.exists() || signalFile.isDirectory()) {
            LOGGER.trace("Signal file not found '{}'", signalFile.getAbsolutePath());
            return signals;
        }

        try {
            // read all the signals from file. signal line must be json formatted
            List<String> lines = Files.readAllLines(signalFile.toPath());
            if (!lines.isEmpty()) {
                // remove signals from file by overriding file with empty content
                new FileWriter(signalFile, false).close();
            }
            Iterator<String> lineIterator = lines.iterator();
            while (lineIterator.hasNext()) {
                String signalLine = lineIterator.next();
                if (signalLine == null || signalLine.isBlank()) {
                    LOGGER.debug("Ignoring empty signal line: `{}`", signalLine);
                    lineIterator.remove();
                    continue;
                }
                try {
                    SignalRecord signal = readSignalString(signalLine);
                    signals.add(signal);
                    LOGGER.info("Processing signal: {}, {}, {}, {}", signal.getId(), signal.getType(), signal.getData(), signal.getAdditionalData());
                }
                catch (final Exception e) {
                    LOGGER.warn("Skipped signal due to an error '{}'", signalLine, e);
                }
                lineIterator.remove();
            }

        }
        catch (Exception e) {
            throw new DebeziumException("Failed to read signal file " + signalFile.getAbsolutePath(), e);
        }

        return signals;
    }

    private SignalRecord readSignalString(String signalLine) throws JsonProcessingException {
        LOGGER.trace("Processing signal line: {}", signalLine);
        JsonNode signalJson = mapper.readTree(signalLine);
        Map<String, Object> additionalData = signalJson.has("additionalData") ? mapper.convertValue(signalJson.get("additionalData"), new TypeReference<>() {
        }) : Map.of();
        Long channelOffset = signalJson.has("channelOffset") ? signalJson.get("channelOffset").asLong(0L) : 0L;
        String id = signalJson.get("id").asText();
        String type = signalJson.get("type").asText();
        String data = signalJson.get("data").toString();
        return new SignalRecord(id, type, data, additionalData);
    }

    @Override
    public void close() {
    }

}
