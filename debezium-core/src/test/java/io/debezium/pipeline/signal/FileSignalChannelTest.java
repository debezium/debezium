/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.signal;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;

import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.pipeline.signal.channels.FileSignalChannel;

/**
 * @author Ismail Simsek
 *
 */
public class FileSignalChannelTest {

    Path signalsData = Paths.get("src", "test", "resources").resolve("debezium_signaling_file.signals.txt");
    Path signalsFile = Paths.get("src", "test", "resources").resolve("debezium_signaling_file.txt");

    @Test
    public void shouldLoadFileSignalsTest() throws IOException {
        Files.copy(signalsData, signalsFile, StandardCopyOption.REPLACE_EXISTING);

        final FileSignalChannel fileSignalChannel = new FileSignalChannel();
        fileSignalChannel.init(config());
        List<SignalRecord> signalRecords = fileSignalChannel.read();
        // only two whitespace lines are ignored
        assertThat(signalRecords).hasSize(2);
        assertThat(signalRecords.get(0).getData().contains("public.MyFirstTable")).isTrue();
        Files.deleteIfExists(signalsFile.toAbsolutePath());
    }

    protected CommonConnectorConfig config() {
        return new CommonConnectorConfig(Configuration.create()
                .with(FileSignalChannel.SIGNAL_FILE, signalsFile.toString())
                .build(), 0) {
            @Override
            protected SourceInfoStructMaker<?> getSourceInfoStructMaker(Version version) {
                return null;
            }

            @Override
            public String getContextName() {
                return null;
            }

            @Override
            public String getConnectorName() {
                return null;
            }
        };
    }
}
