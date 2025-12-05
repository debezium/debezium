/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.signal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Optional;

import org.junit.Test;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.doc.FixFor;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.pipeline.signal.channels.KafkaSignalChannel;

/**
 * @author Chris Cranford
 */
public class KafkaSignalChannelTest {

    @Test
    @FixFor("DBZ-9052")
    public void shouldFailWithInvalidConfigurationMessageAndLogValidationFailures() {
        final LogInterceptor interceptor = new LogInterceptor(KafkaSignalChannel.class);

        assertThrows(DebeziumException.class,
                () -> {
                    final var channel = new KafkaSignalChannel();
                    channel.init(config(Configuration.create().with(KafkaSignalChannel.SIGNAL_TOPIC, "abc").build()));
                },
                "Signal channel kafka configuration is invalid. See logs for details.");

        assertThat(interceptor.containsMessage("The 'signal.kafka.bootstrap.servers' value is invalid: A value is required")).isTrue();
    }

    protected CommonConnectorConfig config(Configuration config) {
        return new CommonConnectorConfig(config, 0) {
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

            @Override
            public EnumeratedValue getSnapshotMode() {
                return null;
            }

            @Override
            public Optional<EnumeratedValue> getSnapshotLockingMode() {
                return Optional.empty();
            }
        };
    }
}
