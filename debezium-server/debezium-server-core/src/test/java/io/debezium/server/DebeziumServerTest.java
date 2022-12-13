/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.Properties;

import javax.enterprise.event.Observes;
import javax.inject.Inject;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.server.events.ConnectorStartedEvent;
import io.debezium.util.Collect;
import io.debezium.util.Testing;
import io.quarkus.test.junit.QuarkusTest;

/**
 * Smoke test that verifies the basic functionality of Quarkus-based server.
 *
 * @author Jiri Pechanec
 */
@QuarkusTest
public class DebeziumServerTest {

    private static final int MESSAGE_COUNT = 5;

    {
        Testing.Files.delete(TestConfigSource.OFFSET_STORE_PATH);
    }

    void setupDependencies(@Observes ConnectorStartedEvent event) {
        Testing.Files.delete(TestConfigSource.TEST_FILE_PATH);
        Testing.Files.createTestingFile(TestConfigSource.TEST_FILE_PATH);
        appendLinesToSource(MESSAGE_COUNT);
        Testing.Print.enable();
    }

    @Inject
    DebeziumServer server;

    @Test
    public void testProps() {
        Properties properties = server.getProps();
        assertThat(properties.getProperty(RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST.name())).isNotNull();
        assertThat(properties.getProperty(RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST.name())).isEqualTo("public.table_name");

        assertThat(properties.getProperty("offset.flush.interval.ms.test")).isNotNull();
        assertThat(properties.getProperty("offset.flush.interval.ms.test")).isEqualTo("0");

        assertThat(properties.getProperty("snapshot.select.statement.overrides.public.table_name")).isNotNull();
        assertThat(properties.getProperty("snapshot.select.statement.overrides.public.table_name")).isEqualTo("SELECT * FROM table_name WHERE 1>2");

        assertThat(properties.getProperty("database.allowPublicKeyRetrieval")).isNotNull();
        assertThat(properties.getProperty("database.allowPublicKeyRetrieval")).isEqualTo("true");
    }

    @Test
    public void testJson() throws Exception {
        final TestConsumer testConsumer = (TestConsumer) server.getConsumer();
        Awaitility.await().atMost(Duration.ofSeconds(TestConfigSource.waitForSeconds())).until(() -> (testConsumer.getValues().size() >= MESSAGE_COUNT));
        assertThat(testConsumer.getValues().size()).isEqualTo(MESSAGE_COUNT);
        assertThat(testConsumer.getValues().get(MESSAGE_COUNT - 1)).isEqualTo("{\"line\":\"" + MESSAGE_COUNT + "\"}");
    }

    static void appendLinesToSource(int numberOfLines) {
        CharSequence[] lines = new CharSequence[numberOfLines];
        for (int i = 0; i != numberOfLines; ++i) {
            lines[i] = generateLine(i + 1);
        }
        try {
            java.nio.file.Files.write(TestConfigSource.TEST_FILE_PATH, Collect.arrayListOf(lines), StandardCharsets.UTF_8, StandardOpenOption.APPEND);
        }
        catch (IOException e) {
            throw new DebeziumException(e);
        }
    }

    static String generateLine(int lineNumber) {
        return Integer.toString(lineNumber);
    }
}
