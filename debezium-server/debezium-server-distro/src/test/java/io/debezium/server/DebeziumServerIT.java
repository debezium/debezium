/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server;

import java.time.Duration;

import javax.enterprise.event.Observes;
import javax.inject.Inject;

import org.awaitility.Awaitility;
import org.fest.assertions.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import io.debezium.server.events.ConnectorCompletedEvent;
import io.debezium.server.events.ConnectorStartedEvent;
import io.debezium.util.Testing;
import io.quarkus.test.junit.QuarkusTest;

/**
 * Integration test that verifies basic reading from PostgreSQL database.
 *
 * @author Jiri Pechanec
 */
@QuarkusTest
public class DebeziumServerIT {

    private static final int MESSAGE_COUNT = 4;
    protected static TestDatabase db = null;

    {
        Testing.Files.delete(TestConfigSource.OFFSET_STORE_PATH);
    }

    @AfterAll
    static void stop() {
        if (db != null) {
            db.stop();
        }
    }

    @Inject
    DebeziumServer server;

    void setupDependencies(@Observes ConnectorStartedEvent event) {
        if (!TestConfigSource.isItTest()) {
            return;
        }

        db = new TestDatabase();
        db.start();
    }

    void connectorCompleted(@Observes ConnectorCompletedEvent event) throws Exception {
        if (!event.isSuccess()) {
            throw (Exception) event.getError().get();
        }
    }

    @Test
    public void testPostgres() throws Exception {
        Testing.Print.enable();
        final TestConsumer testConsumer = (TestConsumer) server.getConsumer();
        Awaitility.await().atMost(Duration.ofSeconds(TestConfigSource.waitForSeconds())).until(() -> (testConsumer.getValues().size() >= MESSAGE_COUNT));
        Assertions.assertThat(testConsumer.getValues().size()).isEqualTo(MESSAGE_COUNT);
        Assertions.assertThat(((String) testConsumer.getValues().get(MESSAGE_COUNT - 1)))
                .contains("\"after\":{\"id\":1004,\"first_name\":\"Anne\",\"last_name\":\"Kretchmar\",\"email\":\"annek@noanswer.org\"}");
    }
}
