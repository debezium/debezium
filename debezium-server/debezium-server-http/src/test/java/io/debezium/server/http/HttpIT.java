/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.http;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.configureFor;
import static com.github.tomakehurst.wiremock.client.WireMock.getAllServeEvents;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.enterprise.event.Observes;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import com.google.inject.Inject;

import io.debezium.server.DebeziumServer;
import io.debezium.server.events.ConnectorCompletedEvent;
import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to an HTTP Server
 *
 * @author Chris Baumbauer
 */

@QuarkusTest
@QuarkusTestResource(PostgresTestResourceLifecycleManager.class)
@QuarkusTestResource(HttpTestResourceLifecycleManager.class)
public class HttpIT {
    @Inject
    DebeziumServer server;

    private static final int MESSAGE_COUNT = 4;

    {
        Testing.Files.delete(HttpTestConfigSource.OFFSET_STORE_PATH);
        Testing.Files.createTestingFile(HttpTestConfigSource.OFFSET_STORE_PATH);
    }

    void connectorCompleted(@Observes ConnectorCompletedEvent event) throws Exception {
        if (!event.isSuccess()) {
            throw (Exception) event.getError().get();
        }
    }

    @Test
    public void testHttpServer() {
        Testing.Print.enable();

        List<ServeEvent> events = new ArrayList<>();
        configureFor(HttpTestResourceLifecycleManager.getHost(), HttpTestResourceLifecycleManager.getPort());
        stubFor(post("/").willReturn(aResponse().withStatus(200)));

        Awaitility.await().atMost(Duration.ofSeconds(60)).until(() -> {
            events.addAll(getAllServeEvents());

            return events.size() == MESSAGE_COUNT;
        });

        Assertions.assertEquals(MESSAGE_COUNT, events.size());

        for (ServeEvent e : events) {
            LoggedRequest request = e.getRequest();
            // Assert the content type is set correctly to reflect a cloudevent
            Assertions.assertEquals(request.getHeader("content-type"), "application/cloudevents+json");

            // deserialize the cloudevent into a HashMap<String, Object> and assert the cloudevent metadata is set properly
            try {
                ObjectMapper om = new ObjectMapper();
                HashMap<String, Object> hm;
                TypeReference<HashMap<String, Object>> tref = new TypeReference<>() {
                };
                hm = om.readValue(request.getBody(), tref);

                Assertions.assertEquals("/debezium/postgresql/testc", (String) hm.get("source"));
                Assertions.assertEquals("io.debezium.postgresql.datachangeevent", (String) hm.get("type"));
                Assertions.assertEquals("1.0", (String) hm.get("specversion"));
                Assertions.assertEquals("postgres", (String) hm.get("iodebeziumdb"));
                Assertions.assertEquals("inventory", (String) hm.get("iodebeziumschema"));
                Assertions.assertEquals("customers", (String) hm.get("iodebeziumtable"));
                String eventID = (String) hm.get("id");
                Assertions.assertTrue(eventID.length() > 0);

            }
            catch (IOException ioe) {
                Assertions.fail(ioe);
            }
        }
    }
}
