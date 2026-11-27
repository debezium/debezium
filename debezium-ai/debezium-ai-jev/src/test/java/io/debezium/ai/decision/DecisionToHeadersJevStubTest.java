/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.decision;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.DebeziumException;
import io.debezium.ai.decision.jev.FakeJevServer;
import io.debezium.ai.decision.jev.FakeJevServer.Reply;

/**
 * Runs the example of the {@code decision.adoc} documentation, configured from a plain map,
 * end to end through the {@code jev} provider against a real in-process HTTP server.
 *
 * @author Jiri Pechanec
 */
public class DecisionToHeadersJevStubTest {

    private static final String PREFIX = "__debezium.ai.decision.";
    private static final String API_KEY = "doc-example-key-5555";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private FakeJevServer server;
    private DecisionToHeaders<SourceRecord> smt;

    @BeforeEach
    public void startServer() throws IOException {
        server = new FakeJevServer();
    }

    @AfterEach
    public void cleanUp() {
        if (smt != null) {
            smt.close();
        }
        server.close();
    }

    /**
     * The "product categorization" example of the documentation.
     */
    private Map<String, String> documentationExample() {
        final Map<String, String> config = new LinkedHashMap<>();
        config.put("decision.provider", "jev");
        config.put("jev.api.key", API_KEY);
        config.put("jev.base.url", server.baseUrl());
        config.put("jev.retry.initial.delay.ms", "1");
        config.put("jev.retry.max.delay.ms", "5");
        config.put("state.fields", "after.name,after.description");
        config.put("questions", "category,severity,emergency");
        config.put("questions.category.type", "choice");
        config.put("questions.category.instructions", "Which team should handle this product change?");
        config.put("questions.category.criteria.MARKETING", "Promotion and positioning");
        config.put("questions.category.criteria.SAFETY", "Hazards and recalls");
        config.put("questions.category.criteria.SHIPPING", "Logistics and delivery");
        config.put("questions.severity.type", "score");
        config.put("questions.severity.instructions", "How severe is the issue described by the product?");
        config.put("questions.severity.criteria.0", "Cosmetic");
        config.put("questions.severity.criteria.1", "Needs attention");
        config.put("questions.severity.criteria.2", "Dangerous");
        config.put("questions.emergency.type", "yesno");
        config.put("questions.emergency.instructions", "Does the product require immediate intervention?");
        return config;
    }

    private void configure(final Map<String, String> config) {
        smt = new DecisionToHeaders<>();
        smt.configure(config);
    }

    @Test
    public void shouldDecorateTheRecordWithTheAnswersOfJev() throws Exception {
        server.respondAlways(Reply.ok(FakeJevServer.fixture("response-mixed.json")));
        configure(documentationExample());
        final var record = ProductRecords.create("Space heater", "Heats a room, may overheat");

        final var result = smt.apply(record);

        assertThat(result.value()).isSameAs(record.value());
        final var headers = result.headers();
        assertThat(headers.lastWithName(PREFIX + "status").value()).isEqualTo("SUCCESS");
        assertThat(headers.lastWithName(PREFIX + "model").value()).isEqualTo("jev-1.13.0");
        assertThat(headers.lastWithName(PREFIX + "error")).isNull();

        assertThat(headers.lastWithName(PREFIX + "category").value()).isEqualTo("SAFETY");
        assertThat(headers.lastWithName(PREFIX + "category").schema()).isSameAs(Schema.STRING_SCHEMA);
        assertThat(headers.lastWithName(PREFIX + "category.confidence").value()).isEqualTo(0.91);
        assertThat(MAPPER.readTree((String) headers.lastWithName(PREFIX + "category.probabilities").value()))
                .isEqualTo(MAPPER.readTree("{\"MARKETING\":0.02,\"SAFETY\":0.95,\"SHIPPING\":0.03}"));

        assertThat(headers.lastWithName(PREFIX + "severity").value()).isEqualTo(1.23);
        assertThat(headers.lastWithName(PREFIX + "severity").schema()).isSameAs(Schema.FLOAT64_SCHEMA);
        assertThat(headers.lastWithName(PREFIX + "severity.confidence").value()).isEqualTo(0.8);
        assertThat(MAPPER.readTree((String) headers.lastWithName(PREFIX + "severity.probabilities").value()))
                .isEqualTo(MAPPER.readTree("{\"0\":0.2,\"1\":0.37,\"2\":0.43}"));

        assertThat(headers.lastWithName(PREFIX + "emergency").value()).isEqualTo(0.12);
        assertThat(headers.lastWithName(PREFIX + "emergency.confidence")).isNull();
    }

    @Test
    public void shouldSendTheExpectedRequestToJev() throws Exception {
        server.respondAlways(Reply.ok(FakeJevServer.fixture("response-mixed.json")));
        configure(documentationExample());

        smt.apply(ProductRecords.create("Space heater", "Heats a room, may overheat"));

        assertThat(server.requests()).hasSize(1);
        final var received = server.requests().get(0);
        assertThat(received.path()).isEqualTo("/v1/systemone");
        assertThat(received.header("Authorization")).isEqualTo("Bearer " + API_KEY);
        assertThat(MAPPER.readTree(received.body())).isEqualTo(MAPPER.readTree(FakeJevServer.fixture("request-mixed.json")));
    }

    @Test
    public void shouldFailTheRecordByDefaultWithoutExposingTheKey() {
        server.respondAlways(Reply.status(401, "{\"detail\":\"Invalid API key " + API_KEY + "\"}"));
        configure(documentationExample());

        assertThatThrownBy(() -> smt.apply(ProductRecords.create("a", "b")))
                .isInstanceOf(DebeziumException.class)
                .hasMessageNotContaining(API_KEY)
                .hasRootCauseMessage("Jev request failed with HTTP status 401: Invalid API key ***");
        assertThat(server.requestCount()).isEqualTo(1);
    }

    @Test
    public void shouldPassTheRecordThroughWhenSkipping() {
        server.respondAlways(Reply.status(401, "{}"));
        final var config = documentationExample();
        config.put("decision.error.mode", "skip");
        configure(config);
        final var record = ProductRecords.create("a", "b");

        final var result = smt.apply(record);

        assertThat(result.headers().size()).isZero();
        assertThat(result.value()).isSameAs(record.value());
    }

    @Test
    public void shouldWriteTheErrorHeaderWhenConfigured() {
        server.respondAlways(Reply.status(401, "{\"detail\":\"Invalid API key " + API_KEY + "\"}"));
        final var config = documentationExample();
        config.put("decision.error.mode", "header");
        configure(config);

        final var result = smt.apply(ProductRecords.create("a", "b"));

        assertThat(result.headers().lastWithName(PREFIX + "status").value()).isEqualTo("ERROR");
        assertThat((String) result.headers().lastWithName(PREFIX + "error").value()).contains("401").doesNotContain(API_KEY);
        assertThat(result.headers().lastWithName(PREFIX + "category")).isNull();
    }

    @ParameterizedTest
    @ValueSource(strings = { "/* unavailable */", "null" })
    public void shouldReportHttpStatusWhenErrorBodyHasNoDetails(final String body) {
        server.respondAlways(Reply.status(503, body));
        final var config = documentationExample();
        config.put("decision.error.mode", "header");
        config.put("jev.retry.max.attempts", "1");
        configure(config);

        final var result = smt.apply(ProductRecords.create("a", "b"));

        assertThat(result.headers().lastWithName(PREFIX + "status").value()).isEqualTo("ERROR");
        assertThat(result.headers().lastWithName(PREFIX + "error").value()).isEqualTo("Jev request failed with HTTP status 503 (after 1 attempts)");
        assertThat(result.headers().lastWithName(PREFIX + "category")).isNull();
    }

    @Test
    public void shouldFailAtStartupWithoutApiKey() {
        final var config = documentationExample();
        config.remove("jev.api.key");
        smt = new DecisionToHeaders<>();

        assertThatThrownBy(() -> smt.configure(config)).isInstanceOf(ConfigException.class).hasMessageContaining("jev.api.key");
    }

    @Test
    public void shouldFailAtStartupWhenJevLimitsAreExceeded() {
        final var config = documentationExample();
        for (var i = 3; i <= 10; i++) {
            config.put("questions.severity.criteria." + i, "level " + i);
        }
        smt = new DecisionToHeaders<>();

        assertThatThrownBy(() -> smt.configure(config)).isInstanceOf(ConfigException.class).hasMessageContaining("severity").hasMessageContaining("at most 10");
    }
}
