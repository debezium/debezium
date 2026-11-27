/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.decision.systemone;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.ai.decision.api.ChoiceAnswer;
import io.debezium.ai.decision.api.DecisionException;
import io.debezium.ai.decision.api.DecisionRequest;
import io.debezium.ai.decision.api.Usage;
import io.debezium.ai.decision.jev.FakeJevServer;
import io.debezium.ai.decision.jev.FakeJevServer.Reply;
import io.debezium.ai.decision.jev.JevDecisionServiceFactory;
import io.debezium.config.Configuration;

/**
 * Tests of {@link SystemOneDecisionService} against a real HTTP server.
 *
 * @author Jiri Pechanec
 */
public class JevDecisionServiceTest {

    private static final String API_KEY = "test-api-key-1234";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private FakeJevServer server;

    @BeforeEach
    public void startServer() throws IOException {
        server = new FakeJevServer();
    }

    @AfterEach
    public void stopServer() {
        server.close();
    }

    private SystemOneSettings settings(final String baseUrl, final Map<String, String> overrides) {
        final Map<String, String> values = new HashMap<>();
        values.put("jev.api.key", API_KEY);
        values.put("jev.base.url", baseUrl);
        values.put("jev.retry.initial.delay.ms", "1");
        values.put("jev.retry.max.delay.ms", "5");
        values.putAll(overrides);
        final SystemOneDecisionServiceFactory factory = new JevDecisionServiceFactory();
        return factory.settings(Configuration.from(values));
    }

    private SystemOneDecisionService service(final Map<String, String> overrides) {
        return new SystemOneDecisionService(settings(server.baseUrl(), overrides));
    }

    private static DecisionRequest request() {
        return new DecisionRequest(SystemOneRequestMapperTest.mixedState(),
                List.of(SystemOneRequestMapperTest.categoryQuestion(), SystemOneRequestMapperTest.severityQuestion(), SystemOneRequestMapperTest.emergencyQuestion()));
    }

    private static Reply okReply() throws IOException {
        return Reply.ok(FakeJevServer.fixture("response-mixed.json"));
    }

    @Test
    public void shouldSendTheRequestAndParseTheResponse() throws Exception {
        server.respondAlways(okReply());

        try (var service = service(Map.of())) {
            final var response = service.decide(request());

            assertThat(response.model()).isEqualTo("jev-1.13.0");
            assertThat(response.usage()).isEqualTo(new Usage(120, 6));
            assertThat(((ChoiceAnswer) response.answers().get("category")).choice()).isEqualTo("SAFETY");
        }

        assertThat(server.requests()).hasSize(1);
        final var received = server.requests().get(0);
        assertThat(received.method()).isEqualTo("POST");
        assertThat(received.path()).isEqualTo("/v1/systemone");
        assertThat(received.header("Authorization")).isEqualTo("Bearer " + API_KEY);
        assertThat(received.header("Content-Type")).isEqualTo("application/json");
        assertThat(received.header("User-Agent")).isEqualTo("debezium-ai-jev");
        assertThat(received.header(SystemOneDecisionService.LONG_CONTEXT_HEADER)).isNull();
        assertThat(MAPPER.readTree(received.body())).isEqualTo(SystemOneRequestMapperTest.fixture("request-mixed.json"));
    }

    @Test
    public void shouldSendTheLongContextHeaderOnlyWhenEnabled() throws Exception {
        server.respondAlways(okReply());

        try (var service = service(Map.of("jev.long.context", "true"))) {
            service.decide(request());
        }

        assertThat(server.requests().get(0).header(SystemOneDecisionService.LONG_CONTEXT_HEADER)).isEqualTo("true");
    }

    @Test
    public void shouldUseTheConfiguredModel() throws Exception {
        server.respondAlways(okReply());

        try (var service = service(Map.of("jev.model", "jev-custom"))) {
            service.decide(request());
        }

        assertThat(MAPPER.readTree(server.requests().get(0).body()).path("model").asText()).isEqualTo("jev-custom");
    }

    @Test
    public void shouldTolerateTrailingSlashInBaseUrl() throws Exception {
        server.respondAlways(okReply());

        try (var service = new SystemOneDecisionService(settings(server.baseUrl() + "/", Map.of()))) {
            service.decide(request());
        }

        assertThat(server.requests().get(0).path()).isEqualTo("/v1/systemone");
    }

    @Test
    public void shouldNotRetryAuthenticationFailureAndHideTheKey() {
        server.respondAlways(Reply.status(401, "{\"detail\":\"Invalid API key " + API_KEY + "\"}"));

        try (var service = service(Map.of())) {
            assertThatThrownBy(() -> service.decide(request()))
                    .isInstanceOfSatisfying(DecisionException.class, e -> {
                        assertThat(e.isRetriable()).isFalse();
                        assertThat(e.httpStatus()).isEqualTo(401);
                    })
                    .hasMessageContaining("HTTP status 401")
                    .hasMessageContaining("Invalid API key ***")
                    .hasMessageNotContaining(API_KEY);
        }
        assertThat(server.requestCount()).isEqualTo(1);
    }

    @Test
    public void shouldNotRetryBadRequest() {
        server.respondAlways(Reply.status(400, "{\"detail\":\"state is too long\"}"));

        try (var service = service(Map.of())) {
            assertThatThrownBy(() -> service.decide(request()))
                    .isInstanceOfSatisfying(DecisionException.class, e -> assertThat(e.isRetriable()).isFalse())
                    .hasMessageContaining("HTTP status 400")
                    .hasMessageContaining("state is too long");
        }
        assertThat(server.requestCount()).isEqualTo(1);
    }

    @Test
    public void shouldNotRetryValidationErrorAndNotEchoTheState() {
        server.respondAlways(Reply.status(422,
                "{\"detail\":[{\"loc\":[\"body\",\"questions\"],\"msg\":\"Field required\",\"type\":\"missing\",\"input\":\"confidential row content\"}]}"));

        try (var service = service(Map.of())) {
            assertThatThrownBy(() -> service.decide(request()))
                    .isInstanceOfSatisfying(DecisionException.class, e -> {
                        assertThat(e.isRetriable()).isFalse();
                        assertThat(e.httpStatus()).isEqualTo(422);
                    })
                    .hasMessageContaining("HTTP status 422")
                    .hasMessageContaining("Field required")
                    .hasMessageNotContaining("confidential row content");
        }
        assertThat(server.requestCount()).isEqualTo(1);
    }

    @Test
    public void shouldNotRetryWhenErrorBodyIsNotJson() {
        server.respondAlways(Reply.status(403, "<html>forbidden</html>"));

        try (var service = service(Map.of())) {
            assertThatThrownBy(() -> service.decide(request()))
                    .hasMessage("Jev request failed with HTTP status 403");
        }
        assertThat(server.requestCount()).isEqualTo(1);
    }

    @Test
    public void shouldRetryRateLimitAndSucceed() throws Exception {
        final var ok = okReply();
        server.respond(n -> n == 1 ? Reply.status(429, "{\"detail\":\"slow down\"}") : ok);

        try (var service = service(Map.of())) {
            assertThat(service.decide(request()).answers()).hasSize(3);
        }
        assertThat(server.requestCount()).isEqualTo(2);
    }

    @Test
    public void shouldHonourRetryAfter() throws Exception {
        final var ok = okReply();
        server.respond(n -> n == 1 ? Reply.status(429, "{}").withHeader("Retry-After", "1") : ok);

        final var started = System.nanoTime();
        try (var service = service(Map.of("jev.retry.max.delay.ms", "5000"))) {
            service.decide(request());
        }
        final var elapsed = Duration.ofNanos(System.nanoTime() - started);

        assertThat(server.requestCount()).isEqualTo(2);
        assertThat(elapsed).isGreaterThanOrEqualTo(Duration.ofMillis(900));
    }

    @Test
    public void shouldUseConfiguredBackoffForNegativeRetryAfter() throws IOException {
        final var ok = okReply();
        server.respond(n -> n == 1 ? Reply.status(429, "{}").withHeader("Retry-After", "-1") : ok);

        try (var service = service(Map.of("jev.retry.initial.delay.ms", "250", "jev.retry.max.delay.ms", "250"))) {
            final var started = System.nanoTime();
            service.decide(request());
            assertThat(Duration.ofNanos(System.nanoTime() - started)).isGreaterThanOrEqualTo(Duration.ofMillis(250));
        }
        assertThat(server.requestCount()).isEqualTo(2);
    }

    @Test
    public void shouldCapRetryAfterByMaximumDelay() throws Exception {
        final var ok = okReply();
        server.respond(n -> n == 1 ? Reply.status(429, "{}").withHeader("Retry-After", "3600") : ok);

        final var started = System.nanoTime();
        try (var service = service(Map.of("jev.retry.max.delay.ms", "20"))) {
            service.decide(request());
        }
        final var elapsed = Duration.ofNanos(System.nanoTime() - started);

        assertThat(server.requestCount()).isEqualTo(2);
        assertThat(elapsed).isLessThan(Duration.ofSeconds(10));
    }

    @Test
    public void shouldFailWithRetriableExceptionAfterAllAttemptsOnOverload() {
        for (final var status : List.of(529, 500, 503)) {
            final var before = server.requestCount();
            server.respondAlways(Reply.status(status, "{\"detail\":\"busy\"}"));

            try (var service = service(Map.of("jev.retry.max.attempts", "3"))) {
                assertThatThrownBy(() -> service.decide(request()))
                        .isInstanceOfSatisfying(DecisionException.class, e -> {
                            assertThat(e.isRetriable()).isTrue();
                            assertThat(e.httpStatus()).isEqualTo(status);
                        })
                        .hasMessageContaining("HTTP status " + status)
                        .hasMessageContaining("busy")
                        .hasMessageEndingWith("(after 3 attempts)");
            }
            assertThat(server.requestCount() - before).isEqualTo(3);
        }
    }

    @Test
    public void shouldMakeASingleAttemptWhenConfigured() {
        server.respondAlways(Reply.status(500, "{}"));

        try (var service = service(Map.of("jev.retry.max.attempts", "1"))) {
            assertThatThrownBy(() -> service.decide(request()))
                    .isInstanceOfSatisfying(DecisionException.class, e -> assertThat(e.isRetriable()).isTrue())
                    .hasMessageEndingWith("(after 1 attempts)");
        }
        assertThat(server.requestCount()).isEqualTo(1);
    }

    @Test
    public void shouldRetryWhenConnectionIsRefused() throws Exception {
        final var deadUrl = server.baseUrl();
        server.close();

        try (var service = new SystemOneDecisionService(settings(deadUrl, Map.of("jev.retry.max.attempts", "2")))) {
            assertThatThrownBy(() -> service.decide(request()))
                    .isInstanceOfSatisfying(DecisionException.class, e -> assertThat(e.isRetriable()).isTrue())
                    .hasMessageStartingWith("Jev request failed:")
                    .hasMessageEndingWith("(after 2 attempts)")
                    .hasMessageNotContaining(API_KEY);
        }
        // the server is closed again in tearDown
        server = new FakeJevServer();
    }

    @Test
    public void shouldTimeOutOnSlowResponse() throws Exception {
        server.respondAlways(okReply().withDelay(2_000));

        final var started = System.nanoTime();
        try (var service = service(Map.of("jev.request.timeout.ms", "200", "jev.retry.max.attempts", "2"))) {
            assertThatThrownBy(() -> service.decide(request()))
                    .isInstanceOfSatisfying(DecisionException.class, e -> assertThat(e.isRetriable()).isTrue())
                    .hasMessageContaining("Timeout")
                    .hasMessageEndingWith("(after 2 attempts)");
        }
        assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofSeconds(10));
        assertThat(server.requestCount()).isEqualTo(2);
    }

    @Test
    public void shouldNotRetryMalformedSuccessfulResponse() {
        server.respondAlways(Reply.ok("{\"model\": \"jev-latest\", \"answers\": "));

        try (var service = service(Map.of())) {
            assertThatThrownBy(() -> service.decide(request()))
                    .isInstanceOfSatisfying(DecisionException.class, e -> assertThat(e.isRetriable()).isFalse())
                    .hasMessageContaining("Invalid System One response");
        }
        assertThat(server.requestCount()).isEqualTo(1);
    }

    @Test
    public void shouldNotRetryResponseMissingAnAnswer() {
        server.respondAlways(Reply.ok("{\"model\": \"jev-latest\", \"answers\": {}}"));

        try (var service = service(Map.of())) {
            assertThatThrownBy(() -> service.decide(request()))
                    .isInstanceOfSatisfying(DecisionException.class, e -> assertThat(e.isRetriable()).isFalse())
                    .hasMessageContaining("missing answer");
        }
        assertThat(server.requestCount()).isEqualTo(1);
    }

    @Test
    public void shouldStopWhenInterruptedDuringBackoff() throws Exception {
        server.respondAlways(Reply.status(503, "{}"));

        try (var service = service(Map.of("jev.retry.initial.delay.ms", "60000", "jev.retry.max.delay.ms", "60000"))) {
            final var caller = Thread.currentThread();
            final var interrupter = new Thread(() -> {
                try {
                    Thread.sleep(500);
                }
                catch (InterruptedException e) {
                    return;
                }
                caller.interrupt();
            });
            interrupter.start();

            final var started = System.nanoTime();
            assertThatThrownBy(() -> service.decide(request()))
                    .isInstanceOfSatisfying(DecisionException.class, e -> assertThat(e.isRetriable()).isFalse())
                    .hasMessageContaining("Interrupted");
            assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofSeconds(30));
            // the interrupt flag must be restored for the caller, clear it for the other tests
            assertThat(Thread.interrupted()).isTrue();
            interrupter.join();
        }
        assertThat(server.requestCount()).isEqualTo(1);
    }

    @Test
    public void shouldStopWhenInterruptedBeforeTheCall() throws Exception {
        server.respondAlways(okReply());

        try (var service = service(Map.of())) {
            Thread.currentThread().interrupt();
            assertThatThrownBy(() -> service.decide(request()))
                    .isInstanceOfSatisfying(DecisionException.class, e -> assertThat(e.isRetriable()).isFalse())
                    .hasMessageContaining("Interrupted");
            assertThat(Thread.interrupted()).isTrue();
        }
    }

    @Test
    public void shouldServeConcurrentCalls() throws Exception {
        server.respondAlways(okReply());
        final var threads = 8;
        final var callsPerThread = 5;

        try (var service = service(Map.of()); var executor = Executors.newFixedThreadPool(threads)) {
            final List<Callable<Integer>> tasks = new ArrayList<>();
            for (var i = 0; i < threads * callsPerThread; i++) {
                tasks.add(() -> service.decide(request()).answers().size());
            }
            for (final var future : executor.invokeAll(tasks)) {
                assertThat(future.get()).isEqualTo(3);
            }
        }
        assertThat(server.requestCount()).isEqualTo(threads * callsPerThread);
    }

    @Test
    public void shouldNeverExposeTheKeyInToString() {
        try (var service = service(Map.of())) {
            assertThat(service.toString()).doesNotContain(API_KEY).contains("endpoint=" + URI.create(server.baseUrl() + "/v1/systemone"));
        }
    }
}
