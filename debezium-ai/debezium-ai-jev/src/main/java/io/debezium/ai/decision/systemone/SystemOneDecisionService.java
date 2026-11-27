/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.decision.systemone;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.ai.decision.api.DecisionException;
import io.debezium.ai.decision.api.DecisionRequest;
import io.debezium.ai.decision.api.DecisionResponse;
import io.debezium.ai.decision.api.DecisionService;

/**
 * {@link DecisionService} calling a System One {@code POST /v1/systemone} endpoint.
 * One call answers all the questions of the request. The HTTP status 429, 529 and 5xx and the I/O failures
 * are retried with an exponential backoff, all the other failures are final.
 * The API key is never part of any message, log or {@link #toString()}.
 * The service is safe for use from multiple threads.
 *
 * @author Jiri Pechanec
 */
class SystemOneDecisionService implements DecisionService {

    private static final Logger LOGGER = LoggerFactory.getLogger(SystemOneDecisionService.class);

    static final String LONG_CONTEXT_HEADER = "TypeSafe-Danger-Zone-Long-Context";
    static final String USER_AGENT = "debezium-ai-jev";

    private static final int MAX_DETAIL_LENGTH = 300;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final SystemOneSettings settings;
    private final SystemOneRequestMapper requestMapper;
    private final HttpClient client;

    SystemOneDecisionService(final SystemOneSettings settings) {
        this.settings = settings;
        this.requestMapper = new SystemOneRequestMapper(settings.model(), settings.provider(), settings.maxScoreLevels());
        this.client = HttpClient.newBuilder()
                .connectTimeout(settings.connectTimeout())
                .build();
    }

    @Override
    public DecisionResponse decide(final DecisionRequest request) throws DecisionException {
        final var body = requestMapper.toJson(request);
        DecisionException last = null;
        for (var attempt = 1; attempt <= settings.maxAttempts(); attempt++) {
            Duration retryAfter = null;
            try {
                final var response = client.send(httpRequest(body), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
                final var status = response.statusCode();
                if (status == 200) {
                    return SystemOneResponseParser.parse(response.body(), request.questions());
                }
                last = failure(status, response.body());
                if (!last.isRetriable()) {
                    throw last;
                }
                retryAfter = retryAfter(response);
            }
            catch (IOException e) {
                // timeouts are IOExceptions too, the message of the exception is deliberately not propagated
                last = new DecisionException(settings.provider() + " request failed: " + e.getClass().getSimpleName(), e, true);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new DecisionException("Interrupted while waiting for " + settings.provider(), e, false);
            }

            if (attempt < settings.maxAttempts()) {
                final var delay = delay(attempt, retryAfter);
                LOGGER.debug("{} request attempt {} of {} failed ({}), retrying in {} ms", settings.provider(), attempt, settings.maxAttempts(), last.getMessage(),
                        delay.toMillis());
                sleep(delay);
            }
        }
        throw new DecisionException(last.getMessage() + " (after " + settings.maxAttempts() + " attempts)", last, true, last.httpStatus());
    }

    @Override
    public void close() {
        client.close();
    }

    @Override
    public String toString() {
        return "SystemOneDecisionService[" + settings + "]";
    }

    private HttpRequest httpRequest(final String body) {
        final var builder = HttpRequest.newBuilder(settings.endpoint())
                .timeout(settings.requestTimeout())
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .header("User-Agent", USER_AGENT)
                .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8));
        if (settings.apiKey() != null) {
            builder.header("Authorization", "Bearer " + settings.apiKey());
        }
        if (settings.longContext()) {
            builder.header(LONG_CONTEXT_HEADER, "true");
        }
        return builder.build();
    }

    private Duration delay(final int attempt, final Duration retryAfter) {
        final var max = settings.maxDelay();
        if (retryAfter != null) {
            return retryAfter.compareTo(max) > 0 ? max : retryAfter;
        }
        final var multiplier = 1L << Math.min(attempt - 1, 20);
        final var backoff = settings.initialDelay().multipliedBy(multiplier);
        return backoff.compareTo(max) > 0 ? max : backoff;
    }

    private static Duration retryAfter(final HttpResponse<String> response) {
        return response.headers().firstValue("Retry-After").map(value -> {
            try {
                final var seconds = Long.parseLong(value.trim());
                return seconds < 0 ? null : Duration.ofSeconds(seconds);
            }
            catch (NumberFormatException e) {
                // HTTP date form is not supported, the exponential backoff is used
                return null;
            }
        }).orElse(null);
    }

    private void sleep(final Duration delay) {
        try {
            Thread.sleep(delay);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DecisionException("Interrupted while waiting for " + settings.provider(), e, false);
        }
    }

    private DecisionException failure(final int status, final String body) {
        final var retriable = status == 429 || status == 529 || status >= 500;
        final var detail = detail(body, settings.apiKey());
        final var message = settings.provider() + " request failed with HTTP status " + status + (detail.isEmpty() ? "" : ": " + detail);
        return new DecisionException(message, retriable, status);
    }

    /**
     * Extracts the {@code detail} of the error body. Only the text of the detail or the messages of the validation
     * errors are used, as the validation errors may echo the submitted state.
     */
    private static String detail(final String body, final String apiKey) {
        if (body == null || body.isBlank()) {
            return "";
        }
        try {
            final var root = MAPPER.readTree(body);
            if (root == null || !root.isObject()) {
                return "";
            }
            final var detail = root.path("detail");
            final var text = new StringBuilder();
            if (detail.isTextual()) {
                text.append(detail.asText());
            }
            else if (detail.isArray()) {
                for (final var item : detail) {
                    final JsonNode message = item.path("msg");
                    if (message.isTextual()) {
                        text.append(text.isEmpty() ? "" : "; ").append(message.asText());
                    }
                }
            }
            final var redacted = apiKey == null ? text.toString() : text.toString().replace(apiKey, "***");
            final var singleLine = redacted.replaceAll("\\p{Cntrl}+", " ").trim();
            return singleLine.length() > MAX_DETAIL_LENGTH ? singleLine.substring(0, MAX_DETAIL_LENGTH) : singleLine;
        }
        catch (IOException e) {
            return "";
        }
    }
}
