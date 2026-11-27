/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.decision.systemone;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SequencedMap;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.ai.decision.api.ChoiceQuestion;
import io.debezium.ai.decision.api.DecisionException;
import io.debezium.ai.decision.api.DecisionRequest;
import io.debezium.ai.decision.api.Question;
import io.debezium.ai.decision.api.ScoreQuestion;
import io.debezium.ai.decision.api.YesNoQuestion;

/**
 * Tests of {@link SystemOneRequestMapper}.
 *
 * @author Jiri Pechanec
 */
public class SystemOneRequestMapperTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    static JsonNode fixture(final String name) throws IOException {
        try (InputStream stream = SystemOneRequestMapperTest.class.getResourceAsStream("/jev/" + name)) {
            assertThat(stream).as("fixture %s", name).isNotNull();
            return MAPPER.readTree(stream);
        }
    }

    private static JsonNode map(final DecisionRequest request) throws IOException {
        return MAPPER.readTree(new SystemOneRequestMapper("jev-latest", "Jev", 10).toJson(request));
    }

    static ChoiceQuestion categoryQuestion() {
        final SequencedMap<String, String> options = new LinkedHashMap<>();
        options.put("MARKETING", "Promotion and positioning");
        options.put("SAFETY", "Hazards and recalls");
        options.put("SHIPPING", "Logistics and delivery");
        return new ChoiceQuestion("category", "Which team should handle this product change?", options);
    }

    static ScoreQuestion severityQuestion() {
        return new ScoreQuestion("severity", "How severe is the issue described by the product?", List.of("Cosmetic", "Needs attention", "Dangerous"));
    }

    static YesNoQuestion emergencyQuestion() {
        return new YesNoQuestion("emergency", "Does the product require immediate intervention?", null, null);
    }

    static Map<String, Object> mixedState() {
        final Map<String, Object> state = new LinkedHashMap<>();
        state.put("after.name", "Space heater");
        state.put("after.description", "Heats a room, may overheat");
        return state;
    }

    @Test
    public void shouldMapChoice() throws Exception {
        final SequencedMap<String, String> options = new LinkedHashMap<>();
        options.put("angry", "The customer is upset");
        options.put("neutral", "");
        final var request = new DecisionRequest("I was charged twice", List.of(new ChoiceQuestion("tone", "What is the tone of the message?", options)));

        assertThat(map(request)).isEqualTo(fixture("request-choice.json"));
    }

    @Test
    public void shouldMapScore() throws Exception {
        final var request = new DecisionRequest(List.of("first", "second"),
                List.of(new ScoreQuestion("urgency", "How urgent is the request?", List.of("Not urgent", "Somewhat urgent", "Very urgent"))));

        assertThat(map(request)).isEqualTo(fixture("request-score.json"));
    }

    @Test
    public void shouldMapYesNoToNoulWithDescriptions() throws Exception {
        final var request = new DecisionRequest(Map.of("message", "I was charged twice, please refund"),
                List.of(new YesNoQuestion("billing", "Is this a billing problem?", "A payment is involved", "No payment is involved")));

        assertThat(map(request)).isEqualTo(fixture("request-noul.json"));
    }

    @Test
    public void shouldMapMixedRequestAndOmitEmptyNoulCriteria() throws Exception {
        final var request = new DecisionRequest(mixedState(), List.of(categoryQuestion(), severityQuestion(), emergencyQuestion()));

        final var json = map(request);

        assertThat(json).isEqualTo(fixture("request-mixed.json"));
        assertThat(json.at("/questions/emergency").has("criteria")).isFalse();
    }

    @Test
    public void shouldKeepOptionAndLevelOrder() throws Exception {
        final SequencedMap<String, String> options = new LinkedHashMap<>();
        options.put("zeta", "z");
        options.put("alpha", "a");
        options.put("mid", "m");
        final var request = new DecisionRequest("state", List.of(new ChoiceQuestion("q", "text", options),
                new ScoreQuestion("s", "text", List.of("third", "first", "second"))));

        final var json = new SystemOneRequestMapper("m", "Jev", 10).toJson(request);

        assertThat(json.indexOf("zeta")).isLessThan(json.indexOf("alpha"));
        assertThat(json.indexOf("alpha")).isLessThan(json.indexOf("mid"));
        assertThat(json.indexOf("third")).isLessThan(json.indexOf("first"));
        assertThat(json.indexOf("first")).isLessThan(json.indexOf("second"));
    }

    @Test
    public void shouldMapScalarsAndNullInState() throws Exception {
        final Map<String, Object> state = new LinkedHashMap<>();
        state.put("int", 1);
        state.put("long", 2L);
        state.put("double", 1.5d);
        state.put("bool", true);
        state.put("nothing", null);
        state.put("nested", Map.of("list", List.of(1, "two")));

        final var json = map(new DecisionRequest(state, List.of(emergencyQuestion()))).get("state");

        assertThat(json.get("int").asInt()).isEqualTo(1);
        assertThat(json.get("long").asLong()).isEqualTo(2L);
        assertThat(json.get("double").asDouble()).isEqualTo(1.5d);
        assertThat(json.get("bool").asBoolean()).isTrue();
        assertThat(json.get("nothing").isNull()).isTrue();
        assertThat(json.at("/nested/list/1").asText()).isEqualTo("two");
    }

    @Test
    public void shouldRejectNonFiniteNumbers() {
        final var mapper = new SystemOneRequestMapper("m", "Jev", 10);

        assertThatThrownBy(() -> mapper.toJson(new DecisionRequest(Map.of("x", Double.NaN), List.of(emergencyQuestion()))))
                .isInstanceOf(DecisionException.class).hasMessageContaining("non finite").matches(e -> !((DecisionException) e).isRetriable());
        assertThatThrownBy(() -> mapper.toJson(new DecisionRequest(Map.of("x", Float.POSITIVE_INFINITY), List.of(emergencyQuestion()))))
                .isInstanceOf(DecisionException.class);
    }

    @Test
    public void shouldRejectTooDeepState() {
        Object state = "leaf";
        for (var i = 0; i < SystemOneRequestMapper.MAX_JSON_NESTING + 1; i++) {
            state = Map.of("k", state);
        }
        final var request = new DecisionRequest(state, List.of(emergencyQuestion()));

        assertThatThrownBy(() -> new SystemOneRequestMapper("m", "Jev", 10).toJson(request)).isInstanceOf(DecisionException.class).hasMessageContaining("nesting");
    }

    @Test
    public void shouldAcceptStateAtTheNestingLimit() {
        // the body is the first level, so there are 62 levels of nested maps left and the leaf
        Object state = "leaf";
        for (var i = 0; i < SystemOneRequestMapper.MAX_JSON_NESTING - 2; i++) {
            state = Map.of("k", state);
        }
        final var request = new DecisionRequest(state, List.of(emergencyQuestion()));

        assertThat(new SystemOneRequestMapper("m", "Jev", 10).toJson(request)).contains("leaf");
    }

    @Test
    public void shouldRejectUnsupportedStateValue() {
        assertThatThrownBy(() -> new SystemOneRequestMapper("m", "Jev", 10).toJson(new DecisionRequest(new Object[]{ 1 }, List.of(emergencyQuestion()))))
                .isInstanceOf(DecisionException.class).hasMessageContaining("unsupported");
        assertThatThrownBy(() -> new SystemOneRequestMapper("m", "Jev", 10).toJson(new DecisionRequest(Map.of(1, "x"), List.of(emergencyQuestion()))))
                .isInstanceOf(DecisionException.class).hasMessageContaining("non-string key");
    }

    @Test
    public void shouldReportLimitViolations() {
        final SequencedMap<String, String> options = new LinkedHashMap<>();
        for (var i = 0; i <= SystemOneRequestMapper.MAX_CHOICE_OPTIONS; i++) {
            options.put("o" + i, "");
        }
        final List<String> levels = new ArrayList<>();
        for (var i = 0; i < 11; i++) {
            levels.add("level " + i);
        }
        final Question tooManyOptions = new ChoiceQuestion("c", "text", options);
        final Question tooManyLevels = new ScoreQuestion("s", "text", levels);

        assertThat(SystemOneRequestMapper.limitViolation(tooManyOptions, "Jev", 10)).contains("255");
        assertThat(SystemOneRequestMapper.limitViolation(tooManyLevels, "Jev", 10)).contains("10");
        assertThat(SystemOneRequestMapper.limitViolation(emergencyQuestion(), "Jev", 10)).isNull();
        assertThatThrownBy(() -> new SystemOneRequestMapper("m", "Jev", 10).toJson(new DecisionRequest("state", List.of(tooManyLevels))))
                .isInstanceOf(DecisionException.class)
                .matches(e -> !((DecisionException) e).isRetriable());
    }
}
