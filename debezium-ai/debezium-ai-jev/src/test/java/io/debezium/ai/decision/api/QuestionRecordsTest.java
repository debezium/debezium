/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.decision.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SequencedMap;

import org.junit.jupiter.api.Test;

/**
 * Tests of the records of the provider neutral decision API.
 *
 * @author Jiri Pechanec
 */
public class QuestionRecordsTest {

    private static SequencedMap<String, String> options(final String... names) {
        final SequencedMap<String, String> options = new LinkedHashMap<>();
        for (final var name : names) {
            options.put(name, "");
        }
        return options;
    }

    @Test
    public void shouldAcceptValidQuestionNames() {
        assertThat(Question.isValidName("a")).isTrue();
        assertThat(Question.isValidName("Category_1-x")).isTrue();
        assertThat(Question.isValidName("a".repeat(64))).isTrue();
    }

    @Test
    public void shouldRejectInvalidQuestionNames() {
        assertThat(Question.isValidName(null)).isFalse();
        assertThat(Question.isValidName("")).isFalse();
        assertThat(Question.isValidName("1abc")).isFalse();
        assertThat(Question.isValidName("a.b")).isFalse();
        assertThat(Question.isValidName("a b")).isFalse();
        assertThat(Question.isValidName("a".repeat(65))).isFalse();
        assertThatThrownBy(() -> new YesNoQuestion("a.b", "text", null, null)).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("a.b");
    }

    @Test
    public void shouldRejectBlankInstructions() {
        assertThatThrownBy(() -> new YesNoQuestion("q", " ", null, null)).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("instructions");
        assertThatThrownBy(() -> new ChoiceQuestion("q", null, options("a"))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ScoreQuestion("q", "", List.of("a", "b"))).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void shouldRejectChoiceWithoutOptions() {
        assertThatThrownBy(() -> new ChoiceQuestion("q", "text", new LinkedHashMap<>())).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least one option");
    }

    @Test
    public void shouldRejectInvalidOptionName() {
        assertThatThrownBy(() -> new ChoiceQuestion("q", "text", options("a b"))).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("a b");
    }

    @Test
    public void shouldKeepOptionOrderAndMakeImmutableCopy() {
        final var source = options("zeta", "alpha", "mid");
        final var question = new ChoiceQuestion("q", "text", source);

        source.put("late", "");

        assertThat(question.options().keySet()).containsExactly("zeta", "alpha", "mid");
        assertThatThrownBy(() -> question.options().put("x", "y")).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void shouldRequireAtLeastTwoScoreLevels() {
        assertThatThrownBy(() -> new ScoreQuestion("q", "text", List.of("only"))).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("at least 2");
        assertThatThrownBy(() -> new ScoreQuestion("q", "text", List.of("a", " "))).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void shouldCopyScoreLevels() {
        final List<String> levels = new ArrayList<>(List.of("low", "high"));
        final var question = new ScoreQuestion("q", "text", levels);

        levels.add("extra");

        assertThat(question.levels()).containsExactly("low", "high");
        assertThatThrownBy(() -> question.levels().add("x")).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void shouldAllowMissingYesNoDescriptions() {
        final var question = new YesNoQuestion("q", "text", null, null);

        assertThat(question.yesDescription()).isNull();
        assertThat(question.noDescription()).isNull();
    }

    @Test
    public void shouldRejectEmptyOrDuplicateQuestions() {
        final var question = new YesNoQuestion("q", "text", null, null);

        assertThatThrownBy(() -> new DecisionRequest("state", List.of())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DecisionRequest("state", List.of(question, question))).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Duplicate");
        assertThatThrownBy(() -> new DecisionRequest(null, List.of(question))).isInstanceOf(NullPointerException.class);
    }

    @Test
    public void shouldValidateAnswerRanges() {
        assertThatThrownBy(() -> new YesNoAnswer("q", 1.1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new YesNoAnswer("q", -0.1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new YesNoAnswer("q", Double.NaN)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ChoiceAnswer("q", "a", 1.5, Map.of("a", 1.0))).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("confidence");
        assertThatThrownBy(() -> new ChoiceAnswer("q", "a", 0.5, Map.of("a", 2.0))).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("option 'a'");
        assertThatThrownBy(() -> new ScoreAnswer("q", Double.POSITIVE_INFINITY, 0.5, Map.of())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ScoreAnswer("q", 1.0, -0.5, Map.of())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ScoreAnswer("q", 1.0, 0.5, Map.of(0, 1.5))).isInstanceOf(IllegalArgumentException.class);
        assertThat(new YesNoAnswer("q", 0.0).probability()).isZero();
        assertThat(new YesNoAnswer("q", 1.0).probability()).isEqualTo(1.0);
    }

    @Test
    public void shouldKeepProbabilityOrder() {
        final Map<String, Double> probabilities = new LinkedHashMap<>();
        probabilities.put("z", 0.1);
        probabilities.put("a", 0.9);

        final var answer = new ChoiceAnswer("q", "a", 0.5, probabilities);

        assertThat(answer.probabilities().keySet()).containsExactly("z", "a");
        assertThat(new ScoreAnswer("q", 1.0, 0.5, Map.of(2, 0.2, 0, 0.5, 1, 0.3)).probabilities().keySet()).containsExactly(0, 1, 2);
    }

    @Test
    public void shouldFlagDecisionException() {
        final var plain = new DecisionException("failed", false);
        final var http = new DecisionException("limited", true, 429);
        final var cause = new RuntimeException("cause");
        final var wrapped = new DecisionException("wrapped", cause, true);

        assertThat(plain.isRetriable()).isFalse();
        assertThat(plain.httpStatus()).isZero();
        assertThat(http.isRetriable()).isTrue();
        assertThat(http.httpStatus()).isEqualTo(429);
        assertThat(wrapped.getCause()).isSameAs(cause);
        assertThat(wrapped.httpStatus()).isZero();
    }

    @Test
    public void shouldValidateUsageAndResponse() {
        assertThatThrownBy(() -> new Usage(-1, 0)).isInstanceOf(IllegalArgumentException.class);
        final var response = new DecisionResponse("model", Map.of("q", new YesNoAnswer("q", 0.5)), new Usage(1, 2));

        assertThat(response.answers()).containsKey("q");
        assertThatThrownBy(() -> response.answers().clear()).isInstanceOf(UnsupportedOperationException.class);
    }
}
