/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.decision;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SequencedMap;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.junit.jupiter.api.Test;

import io.debezium.ai.decision.api.Answer;
import io.debezium.ai.decision.api.ChoiceAnswer;
import io.debezium.ai.decision.api.ChoiceQuestion;
import io.debezium.ai.decision.api.DecisionResponse;
import io.debezium.ai.decision.api.Question;
import io.debezium.ai.decision.api.ScoreAnswer;
import io.debezium.ai.decision.api.ScoreQuestion;
import io.debezium.ai.decision.api.Usage;
import io.debezium.ai.decision.api.YesNoAnswer;
import io.debezium.ai.decision.api.YesNoQuestion;

/**
 * Tests of {@link DecisionHeaderWriter}.
 *
 * @author Jiri Pechanec
 */
public class DecisionHeaderWriterTest {

    private static final String PREFIX = "__debezium.ai.decision.";

    private static final List<Question> QUESTIONS = List.of(
            new ChoiceQuestion("category", "text", options()),
            new ScoreQuestion("severity", "text", List.of("low", "medium", "high")),
            new YesNoQuestion("emergency", "text", null, null));

    private static SequencedMap<String, String> options() {
        final SequencedMap<String, String> options = new LinkedHashMap<>();
        options.put("SHIPPING", "");
        options.put("SAFETY", "");
        options.put("MARKETING", "");
        return options;
    }

    private static DecisionResponse response() {
        final Map<String, Answer> answers = new LinkedHashMap<>();
        // probabilities deliberately not in the configured option order
        final Map<String, Double> probabilities = new LinkedHashMap<>();
        probabilities.put("MARKETING", 0.05);
        probabilities.put("SAFETY", 0.9);
        probabilities.put("SHIPPING", 0.05);
        answers.put("category", new ChoiceAnswer("category", "SAFETY", 0.8, probabilities));
        answers.put("severity", new ScoreAnswer("severity", 1.25, 0.6, Map.of(0, 0.25, 1, 0.25, 2, 0.5)));
        answers.put("emergency", new YesNoAnswer("emergency", 0.15));
        return new DecisionResponse("jev-1.13.0", answers, new Usage(10, 2));
    }

    @Test
    public void shouldWriteAllHeadersWithTypes() {
        final var headers = new DecisionHeaderWriter(QUESTIONS, true).success(new ConnectHeaders(), response());

        assertThat(headers.lastWithName(PREFIX + "status").schema()).isSameAs(Schema.STRING_SCHEMA);
        assertThat(headers.lastWithName(PREFIX + "status").value()).isEqualTo("SUCCESS");
        assertThat(headers.lastWithName(PREFIX + "model").value()).isEqualTo("jev-1.13.0");

        assertThat(headers.lastWithName(PREFIX + "category").schema()).isSameAs(Schema.STRING_SCHEMA);
        assertThat(headers.lastWithName(PREFIX + "category").value()).isEqualTo("SAFETY");
        assertThat(headers.lastWithName(PREFIX + "category.confidence").schema()).isSameAs(Schema.FLOAT64_SCHEMA);
        assertThat(headers.lastWithName(PREFIX + "category.confidence").value()).isEqualTo(0.8);
        assertThat(headers.lastWithName(PREFIX + "category.probabilities").schema()).isSameAs(Schema.STRING_SCHEMA);

        assertThat(headers.lastWithName(PREFIX + "severity").schema()).isSameAs(Schema.FLOAT64_SCHEMA);
        assertThat(headers.lastWithName(PREFIX + "severity").value()).isEqualTo(1.25);
        assertThat(headers.lastWithName(PREFIX + "severity.confidence").value()).isEqualTo(0.6);
        assertThat(headers.lastWithName(PREFIX + "severity.probabilities").schema()).isSameAs(Schema.STRING_SCHEMA);

        assertThat(headers.lastWithName(PREFIX + "emergency").schema()).isSameAs(Schema.FLOAT64_SCHEMA);
        assertThat(headers.lastWithName(PREFIX + "emergency").value()).isEqualTo(0.15);
        assertThat(headers.lastWithName(PREFIX + "emergency.confidence")).isNull();
        assertThat(headers.lastWithName(PREFIX + "emergency.probabilities")).isNull();
    }

    @Test
    public void shouldWriteProbabilitiesAsJsonInConfiguredOrder() {
        final var headers = new DecisionHeaderWriter(QUESTIONS, true).success(new ConnectHeaders(), response());

        assertThat(headers.lastWithName(PREFIX + "category.probabilities").value()).isEqualTo("{\"SHIPPING\":0.05,\"SAFETY\":0.9,\"MARKETING\":0.05}");
        assertThat(headers.lastWithName(PREFIX + "severity.probabilities").value()).isEqualTo("{\"0\":0.25,\"1\":0.25,\"2\":0.5}");
    }

    @Test
    public void shouldSkipDetailsWhenDisabled() {
        final var headers = new DecisionHeaderWriter(QUESTIONS, false).success(new ConnectHeaders(), response());

        assertThat(headers.lastWithName(PREFIX + "category").value()).isEqualTo("SAFETY");
        assertThat(headers.lastWithName(PREFIX + "severity").value()).isEqualTo(1.25);
        assertThat(headers.lastWithName(PREFIX + "emergency").value()).isEqualTo(0.15);
        assertThat(headers.lastWithName(PREFIX + "category.confidence")).isNull();
        assertThat(headers.lastWithName(PREFIX + "category.probabilities")).isNull();
        assertThat(headers.lastWithName(PREFIX + "severity.confidence")).isNull();
        assertThat(headers.lastWithName(PREFIX + "severity.probabilities")).isNull();
    }

    @Test
    public void shouldWriteErrorHeaders() {
        final var headers = new DecisionHeaderWriter(QUESTIONS, true).error(new ConnectHeaders(), "Jev request failed with HTTP status 401");

        assertThat(headers.lastWithName(PREFIX + "status").value()).isEqualTo("ERROR");
        assertThat(headers.lastWithName(PREFIX + "error").value()).isEqualTo("Jev request failed with HTTP status 401");
        assertThat(headers.lastWithName(PREFIX + "model")).isNull();
        assertThat(headers.lastWithName(PREFIX + "category")).isNull();
    }

    @Test
    public void shouldSanitizeErrorMessage() {
        assertThat(DecisionHeaderWriter.sanitize("line one\nline two\t end")).isEqualTo("line one line two end");
        assertThat(DecisionHeaderWriter.sanitize(null)).isEqualTo("Unknown error");
        assertThat(DecisionHeaderWriter.sanitize("  ")).isEqualTo("Unknown error");
        assertThat(DecisionHeaderWriter.sanitize("x".repeat(2000))).hasSize(DecisionHeaderWriter.MAX_ERROR_LENGTH);
    }

    @Test
    public void shouldReplaceExistingHeadersOfTheSameName() {
        final var original = new ConnectHeaders();
        original.addString(PREFIX + "status", "ERROR");
        original.addString(PREFIX + "error", "old failure");
        original.addString(PREFIX + "category", "OLD");
        original.addDouble(PREFIX + "category.confidence", 0.1);
        original.addString(PREFIX + "category", "OLDER");

        final var headers = new DecisionHeaderWriter(QUESTIONS, true).success(original, response());

        assertThat(headers.allWithName(PREFIX + "status")).toIterable().hasSize(1);
        assertThat(headers.lastWithName(PREFIX + "status").value()).isEqualTo("SUCCESS");
        assertThat(headers.lastWithName(PREFIX + "error")).isNull();
        assertThat(headers.allWithName(PREFIX + "category")).toIterable().hasSize(1);
        assertThat(headers.lastWithName(PREFIX + "category").value()).isEqualTo("SAFETY");
        assertThat(headers.allWithName(PREFIX + "category.confidence")).toIterable().hasSize(1);
    }

    @Test
    public void shouldBeIdempotent() {
        final var writer = new DecisionHeaderWriter(QUESTIONS, true);

        final var once = writer.success(new ConnectHeaders(), response());
        final var twice = writer.success(once, response());

        assertThat(twice.size()).isEqualTo(once.size());
    }

    @Test
    public void shouldPreserveForeignHeaders() {
        final var original = new ConnectHeaders();
        original.addString("foreign", "value");
        original.addString("__debezium.context.taskId", "0");

        final var headers = new DecisionHeaderWriter(QUESTIONS, true).success(original, response());

        assertThat(headers.lastWithName("foreign").value()).isEqualTo("value");
        assertThat(headers.lastWithName("__debezium.context.taskId").value()).isEqualTo("0");
    }

    @Test
    public void shouldNotMutateOriginalHeaders() {
        final var original = new ConnectHeaders();
        original.addString("foreign", "value");
        original.addString(PREFIX + "category", "OLD");
        final var writer = new DecisionHeaderWriter(QUESTIONS, true);

        final Headers success = writer.success(original, response());
        final Headers error = writer.error(original, "failure");

        assertThat(success).isNotSameAs(original);
        assertThat(error).isNotSameAs(original);
        assertThat(original.size()).isEqualTo(2);
        assertThat(original.lastWithName(PREFIX + "category").value()).isEqualTo("OLD");
        assertThat(original.lastWithName(PREFIX + "status")).isNull();
    }

    @Test
    public void shouldOnlyWriteHeadersInTheDecisionNamespace() {
        final var original = new ConnectHeaders();
        original.addString("foreign", "value");

        final var headers = new DecisionHeaderWriter(QUESTIONS, true).success(original, response());

        for (final var header : headers) {
            assertThat(header.key()).matches(key -> key.equals("foreign") || key.startsWith(PREFIX));
        }
    }
}
