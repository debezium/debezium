/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.decision.systemone;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.SequencedMap;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.node.ObjectNode;

import io.debezium.ai.decision.api.ChoiceAnswer;
import io.debezium.ai.decision.api.ChoiceQuestion;
import io.debezium.ai.decision.api.DecisionException;
import io.debezium.ai.decision.api.Question;
import io.debezium.ai.decision.api.ScoreAnswer;
import io.debezium.ai.decision.api.ScoreQuestion;
import io.debezium.ai.decision.api.Usage;
import io.debezium.ai.decision.api.YesNoAnswer;
import io.debezium.ai.decision.api.YesNoQuestion;

/**
 * Tests of {@link SystemOneResponseParser}.
 *
 * @author Jiri Pechanec
 */
public class SystemOneResponseParserTest {

    private static final List<Question> ALL = List.of(SystemOneRequestMapperTest.categoryQuestion(), SystemOneRequestMapperTest.severityQuestion(),
            SystemOneRequestMapperTest.emergencyQuestion());

    private static ObjectNode mixed() throws IOException {
        return (ObjectNode) SystemOneRequestMapperTest.fixture("response-mixed.json");
    }

    private static DecisionException failure(final String body) {
        final var thrown = assertThrows(DecisionException.class, () -> SystemOneResponseParser.parse(body, ALL));
        assertThat(thrown.isRetriable()).isFalse();
        return thrown;
    }

    @Test
    public void shouldParseAllAnswerTypes() throws Exception {
        final var response = SystemOneResponseParser.parse(mixed().toString(), ALL);

        assertThat(response.model()).isEqualTo("jev-1.13.0");
        assertThat(response.usage()).isEqualTo(new Usage(120, 6));
        assertThat(response.answers()).containsOnlyKeys("category", "severity", "emergency");

        final var category = (ChoiceAnswer) response.answers().get("category");
        assertThat(category.choice()).isEqualTo("SAFETY");
        assertThat(category.confidence()).isEqualTo(0.91);
        assertThat(category.probabilities()).containsEntry("MARKETING", 0.02).containsEntry("SAFETY", 0.95).containsEntry("SHIPPING", 0.03);

        final var severity = (ScoreAnswer) response.answers().get("severity");
        assertThat(severity.score()).isEqualTo(1.23);
        assertThat(severity.confidence()).isEqualTo(0.8);
        assertThat(severity.probabilities()).containsEntry(0, 0.2).containsEntry(1, 0.37).containsEntry(2, 0.43);

        final var emergency = (YesNoAnswer) response.answers().get("emergency");
        assertThat(emergency.probability()).isEqualTo(0.12);
    }

    @Test
    public void shouldParseSingleTypeFixtures() throws Exception {
        final SequencedMap<String, String> options = new LinkedHashMap<>();
        options.put("angry", "The customer is upset");
        options.put("neutral", "");
        final var choice = SystemOneResponseParser.parse(SystemOneRequestMapperTest.fixture("response-choice.json").toString(),
                List.of(new ChoiceQuestion("tone", "What is the tone of the message?", options)));
        assertThat(choice.answers().get("tone")).isInstanceOf(ChoiceAnswer.class);

        final var score = SystemOneResponseParser.parse(SystemOneRequestMapperTest.fixture("response-score.json").toString(),
                List.of(new ScoreQuestion("urgency", "How urgent is the request?", List.of("Not urgent", "Somewhat urgent", "Very urgent"))));
        assertThat(score.answers().get("urgency")).isInstanceOf(ScoreAnswer.class);

        final var noul = SystemOneResponseParser.parse(SystemOneRequestMapperTest.fixture("response-noul.json").toString(),
                List.of(new YesNoQuestion("billing", "Is this a billing problem?", "A payment is involved", "No payment is involved")));
        assertThat(noul.answers().get("billing")).isInstanceOf(YesNoAnswer.class);
    }

    @Test
    public void shouldIgnoreUnknownFields() throws Exception {
        final var body = mixed();
        body.put("request_id", "abc");
        body.putObject("future").put("anything", true);
        ((ObjectNode) body.at("/answers/category")).put("explanation", "because");
        ((ObjectNode) body.at("/answers/emergency")).put("legend", "ignored");
        ((ObjectNode) body.get("usage")).put("cached_tokens", 3);

        final var response = SystemOneResponseParser.parse(body.toString(), ALL);

        assertThat(response.answers()).hasSize(3);
        assertThat(response.usage()).isEqualTo(new Usage(120, 6));
    }

    @Test
    public void shouldDefaultUsageWhenAbsent() throws Exception {
        final var body = mixed();
        body.remove("usage");

        assertThat(SystemOneResponseParser.parse(body.toString(), ALL).usage()).isEqualTo(new Usage(0, 0));
    }

    @Test
    public void shouldRejectTypeMismatch() throws Exception {
        final var body = mixed();
        ((ObjectNode) body.at("/answers/category")).put("type", "score");

        assertThat(failure(body.toString())).hasMessageContaining("category").hasMessageContaining("expected type 'choice'");
    }

    @Test
    public void shouldRejectMissingAnswer() throws Exception {
        final var body = mixed();
        ((ObjectNode) body.get("answers")).remove("severity");

        assertThat(failure(body.toString())).hasMessageContaining("missing answer to question 'severity'");
    }

    @Test
    public void shouldRejectExtraAnswer() throws Exception {
        final var body = mixed();
        ((ObjectNode) body.get("answers")).putObject("surprise").put("type", "noul").put("noul", 0.5);

        assertThat(failure(body.toString())).hasMessageContaining("unknown question 'surprise'");
    }

    @Test
    public void shouldRejectMissingConfidence() throws Exception {
        final var body = mixed();
        ((ObjectNode) body.at("/answers/category")).remove("confidence");

        assertThat(failure(body.toString())).hasMessageContaining("confidence");
    }

    @Test
    public void shouldRejectMissingScore() throws Exception {
        final var body = mixed();
        ((ObjectNode) body.at("/answers/severity")).remove("score");

        assertThat(failure(body.toString())).hasMessageContaining("'score'");
    }

    @Test
    public void shouldRejectNonNumericProbability() throws Exception {
        final var body = mixed();
        ((ObjectNode) body.at("/answers/category/probabilities")).put("SAFETY", "high");

        assertThat(failure(body.toString())).hasMessageContaining("probabilities");
    }

    @Test
    public void shouldRejectNonNumericScoreLevel() throws Exception {
        final var body = mixed();
        ((ObjectNode) body.at("/answers/severity/probabilities")).put("high", 0.1);

        assertThat(failure(body.toString())).hasMessageContaining("non numeric level 'high'");
    }

    @Test
    public void shouldRejectProbabilityOutOfRange() throws Exception {
        final var body = mixed();
        ((ObjectNode) body.at("/answers/emergency")).put("noul", 1.5);

        assertThat(failure(body.toString())).hasMessageContaining("emergency");
    }

    @Test
    public void shouldRejectMissingModelOrAnswers() throws Exception {
        final var withoutModel = mixed();
        withoutModel.remove("model");
        assertThat(failure(withoutModel.toString())).hasMessageContaining("'model'");

        final var withoutAnswers = mixed();
        withoutAnswers.remove("answers");
        assertThat(failure(withoutAnswers.toString())).hasMessageContaining("'answers'");
    }

    @Test
    public void shouldRejectMalformedJsonWithoutEchoingTheBody() {
        final var thrown = failure("{\"model\": \"secret-state-content\", ");

        assertThat(thrown).hasMessageContaining("not a valid JSON");
        assertThat(thrown.getMessage()).doesNotContain("secret-state-content");
    }

    @Test
    public void shouldRejectNonObjectJson() {
        assertThat(failure("[1, 2, 3]")).hasMessageContaining("not a JSON object");
        assertThat(failure("\"text\"")).hasMessageContaining("not a JSON object");
    }

    @Test
    public void shouldRejectEmptyBody() {
        assertThat(failure("")).hasMessageContaining("empty");
        assertThat(failure("   ")).hasMessageContaining("empty");
        assertThatThrownBy(() -> SystemOneResponseParser.parse(null, ALL)).isInstanceOf(DecisionException.class).hasMessageContaining("empty");
    }
}
