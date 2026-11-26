/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.decision;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.ai.decision.api.Answer;
import io.debezium.ai.decision.api.ChoiceAnswer;
import io.debezium.ai.decision.api.DecisionException;
import io.debezium.ai.decision.api.DecisionRequest;
import io.debezium.ai.decision.api.DecisionResponse;
import io.debezium.ai.decision.api.ScoreAnswer;
import io.debezium.ai.decision.api.Usage;
import io.debezium.ai.decision.api.YesNoAnswer;

/**
 * Tests of {@link DecisionToHeaders} against the real in-memory {@link StubDecisionService}.
 *
 * @author Jiri Pechanec
 */
public class DecisionToHeadersTest {

    private static final String PREFIX = "__debezium.ai.decision.";

    private DecisionToHeaders<SourceRecord> smt;

    @AfterEach
    public void closeSmt() {
        if (smt != null) {
            smt.close();
        }
    }

    private static Map<String, String> config() {
        final Map<String, String> config = new HashMap<>();
        config.put("decision.provider", "stub");
        config.put("state.fields", "after.name,after.description");
        config.put("questions", "category,severity,emergency");
        config.put("questions.category.type", "choice");
        config.put("questions.category.instructions", "Which team should handle the change?");
        config.put("questions.category.criteria.MARKETING", "Promotion");
        config.put("questions.category.criteria.SAFETY", "Hazards");
        config.put("questions.category.criteria.SHIPPING", "Logistics");
        config.put("questions.severity.type", "score");
        config.put("questions.severity.instructions", "How severe?");
        config.put("questions.severity.criteria.0", "low");
        config.put("questions.severity.criteria.1", "high");
        config.put("questions.emergency.type", "yesno");
        config.put("questions.emergency.instructions", "Is it an emergency?");
        return config;
    }

    private StubDecisionService configure(final Map<String, String> config) {
        smt = new DecisionToHeaders<>();
        smt.configure(config);
        return StubDecisionService.last();
    }

    @Test
    public void shouldAddHeadersAndPreserveEverythingElse() {
        final var stub = configure(config());
        final var record = ProductRecords.create("Space heater", "Heats a room");

        final var result = smt.apply(record);

        assertThat(result).isNotSameAs(record);
        assertThat(result.key()).isSameAs(record.key());
        assertThat(result.value()).isSameAs(record.value());
        assertThat(result.keySchema()).isSameAs(record.keySchema());
        assertThat(result.valueSchema()).isSameAs(record.valueSchema());
        assertThat(result.topic()).isEqualTo(record.topic());
        assertThat(result.kafkaPartition()).isEqualTo(record.kafkaPartition());
        assertThat(result.timestamp()).isEqualTo(record.timestamp());
        assertThat(result.sourcePartition()).isEqualTo(record.sourcePartition());
        assertThat(result.headers().lastWithName(PREFIX + "status").value()).isEqualTo("SUCCESS");
        assertThat(result.headers().lastWithName(PREFIX + "model").value()).isEqualTo("stub-1");
        assertThat(result.headers().lastWithName(PREFIX + "category").value()).isEqualTo("MARKETING");
        assertThat(result.headers().lastWithName(PREFIX + "category.confidence").schema()).isSameAs(Schema.FLOAT64_SCHEMA);
        assertThat(result.headers().lastWithName(PREFIX + "severity").value()).isEqualTo(0.5);
        assertThat(result.headers().lastWithName(PREFIX + "emergency").value()).isEqualTo(0.75);
        assertThat(record.headers().size()).isZero();
        assertThat(stub.requests).hasSize(1);
    }

    @Test
    public void shouldCallTheServiceOncePerRecordRegardlessOfTheQuestionCount() {
        final var stub = configure(config());

        smt.apply(ProductRecords.create("a", "b"));
        smt.apply(ProductRecords.update("c", "d"));

        assertThat(stub.requests).hasSize(2);
        assertThat(stub.requests.get(0).questions()).hasSize(3);
    }

    @Test
    public void shouldSendExpectedStateAndQuestions() {
        final var stub = configure(config());

        smt.apply(ProductRecords.create("Space heater", "Heats a room"));

        final var request = stub.requests.get(0);
        assertThat(request.state()).isEqualTo(Map.of("after.name", "Space heater", "after.description", "Heats a room"));
        assertThat(request.questions()).extracting("name").containsExactly("category", "severity", "emergency");
    }

    @Test
    public void shouldUseTextState() {
        final var configuration = config();
        configuration.put("state.format", "text");
        final var stub = configure(configuration);

        smt.apply(ProductRecords.create("Space heater", "Heats a room"));

        assertThat(stub.requests.get(0).state()).isEqualTo("Space heater\nHeats a room");
    }

    @Test
    public void shouldSupportBeforeAndAfterFields() {
        final var configuration = config();
        configuration.put("state.fields", "before.description,after.description");
        final var stub = configure(configuration);

        smt.apply(ProductRecords.update("name", "new text"));

        assertThat(stub.requests.get(0).state()).isEqualTo(Map.of("before.description", "old new text", "after.description", "new text"));
    }

    @Test
    public void shouldPassTombstoneNonEnvelopeAndDeleteUnchanged() {
        final var stub = configure(config());
        final var tombstone = ProductRecords.tombstone();
        final var plain = new SourceRecord(Map.of(), Map.of(), "topic", Schema.STRING_SCHEMA, "value");
        final var delete = ProductRecords.delete("a", "b");

        assertThat(smt.apply(tombstone)).isSameAs(tombstone);
        assertThat(smt.apply(plain)).isSameAs(plain);
        assertThat(smt.apply(delete)).isSameAs(delete);
        assertThat(stub.requests).isEmpty();
    }

    @Test
    public void shouldWrapFailureInFailMode() {
        final var stub = configure(config());
        stub.responder = request -> {
            throw new DecisionException("service unavailable", true, 503);
        };

        assertThatThrownBy(() -> smt.apply(ProductRecords.create("a", "b"))).isInstanceOf(DebeziumException.class)
                .hasMessageContaining("service unavailable")
                .hasMessageContaining("dbserver1.inventory.products")
                .hasCauseInstanceOf(DecisionException.class);
    }

    @Test
    public void shouldPassRecordUnchangedInSkipMode() {
        final var configuration = config();
        configuration.put("decision.error.mode", "skip");
        final var stub = configure(configuration);
        stub.responder = request -> {
            throw new DecisionException("service unavailable", true, 503);
        };
        final var record = ProductRecords.create("a", "b");

        assertThat(smt.apply(record)).isSameAs(record);
    }

    @Test
    public void shouldWriteErrorHeadersInHeaderMode() {
        final var configuration = config();
        configuration.put("decision.error.mode", "header");
        final var stub = configure(configuration);
        stub.responder = request -> {
            throw new DecisionException("service\nunavailable", true, 503);
        };
        final var record = ProductRecords.create("a", "b");

        final var result = smt.apply(record);

        assertThat(result.value()).isSameAs(record.value());
        assertThat(result.headers().lastWithName(PREFIX + "status").value()).isEqualTo("ERROR");
        assertThat(result.headers().lastWithName(PREFIX + "error").value()).isEqualTo("service unavailable");
        assertThat(result.headers().lastWithName(PREFIX + "category")).isNull();
        assertThat(record.headers().size()).isZero();
    }

    private void shouldRejectResponse(final java.util.function.Function<DecisionRequest, DecisionResponse> responder, final String message) {
        final var stub = configure(config());
        stub.responder = responder;

        assertThatThrownBy(() -> smt.apply(ProductRecords.create("a", "b"))).isInstanceOf(DebeziumException.class)
                .hasRootCauseInstanceOf(DecisionException.class)
                .hasMessageContaining(message);
    }

    private static DecisionResponse withAnswer(final DecisionRequest request, final String question, final Answer replacement) {
        final var canned = StubDecisionService.cannedResponse(request);
        final Map<String, Answer> answers = new HashMap<>(canned.answers());
        if (replacement == null) {
            answers.remove(question);
        }
        else {
            answers.put(question, replacement);
        }
        return new DecisionResponse(canned.model(), answers, new Usage(1, 1));
    }

    @Test
    public void shouldRejectMissingAnswer() {
        shouldRejectResponse(request -> withAnswer(request, "severity", null), "Missing answer to question 'severity'");
    }

    @Test
    public void shouldRejectAnswerOfWrongKind() {
        shouldRejectResponse(request -> withAnswer(request, "emergency", new ChoiceAnswer("emergency", "x", 0.5, Map.of("x", 1.0))), "not a yesno answer");
    }

    @Test
    public void shouldRejectUnknownChoice() {
        shouldRejectResponse(request -> withAnswer(request, "category", new ChoiceAnswer("category", "UNKNOWN", 0.5, Map.of("UNKNOWN", 1.0))),
                "not one of the configured options");
    }

    @Test
    public void shouldRejectScoreOutOfRange() {
        shouldRejectResponse(request -> withAnswer(request, "severity", new ScoreAnswer("severity", 2.5, 0.5, Map.of(0, 1.0))),
                "outside of the configured levels");
    }

    @Test
    public void shouldRejectEmptyChoiceProbabilities() {
        shouldRejectResponse(request -> withAnswer(request, "category", new ChoiceAnswer("category", "SAFETY", 0.5, Map.of())),
                "must match the configured options");
    }

    @Test
    public void shouldRejectUnknownChoiceProbabilityKey() {
        shouldRejectResponse(
                request -> withAnswer(request, "category", new ChoiceAnswer("category", "SAFETY", 0.5, Map.of("SAFETY", 0.6, "MARKETING", 0.3, "UNKNOWN", 0.1))),
                "must match the configured options");
    }

    @Test
    public void shouldRejectMissingScoreProbabilityKey() {
        shouldRejectResponse(request -> withAnswer(request, "severity", new ScoreAnswer("severity", 0.5, 0.5, Map.of(0, 0.5))),
                "must match the configured levels");
    }

    @Test
    public void shouldRejectUnknownScoreProbabilityKey() {
        shouldRejectResponse(request -> withAnswer(request, "severity", new ScoreAnswer("severity", 0.5, 0.5, Map.of(0, 0.5, 2, 0.5))),
                "must match the configured levels");
    }

    @Test
    public void shouldRejectUnexpectedAnswer() {
        shouldRejectResponse(request -> withAnswer(request, "extra", new YesNoAnswer("extra", 0.5)), "unknown question 'extra'");
    }

    @Test
    public void shouldRejectAnswerForOtherQuestion() {
        shouldRejectResponse(request -> withAnswer(request, "emergency", new YesNoAnswer("severity", 0.5)), "reported as answer to question 'emergency'");
    }

    @Test
    public void shouldReportInvalidAnswerInHeaderMode() {
        final var configuration = config();
        configuration.put("decision.error.mode", "header");
        final var stub = configure(configuration);
        stub.responder = request -> withAnswer(request, "severity", null);

        final var result = smt.apply(ProductRecords.create("a", "b"));

        assertThat(result.headers().lastWithName(PREFIX + "status").value()).isEqualTo("ERROR");
        assertThat(result.headers().lastWithName(PREFIX + "error").value().toString()).contains("Missing answer");
    }

    @Test
    public void shouldNotWriteDetailsWhenDisabled() {
        final var configuration = config();
        configuration.put("headers.details", "false");
        configure(configuration);

        final var result = smt.apply(ProductRecords.create("a", "b"));

        assertThat(result.headers().lastWithName(PREFIX + "category")).isNotNull();
        assertThat(result.headers().lastWithName(PREFIX + "category.confidence")).isNull();
        assertThat(result.headers().lastWithName(PREFIX + "category.probabilities")).isNull();
    }

    @Test
    public void shouldFailOnUnknownProviderAndListAvailableOnes() {
        final var configuration = config();
        configuration.put("decision.provider", "nothing");
        smt = new DecisionToHeaders<>();

        assertThatThrownBy(() -> smt.configure(configuration)).isInstanceOf(ConfigException.class)
                .hasMessageContaining("nothing").hasMessageContaining("jev").hasMessageContaining("stub");
    }

    @Test
    public void shouldFailWithoutStateFields() {
        final var configuration = config();
        configuration.remove("state.fields");
        smt = new DecisionToHeaders<>();

        assertThatThrownBy(() -> smt.configure(configuration)).isInstanceOf(ConfigException.class).hasMessageContaining("state.fields");
    }

    @Test
    public void shouldFailOnInvalidStateFields() {
        final var configuration = config();
        configuration.put("state.fields", "after..name");
        smt = new DecisionToHeaders<>();

        assertThatThrownBy(() -> smt.configure(configuration)).isInstanceOf(ConfigException.class).hasMessageContaining("after..name");
    }

    @Test
    public void shouldFailOnBadQuestionConfiguration() {
        final var configuration = config();
        configuration.put("questions.severity.criteria.5", "gap");
        smt = new DecisionToHeaders<>();

        assertThatThrownBy(() -> smt.configure(configuration)).isInstanceOf(ConfigException.class).hasMessageContaining("questions.severity.criteria");
    }

    @Test
    public void shouldFailOnInvalidEnumValues() {
        final var configuration = config();
        configuration.put("decision.error.mode", "explode");
        smt = new DecisionToHeaders<>();

        assertThatThrownBy(() -> smt.configure(configuration)).isInstanceOf(ConfigException.class).hasMessageContaining("decision.error.mode");
    }

    @Test
    public void shouldFailWithoutJevApiKey() {
        final var configuration = config();
        configuration.put("decision.provider", "jev");
        smt = new DecisionToHeaders<>();

        assertThatThrownBy(() -> smt.configure(configuration)).isInstanceOf(ConfigException.class).hasMessageContaining("jev.api.key");
    }

    @Test
    public void shouldExposeVersionAndConfiguration() {
        smt = new DecisionToHeaders<>();

        assertThat(smt.version()).isEqualTo(Module.version());
        assertThat(smt.version()).isNotBlank();
        assertThat(smt.config().names()).contains("decision.provider", "state.fields", "state.format", "questions", "headers.details", "decision.error.mode",
                "jev.api.key", "jev.base.url");
        final List<String> fieldNames = smt.getConfigFields().allFieldNames().stream().toList();
        assertThat(fieldNames).contains("decision.provider", "state.fields", "questions", "jev.api.key", "jev.model");
    }

    @Test
    public void shouldCloseTheService() {
        final var stub = configure(config());

        smt.close();

        assertThat(stub.closed).isTrue();
        smt = null;
    }

    @Test
    public void shouldAllowCloseWithoutConfigure() {
        new DecisionToHeaders<SourceRecord>().close();
    }
}
