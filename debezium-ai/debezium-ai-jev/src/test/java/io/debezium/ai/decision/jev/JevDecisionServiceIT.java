/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.decision.jev;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SequencedMap;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import io.debezium.ai.decision.api.ChoiceAnswer;
import io.debezium.ai.decision.api.ChoiceQuestion;
import io.debezium.ai.decision.api.DecisionException;
import io.debezium.ai.decision.api.DecisionRequest;
import io.debezium.ai.decision.api.DecisionService;
import io.debezium.ai.decision.api.Question;
import io.debezium.ai.decision.api.ScoreAnswer;
import io.debezium.ai.decision.api.ScoreQuestion;
import io.debezium.ai.decision.api.YesNoAnswer;
import io.debezium.ai.decision.api.YesNoQuestion;
import io.debezium.config.Configuration;

/**
 * Integration test of {@link DecisionService} against the real Jev (TypeSafe) API. Skipped unless the
 * {@code TYPESAFE_API_KEY} environment variable is set. Optional overrides are {@code TYPESAFE_BASE_URL} and {@code TYPESAFE_MODEL}.
 * The tests use synthetic data only.
 *
 * @author Jiri Pechanec
 */
@EnabledIfEnvironmentVariable(named = "TYPESAFE_API_KEY", matches = ".+")
public class JevDecisionServiceIT {

    private static final List<String> LEVELS = List.of("Not urgent", "Somewhat urgent", "Very urgent");

    static Map<String, String> jevConfig(final String apiKey) {
        final Map<String, String> config = new HashMap<>();
        config.put("jev.api.key", apiKey);
        final var baseUrl = System.getenv("TYPESAFE_BASE_URL");
        if (baseUrl != null && !baseUrl.isBlank()) {
            config.put("jev.base.url", baseUrl);
        }
        final var model = System.getenv("TYPESAFE_MODEL");
        if (model != null && !model.isBlank()) {
            config.put("jev.model", model);
        }
        return config;
    }

    private static List<Question> questions() {
        final SequencedMap<String, String> options = new LinkedHashMap<>();
        options.put("billing", "Questions about charges and invoices");
        options.put("technical", "Technical problems");
        options.put("other", "Anything else");
        return List.of(
                new ChoiceQuestion("topic", "What is the topic of the message?", options),
                new ScoreQuestion("urgency", "How urgent is the message?", LEVELS),
                new YesNoQuestion("refund", "Does the customer ask for a refund?", "A refund is requested", "No refund is requested"));
    }

    private static DecisionService service(final Map<String, String> config, final List<Question> questions) {
        return new JevDecisionServiceFactory().create(Configuration.from(config), questions);
    }

    @Test
    public void shouldAnswerAllQuestionTypesInOneRequest() {
        final var questions = questions();
        try (var service = service(jevConfig(System.getenv("TYPESAFE_API_KEY")), questions)) {
            final var response = service.decide(new DecisionRequest(Map.of("message", "I was charged twice, please refund"), questions));

            assertThat(response.model()).isNotBlank();
            assertThat(response.usage().inputTokens()).isPositive();
            assertThat(response.answers()).containsOnlyKeys("topic", "urgency", "refund");

            final var topic = (ChoiceAnswer) response.answers().get("topic");
            assertThat(topic.choice()).isIn("billing", "technical", "other");
            assertThat(topic.confidence()).isBetween(0.0, 1.0);
            assertThat(topic.probabilities().values()).allSatisfy(p -> assertThat(p).isBetween(0.0, 1.0));
            assertThat(topic.probabilities().values().stream().mapToDouble(Double::doubleValue).sum()).isBetween(0.98, 1.02);

            final var urgency = (ScoreAnswer) response.answers().get("urgency");
            assertThat(urgency.score()).isBetween(0.0, (double) (LEVELS.size() - 1));
            assertThat(urgency.confidence()).isBetween(0.0, 1.0);

            final var refund = (YesNoAnswer) response.answers().get("refund");
            assertThat(refund.probability()).isBetween(0.0, 1.0);
        }
    }

    @Test
    public void shouldUnderstandObviousSemantics() {
        final var questions = List.<Question> of(
                new YesNoQuestion("refund", "Does the customer ask for a refund?", "A refund is requested", "No refund is requested"));
        try (var service = service(jevConfig(System.getenv("TYPESAFE_API_KEY")), questions)) {
            final var asks = (YesNoAnswer) service.decide(new DecisionRequest(Map.of("message", "I was charged twice, please refund"), questions))
                    .answers().get("refund");
            final var thanks = (YesNoAnswer) service.decide(new DecisionRequest(Map.of("message", "Thanks, that fixed it!"), questions))
                    .answers().get("refund");

            assertThat(asks.probability()).isGreaterThan(0.5);
            assertThat(thanks.probability()).isLessThan(0.5);
        }
    }

    @Test
    public void shouldRejectAnInvalidApiKeyWithoutLeakingIt() {
        final var fakeKey = UUID.randomUUID().toString();
        final var questions = questions();
        try (var service = service(jevConfig(fakeKey), questions)) {
            assertThatThrownBy(() -> service.decide(new DecisionRequest(Map.of("message", "hello"), questions)))
                    .isInstanceOfSatisfying(DecisionException.class, e -> {
                        assertThat(e.isRetriable()).isFalse();
                        assertThat(e.httpStatus()).isEqualTo(401);
                    })
                    .hasMessageNotContaining(fakeKey);
        }
    }
}
