/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.decision.systemone;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.debezium.ai.decision.api.ChoiceQuestion;
import io.debezium.ai.decision.api.DecisionException;
import io.debezium.ai.decision.api.DecisionRequest;
import io.debezium.ai.decision.api.Question;
import io.debezium.ai.decision.api.ScoreQuestion;
import io.debezium.ai.decision.api.YesNoQuestion;

/**
 * Maps a {@link DecisionRequest} to the JSON body of a System One {@code POST /v1/systemone} call.
 * This and {@link SystemOneResponseParser} are the only classes which know the wire format.
 *
 * @author Jiri Pechanec
 */
final class SystemOneRequestMapper {

    static final int MAX_CHOICE_OPTIONS = 255;
    static final int MAX_JSON_NESTING = 64;

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final JsonNodeFactory NODES = JsonNodeFactory.instance;

    private final String model;
    private final String provider;
    private final int maxScoreLevels;

    SystemOneRequestMapper(final String model, final String provider, final int maxScoreLevels) {
        this.model = model;
        this.provider = provider;
        this.maxScoreLevels = maxScoreLevels;
    }

    /**
     * @return description of the violated provider limit or {@code null} if the question is supported
     */
    static String limitViolation(final Question question, final String provider, final int maxScoreLevels) {
        return switch (question) {
            case ChoiceQuestion choice when choice.options().size() > MAX_CHOICE_OPTIONS ->
                "Question '" + choice.name() + "' has " + choice.options().size() + " options but " + provider + " supports at most " + MAX_CHOICE_OPTIONS;
            case ScoreQuestion score when score.levels().size() > maxScoreLevels ->
                "Question '" + score.name() + "' has " + score.levels().size() + " levels but " + provider + " supports at most " + maxScoreLevels;
            default -> null;
        };
    }

    String toJson(final DecisionRequest request) {
        final var body = NODES.objectNode();
        body.put("model", model);
        // The body itself is the first level of the nesting
        body.set("state", state(request.state(), 2));
        final var questions = body.putObject("questions");
        for (final var question : request.questions()) {
            final var violation = limitViolation(question, provider, maxScoreLevels);
            if (violation != null) {
                throw new DecisionException(violation, false);
            }
            questions.set(question.name(), question(question));
        }
        try {
            return MAPPER.writeValueAsString(body);
        }
        catch (JsonProcessingException e) {
            throw new DecisionException("Unable to serialize a System One request", e, false);
        }
    }

    private static ObjectNode question(final Question question) {
        final var node = NODES.objectNode();
        switch (question) {
            case ChoiceQuestion choice -> {
                node.put("type", "choice");
                node.put("instructions", choice.instructions());
                final var criteria = node.putObject("criteria");
                choice.options().forEach(criteria::put);
            }
            case ScoreQuestion score -> {
                node.put("type", "score");
                node.put("instructions", score.instructions());
                final var criteria = node.putArray("criteria");
                score.levels().forEach(criteria::add);
            }
            case YesNoQuestion yesNo -> {
                node.put("type", "noul");
                node.put("instructions", yesNo.instructions());
                if (yesNo.yesDescription() != null || yesNo.noDescription() != null) {
                    final var criteria = node.putObject("criteria");
                    if (yesNo.yesDescription() != null) {
                        criteria.put("true", yesNo.yesDescription());
                    }
                    if (yesNo.noDescription() != null) {
                        criteria.put("false", yesNo.noDescription());
                    }
                }
            }
        }
        return node;
    }

    private static JsonNode state(final Object value, final int depth) {
        if (depth > MAX_JSON_NESTING) {
            throw new DecisionException("The state exceeds the maximum JSON nesting of " + MAX_JSON_NESTING, false);
        }
        return switch (value) {
            case null -> NODES.nullNode();
            case String text -> NODES.textNode(text);
            case Boolean bool -> NODES.booleanNode(bool);
            case Double number when !Double.isFinite(number) -> throw nonFinite();
            case Float number when !Float.isFinite(number) -> throw nonFinite();
            case Number number -> MAPPER.valueToTree(number);
            case Map<?, ?> map -> {
                final var node = NODES.objectNode();
                for (final var entry : map.entrySet()) {
                    if (!(entry.getKey() instanceof String key)) {
                        throw new DecisionException("The state contains a map with a non-string key", false);
                    }
                    node.set(key, state(entry.getValue(), depth + 1));
                }
                yield node;
            }
            case List<?> list -> {
                final var node = NODES.arrayNode();
                for (final var element : list) {
                    node.add(state(element, depth + 1));
                }
                yield node;
            }
            default -> throw new DecisionException("The state contains an unsupported value of type " + value.getClass().getName(), false);
        };
    }

    private static DecisionException nonFinite() {
        return new DecisionException("The state contains a non finite number", false);
    }
}
