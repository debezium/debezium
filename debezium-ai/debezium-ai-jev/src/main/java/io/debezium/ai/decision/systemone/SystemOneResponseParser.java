/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.decision.systemone;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.ai.decision.api.Answer;
import io.debezium.ai.decision.api.ChoiceAnswer;
import io.debezium.ai.decision.api.ChoiceQuestion;
import io.debezium.ai.decision.api.DecisionException;
import io.debezium.ai.decision.api.DecisionResponse;
import io.debezium.ai.decision.api.Question;
import io.debezium.ai.decision.api.ScoreAnswer;
import io.debezium.ai.decision.api.ScoreQuestion;
import io.debezium.ai.decision.api.Usage;
import io.debezium.ai.decision.api.YesNoAnswer;
import io.debezium.ai.decision.api.YesNoQuestion;

/**
 * Parses the body of a successful System One {@code POST /v1/systemone} response.
 * Unknown fields are ignored, anything else which deviates from the expected shape
 * is reported as a non retriable {@link DecisionException}.
 * The messages never contain the body.
 *
 * @author Jiri Pechanec
 */
final class SystemOneResponseParser {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private SystemOneResponseParser() {
    }

    static DecisionResponse parse(final String body, final List<Question> questions) {
        if (body == null || body.isBlank()) {
            throw invalid("the response body is empty");
        }
        final JsonNode root;
        try {
            root = MAPPER.readTree(body);
        }
        catch (JsonProcessingException e) {
            throw invalid("the response is not a valid JSON");
        }
        if (root == null || !root.isObject()) {
            throw invalid("the response is not a JSON object");
        }
        final var model = root.path("model");
        if (!model.isTextual()) {
            throw invalid("the response does not contain the 'model'");
        }
        final var answersNode = root.path("answers");
        if (!answersNode.isObject()) {
            throw invalid("the response does not contain the 'answers'");
        }

        final Map<String, Answer> answers = new LinkedHashMap<>();
        final Set<String> expected = new HashSet<>();
        for (final var question : questions) {
            expected.add(question.name());
            final var answer = answersNode.get(question.name());
            if (answer == null || !answer.isObject()) {
                throw invalid("missing answer to question '" + question.name() + "'");
            }
            try {
                answers.put(question.name(), answer(question, answer));
            }
            catch (IllegalArgumentException e) {
                throw invalid("invalid answer to question '" + question.name() + "': " + e.getMessage());
            }
        }
        for (final var entry : answersNode.properties()) {
            if (!expected.contains(entry.getKey())) {
                throw invalid("unexpected answer to unknown question '" + entry.getKey() + "'");
            }
        }
        return new DecisionResponse(model.asText(), answers, usage(root.path("usage")));
    }

    private static Answer answer(final Question question, final JsonNode node) {
        final var name = question.name();
        return switch (question) {
            case ChoiceQuestion choice -> {
                requireType(node, "choice");
                final var probabilities = new LinkedHashMap<String, Double>();
                for (final var option : requireObject(node, "probabilities").properties()) {
                    probabilities.put(option.getKey(), number(option.getValue(), "probabilities"));
                }
                yield new ChoiceAnswer(name, text(node, "choice"), number(node.path("confidence"), "confidence"), probabilities);
            }
            case ScoreQuestion score -> {
                requireType(node, "score");
                final var probabilities = new LinkedHashMap<Integer, Double>();
                for (final var level : requireObject(node, "probabilities").properties()) {
                    try {
                        probabilities.put(Integer.valueOf(level.getKey()), number(level.getValue(), "probabilities"));
                    }
                    catch (NumberFormatException e) {
                        throw new IllegalArgumentException("'probabilities' contains non numeric level '" + level.getKey() + "'");
                    }
                }
                yield new ScoreAnswer(name, number(node.path("score"), "score"), number(node.path("confidence"), "confidence"), probabilities);
            }
            case YesNoQuestion yesNo -> {
                requireType(node, "noul");
                yield new YesNoAnswer(name, number(node.path("noul"), "noul"));
            }
        };
    }

    private static void requireType(final JsonNode node, final String expected) {
        final var type = node.path("type");
        if (!type.isTextual() || !expected.equals(type.asText())) {
            throw new IllegalArgumentException("expected type '" + expected + "'");
        }
    }

    private static JsonNode requireObject(final JsonNode node, final String field) {
        final var value = node.path(field);
        if (!value.isObject()) {
            throw new IllegalArgumentException("'" + field + "' is missing");
        }
        return value;
    }

    private static String text(final JsonNode node, final String field) {
        final var value = node.path(field);
        if (!value.isTextual()) {
            throw new IllegalArgumentException("'" + field + "' is missing");
        }
        return value.asText();
    }

    private static double number(final JsonNode value, final String field) {
        if (!value.isNumber()) {
            throw new IllegalArgumentException("'" + field + "' is missing or not a number");
        }
        return value.asDouble();
    }

    private static Usage usage(final JsonNode node) {
        if (!node.isObject()) {
            return new Usage(0, 0);
        }
        return new Usage(Math.max(0, node.path("input_tokens").asLong(0)), Math.max(0, node.path("output_tokens").asLong(0)));
    }

    private static DecisionException invalid(final String reason) {
        return new DecisionException("Invalid System One response: " + reason, false);
    }
}
