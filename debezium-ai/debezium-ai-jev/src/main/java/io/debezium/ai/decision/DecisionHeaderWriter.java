/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.decision;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.Headers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.ai.decision.api.Answer;
import io.debezium.ai.decision.api.ChoiceAnswer;
import io.debezium.ai.decision.api.ChoiceQuestion;
import io.debezium.ai.decision.api.DecisionResponse;
import io.debezium.ai.decision.api.Question;
import io.debezium.ai.decision.api.ScoreAnswer;
import io.debezium.ai.decision.api.YesNoAnswer;

/**
 * Writes the outcome of an evaluation to record headers. The original headers are never modified,
 * a copy is returned instead. Existing headers of the same names are replaced so the result is idempotent.
 *
 * @author Jiri Pechanec
 */
final class DecisionHeaderWriter {

    static final int MAX_ERROR_LENGTH = 512;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final List<Question> questions;
    private final boolean details;

    DecisionHeaderWriter(final List<Question> questions, final boolean details) {
        this.questions = List.copyOf(questions);
        this.details = details;
    }

    Headers success(final Headers original, final DecisionResponse response) {
        final var headers = cleared(original);
        headers.addString(DecisionHeaders.STATUS, DecisionHeaders.STATUS_SUCCESS);
        headers.addString(DecisionHeaders.MODEL, response.model());
        for (final var question : questions) {
            write(headers, question, response.answers().get(question.name()));
        }
        return headers;
    }

    Headers error(final Headers original, final String message) {
        final var headers = cleared(original);
        headers.addString(DecisionHeaders.STATUS, DecisionHeaders.STATUS_ERROR);
        headers.addString(DecisionHeaders.ERROR, sanitize(message));
        return headers;
    }

    static String sanitize(final String message) {
        if (message == null || message.isBlank()) {
            return "Unknown error";
        }
        final var singleLine = message.replaceAll("[\\p{Cntrl}\\s]+", " ").trim();
        return singleLine.length() > MAX_ERROR_LENGTH ? singleLine.substring(0, MAX_ERROR_LENGTH) : singleLine;
    }

    private Headers cleared(final Headers original) {
        final var headers = original.duplicate();
        headers.remove(DecisionHeaders.STATUS);
        headers.remove(DecisionHeaders.MODEL);
        headers.remove(DecisionHeaders.ERROR);
        for (final var question : questions) {
            headers.remove(DecisionHeaders.answer(question.name()));
            headers.remove(DecisionHeaders.confidence(question.name()));
            headers.remove(DecisionHeaders.probabilities(question.name()));
        }
        return headers;
    }

    private void write(final Headers headers, final Question question, final Answer answer) {
        final var name = question.name();
        switch (answer) {
            case ChoiceAnswer choice -> {
                headers.addString(DecisionHeaders.answer(name), choice.choice());
                if (details) {
                    headers.addDouble(DecisionHeaders.confidence(name), choice.confidence());
                    final Map<String, Double> ordered = new LinkedHashMap<>();
                    if (question instanceof ChoiceQuestion choiceQuestion) {
                        for (final var option : choiceQuestion.options().keySet()) {
                            if (choice.probabilities().containsKey(option)) {
                                ordered.put(option, choice.probabilities().get(option));
                            }
                        }
                    }
                    else {
                        ordered.putAll(choice.probabilities());
                    }
                    headers.addString(DecisionHeaders.probabilities(name), toJson(ordered));
                }
            }
            case ScoreAnswer score -> {
                headers.addDouble(DecisionHeaders.answer(name), score.score());
                if (details) {
                    headers.addDouble(DecisionHeaders.confidence(name), score.confidence());
                    final Map<String, Double> byLevel = new LinkedHashMap<>();
                    score.probabilities().forEach((level, probability) -> byLevel.put(String.valueOf(level), probability));
                    headers.addString(DecisionHeaders.probabilities(name), toJson(byLevel));
                }
            }
            case YesNoAnswer yesNo -> headers.addDouble(DecisionHeaders.answer(name), yesNo.probability());
        }
    }

    private static String toJson(final Map<String, Double> probabilities) {
        try {
            return MAPPER.writeValueAsString(probabilities);
        }
        catch (JsonProcessingException e) {
            throw new DataException("Unable to serialize probabilities", e);
        }
    }
}
