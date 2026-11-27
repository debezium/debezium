/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.decision;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.SequencedMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.apache.kafka.common.config.ConfigException;

import io.debezium.ai.decision.api.ChoiceQuestion;
import io.debezium.ai.decision.api.Question;
import io.debezium.ai.decision.api.ScoreQuestion;
import io.debezium.ai.decision.api.YesNoQuestion;
import io.debezium.config.Configuration;

/**
 * Parses the flat {@code questions} and {@code questions.<name>.*} options into {@link Question}s.
 * Every error names the offending key. Unknown keys are rejected so typos are never silent.
 *
 * @author Jiri Pechanec
 */
final class QuestionConfigParser {

    static final String QUESTIONS_KEY = "questions";

    static final Set<String> RESERVED_NAMES = Set.of("model", "status", "error");

    private static final String PREFIX = QUESTIONS_KEY + ".";
    private static final String TYPE = "type";
    private static final String INSTRUCTIONS = "instructions";
    private static final String CRITERIA_PREFIX = "criteria.";
    private static final Pattern LEVEL_INDEX = Pattern.compile("\\d{1,9}");

    private QuestionConfigParser() {
    }

    private enum QuestionType {
        CHOICE,
        SCORE,
        YESNO
    }

    static List<Question> parse(final Configuration config) {
        final var names = parseNames(config);
        validateKeys(config, names);

        final List<Question> questions = new ArrayList<>();
        for (final var name : names) {
            try {
                questions.add(parseQuestion(config, name));
            }
            catch (IllegalArgumentException e) {
                throw new ConfigException("Invalid question '" + name + "' configured under '" + PREFIX + name + ".*': " + e.getMessage());
            }
        }
        return questions;
    }

    private static List<String> parseNames(final Configuration config) {
        final var value = config.getString(QUESTIONS_KEY);
        if (value == null || value.isBlank()) {
            throw new ConfigException("'" + QUESTIONS_KEY + "' must be set to a non-empty list of question names.");
        }
        final List<String> names = new ArrayList<>();
        final Set<String> seen = new HashSet<>();
        for (final var name : config.getList(QUESTIONS_KEY)) {
            if (!Question.isValidName(name)) {
                throw new ConfigException("'" + QUESTIONS_KEY + "' contains invalid question name '" + name + "', it must match " + Question.NAME_PATTERN.pattern());
            }
            if (RESERVED_NAMES.contains(name)) {
                throw new ConfigException("'" + QUESTIONS_KEY + "' contains reserved question name '" + name + "', reserved names are error, model and status");
            }
            if (!seen.add(name)) {
                throw new ConfigException("'" + QUESTIONS_KEY + "' contains duplicate question name '" + name + "'");
            }
            names.add(name);
        }
        return names;
    }

    private static void validateKeys(final Configuration config, final List<String> names) {
        for (final var key : new TreeMap<>(config.asMap()).keySet()) {
            if (!key.startsWith(PREFIX)) {
                continue;
            }
            final var rest = key.substring(PREFIX.length());
            final var dot = rest.indexOf('.');
            if (dot < 0) {
                throw new ConfigException("Unknown option '" + key + "', expected '" + PREFIX + "<name>." + TYPE + "', '" + PREFIX + "<name>." + INSTRUCTIONS + "' or '"
                        + PREFIX + "<name>." + CRITERIA_PREFIX + "<key>'");
            }
            final var name = rest.substring(0, dot);
            final var subKey = rest.substring(dot + 1);
            if (!names.contains(name)) {
                throw new ConfigException("Option '" + key + "' refers to question '" + name + "' which is not listed in '" + QUESTIONS_KEY + "'");
            }
            if (!TYPE.equals(subKey) && !INSTRUCTIONS.equals(subKey) && !(subKey.startsWith(CRITERIA_PREFIX) && subKey.length() > CRITERIA_PREFIX.length())) {
                throw new ConfigException("Unknown option '" + key + "', supported keys of a question are '" + TYPE + "', '" + INSTRUCTIONS + "' and '" + CRITERIA_PREFIX
                        + "<key>'");
            }
        }
    }

    private static Question parseQuestion(final Configuration config, final String name) {
        final var typeKey = PREFIX + name + "." + TYPE;
        final var instructionsKey = PREFIX + name + "." + INSTRUCTIONS;
        final var type = parseType(typeKey, config.getString(typeKey));
        final var instructions = config.getString(instructionsKey);
        if (instructions == null || instructions.isBlank()) {
            throw new ConfigException("'" + instructionsKey + "' must be set to non-empty value.");
        }
        final var criteria = criteria(config, name);
        return switch (type) {
            case CHOICE -> new ChoiceQuestion(name, instructions, choiceOptions(name, criteria));
            case SCORE -> new ScoreQuestion(name, instructions, scoreLevels(name, criteria));
            case YESNO -> yesNo(name, instructions, criteria);
        };
    }

    private static QuestionType parseType(final String key, final String value) {
        if (value == null || value.isBlank()) {
            throw new ConfigException("'" + key + "' must be set to one of choice, score, yesno, noul.");
        }
        return switch (value.trim().toLowerCase()) {
            case "choice" -> QuestionType.CHOICE;
            case "score" -> QuestionType.SCORE;
            case "yesno", "noul" -> QuestionType.YESNO;
            default -> throw new ConfigException("'" + key + "' has unknown value '" + value + "', use one of choice, score, yesno, noul.");
        };
    }

    private static SortedMap<String, String> criteria(final Configuration config, final String name) {
        final var prefix = PREFIX + name + "." + CRITERIA_PREFIX;
        final SortedMap<String, String> criteria = new TreeMap<>();
        for (final var entry : config.asMap().entrySet()) {
            if (entry.getKey().startsWith(prefix)) {
                criteria.put(entry.getKey().substring(prefix.length()), entry.getValue() == null ? "" : entry.getValue());
            }
        }
        return criteria;
    }

    private static SequencedMap<String, String> choiceOptions(final String name, final SortedMap<String, String> criteria) {
        if (criteria.isEmpty()) {
            throw new ConfigException("Question '" + name + "' of type choice requires at least one option configured as '" + PREFIX + name + "." + CRITERIA_PREFIX
                    + "<option>'.");
        }
        final SequencedMap<String, String> options = new LinkedHashMap<>();
        for (final var criterion : criteria.entrySet()) {
            if (!ChoiceQuestion.isValidOptionName(criterion.getKey())) {
                throw new ConfigException("'" + PREFIX + name + "." + CRITERIA_PREFIX + criterion.getKey() + "' is not a valid option name, it must match "
                        + ChoiceQuestion.OPTION_NAME_PATTERN.pattern());
            }
            options.put(criterion.getKey(), criterion.getValue());
        }
        return options;
    }

    private static List<String> scoreLevels(final String name, final SortedMap<String, String> criteria) {
        final var levels = new TreeMap<Integer, String>();
        for (final var criterion : criteria.entrySet()) {
            if (!LEVEL_INDEX.matcher(criterion.getKey()).matches()) {
                throw new ConfigException(
                        "'" + PREFIX + name + "." + CRITERIA_PREFIX + criterion.getKey() + "' is not a valid level index, a zero based number is expected.");
            }
            levels.put(Integer.parseInt(criterion.getKey()), criterion.getValue());
        }
        if (levels.size() < ScoreQuestion.MIN_LEVELS) {
            throw new ConfigException(
                    "Question '" + name + "' of type score requires at least " + ScoreQuestion.MIN_LEVELS + " levels configured as '" + PREFIX + name + "."
                            + CRITERIA_PREFIX + "0', '" + PREFIX + name + "." + CRITERIA_PREFIX + "1', ...");
        }
        final List<String> result = new ArrayList<>();
        for (final var level : levels.entrySet()) {
            if (level.getKey() != result.size()) {
                throw new ConfigException(
                        "Levels of question '" + name + "' must be contiguous and zero based, but '" + PREFIX + name + "." + CRITERIA_PREFIX + result.size()
                                + "' is missing.");
            }
            result.add(level.getValue());
        }
        return result;
    }

    private static YesNoQuestion yesNo(final String name, final String instructions, final SortedMap<String, String> criteria) {
        for (final var key : criteria.keySet()) {
            if (!"true".equals(key) && !"false".equals(key)) {
                throw new ConfigException("'" + PREFIX + name + "." + CRITERIA_PREFIX + key + "' is not supported for yesno question, use only 'true' and 'false'.");
            }
        }
        return new YesNoQuestion(name, instructions, criteria.get("true"), criteria.get("false"));
    }
}
