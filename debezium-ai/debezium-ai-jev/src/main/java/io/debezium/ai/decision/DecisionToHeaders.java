/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.decision;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.ai.decision.api.Answer;
import io.debezium.ai.decision.api.ChoiceAnswer;
import io.debezium.ai.decision.api.ChoiceQuestion;
import io.debezium.ai.decision.api.DecisionException;
import io.debezium.ai.decision.api.DecisionRequest;
import io.debezium.ai.decision.api.DecisionResponse;
import io.debezium.ai.decision.api.DecisionService;
import io.debezium.ai.decision.api.Question;
import io.debezium.ai.decision.api.ScoreAnswer;
import io.debezium.ai.decision.api.ScoreQuestion;
import io.debezium.ai.decision.api.YesNoAnswer;
import io.debezium.ai.decision.api.YesNoQuestion;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.metadata.ConfigDescriptor;
import io.debezium.transforms.SmtManager;

/**
 * Single message transform which asks an AI decision service a set of questions about selected fields of the record
 * and writes the answers to the record headers in the {@value DecisionHeaders#PREFIX} namespace.
 * The key, the value and the schemas of the record are never modified.
 * <p>
 * All the questions are evaluated by a single request to the decision service per record.
 * The decision service is provided by a {@link io.debezium.ai.decision.spi.DecisionServiceFactory}
 * selected by the {@code decision.provider} option.
 *
 * @author Jiri Pechanec
 */
public class DecisionToHeaders<R extends ConnectRecord<R>> implements Transformation<R>, Versioned, ConfigDescriptor {

    private static final Logger LOGGER = LoggerFactory.getLogger(DecisionToHeaders.class);

    private static final String DEFAULT_PROVIDER = "jev";

    private static final Field PROVIDER = Field.create("decision.provider")
            .withDisplayName("Decision provider.")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDefault(DEFAULT_PROVIDER)
            .withDescription("Name of the decision service provider, discovered via the Java service loader. The default is '" + DEFAULT_PROVIDER + "'.");

    private static final Field STATE_FIELDS = Field.create("state.fields")
            .withDisplayName("Fields forming the state.")
            .withType(ConfigDef.Type.LIST)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .required()
            .withDescription("Comma separated paths of the fields of the record value envelope which are sent to the decision service, "
                    + "nested fields are separated by a dot, e.g. 'after.name,after.description' or 'before.description,after.description'.");

    private static final Field STATE_FORMAT = Field.create("state.format")
            .withDisplayName("State format.")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withEnum(StateFormat.class, StateFormat.JSON)
            .withDescription("Format of the state sent to the decision service. With 'json' the state is an object keyed by the configured field paths, "
                    + "with 'text' the non-null values are converted to strings and joined by a new line.");

    private static final Field QUESTIONS = Field.create(QuestionConfigParser.QUESTIONS_KEY)
            .withDisplayName("Questions.")
            .withType(ConfigDef.Type.LIST)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .required()
            .withDescription("Comma separated names of the questions to be answered for every record. Every question is described by the options "
                    + "'questions.<name>.type' (choice, score, yesno or noul), 'questions.<name>.instructions' and "
                    + "'questions.<name>.criteria.<key>'. The names must match [A-Za-z][A-Za-z0-9_-]{0,63} and 'model', 'status' and 'error' are reserved.");

    private static final Field HEADERS_DETAILS = Field.create("headers.details")
            .withDisplayName("Write detail headers.")
            .withType(ConfigDef.Type.BOOLEAN)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(true)
            .withDescription("Whether to write also the '.confidence' and '.probabilities' headers of the questions.");

    private static final Field ERROR_MODE = Field.create("decision.error.mode")
            .withDisplayName("Error mode.")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withEnum(ErrorMode.class, ErrorMode.FAIL)
            .withDescription(
                    "Reaction to a failed evaluation. With 'fail' the transformation throws an exception, with 'skip' it logs a warning and passes the record unchanged, "
                            + "with 'header' it logs a warning and writes the '" + DecisionHeaders.STATUS + "' and '" + DecisionHeaders.ERROR + "' headers.");

    public static final Field.Set ALL_FIELDS = Field.setOf(PROVIDER, STATE_FIELDS, STATE_FORMAT, QUESTIONS, HEADERS_DETAILS, ERROR_MODE)
            .with(DecisionServiceFactories.allConfigFields());

    private SmtManager<R> smtManager;
    private StateBuilder stateBuilder;
    private List<Question> questions;
    private DecisionHeaderWriter headerWriter;
    private ErrorMode errorMode;
    private DecisionService decisionService;

    @Override
    public void configure(final Map<String, ?> configs) {
        final var config = Configuration.from(configs);
        smtManager = new SmtManager<>(config);
        smtManager.validate(config, ALL_FIELDS);

        final var factory = DecisionServiceFactories.find(config.getString(PROVIDER));
        final var stateFields = config.getList(STATE_FIELDS);
        validateStateFields(stateFields);
        stateBuilder = new StateBuilder(stateFields, StateFormat.parse(config.getString(STATE_FORMAT)));
        questions = QuestionConfigParser.parse(config);
        headerWriter = new DecisionHeaderWriter(questions, config.getBoolean(HEADERS_DETAILS));
        errorMode = ErrorMode.parse(config.getString(ERROR_MODE));

        factory.validateConfiguration(config);
        decisionService = factory.create(config, questions);
    }

    @Override
    public R apply(final R record) {
        if (record.value() == null || !smtManager.isValidEnvelope(record) || !(record.value() instanceof Struct value)) {
            LOGGER.trace("Record of topic {} has null value or invalid envelope and will be skipped.", record.topic());
            return record;
        }

        final var state = stateBuilder.build(value);
        if (state == null) {
            LOGGER.trace("Record of topic {} has none of the state fields and will be skipped.", record.topic());
            return record;
        }

        final DecisionResponse response;
        try {
            response = decisionService.decide(new DecisionRequest(state, questions));
            verify(response);
        }
        catch (DecisionException e) {
            return onError(record, e);
        }
        return withHeaders(record, headerWriter.success(record.headers(), response));
    }

    @Override
    public ConfigDef config() {
        final var config = new ConfigDef();
        Field.group(config, null, ALL_FIELDS.asArray());
        return config;
    }

    @Override
    public void close() {
        if (decisionService != null) {
            decisionService.close();
        }
    }

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public Field.Set getConfigFields() {
        return ALL_FIELDS;
    }

    private static void validateStateFields(final List<String> stateFields) {
        if (stateFields.isEmpty()) {
            throw new ConfigException("'" + STATE_FIELDS.name() + "' must be set to a non-empty list of field paths.");
        }
        for (final var path : stateFields) {
            if (path.isBlank() || path.startsWith(".") || path.endsWith(".") || path.contains("..")) {
                throw new ConfigException("'" + STATE_FIELDS.name() + "' contains invalid field path '" + path + "'.");
            }
        }
    }

    private R onError(final R record, final DecisionException e) {
        final var coordinates = "topic '" + record.topic() + "', partition " + record.kafkaPartition() + ", timestamp " + record.timestamp();
        return switch (errorMode) {
            case FAIL -> throw new DebeziumException("Decision evaluation failed for record from " + coordinates + ": " + e.getMessage(), e);
            case SKIP -> {
                LOGGER.warn("Decision evaluation failed for record from {}, the record is passed unchanged: {}", coordinates, e.getMessage());
                yield record;
            }
            case HEADER -> {
                LOGGER.warn("Decision evaluation failed for record from {}, the failure is recorded in the headers: {}", coordinates, e.getMessage());
                yield withHeaders(record, headerWriter.error(record.headers(), e.getMessage()));
            }
        };
    }

    private R withHeaders(final R record, final Headers headers) {
        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                record.valueSchema(),
                record.value(),
                record.timestamp(),
                headers);
    }

    /**
     * Checks that the response contains exactly one valid answer of the right kind for every requested question.
     */
    private void verify(final DecisionResponse response) {
        final Set<String> expected = new HashSet<>();
        for (final var question : questions) {
            expected.add(question.name());
            final var answer = response.answers().get(question.name());
            if (answer == null) {
                throw invalid("Missing answer to question '" + question.name() + "'");
            }
            verify(question, answer);
        }
        for (final var name : response.answers().keySet()) {
            if (!expected.contains(name)) {
                throw invalid("Unexpected answer to unknown question '" + name + "'");
            }
        }
    }

    private static void verify(final Question question, final Answer answer) {
        if (!question.name().equals(answer.question())) {
            throw invalid("Answer to question '" + answer.question() + "' is reported as answer to question '" + question.name() + "'");
        }
        switch (question) {
            case ChoiceQuestion choiceQuestion -> {
                if (!(answer instanceof ChoiceAnswer choiceAnswer)) {
                    throw wrongKind(question, "choice");
                }
                if (!choiceQuestion.options().containsKey(choiceAnswer.choice())) {
                    throw invalid("Answer to question '" + question.name() + "' is not one of the configured options");
                }
                if (!choiceAnswer.probabilities().keySet().equals(choiceQuestion.options().keySet())) {
                    throw invalid("Probability keys for question '" + question.name() + "' must match the configured options");
                }
            }
            case ScoreQuestion scoreQuestion -> {
                if (!(answer instanceof ScoreAnswer scoreAnswer)) {
                    throw wrongKind(question, "score");
                }
                final var levelCount = scoreQuestion.levels().size();
                if (scoreAnswer.score() < 0 || scoreAnswer.score() > levelCount - 1) {
                    throw invalid("Score of question '" + question.name() + "' is outside of the configured levels");
                }
                final var levels = scoreAnswer.probabilities().keySet();
                if (levels.size() != levelCount || levels.stream().anyMatch(level -> level < 0 || level >= levelCount)) {
                    throw invalid("Probability keys for question '" + question.name() + "' must match the configured levels");
                }
            }
            case YesNoQuestion yesNoQuestion -> {
                if (!(answer instanceof YesNoAnswer)) {
                    throw wrongKind(question, "yesno");
                }
            }
        }
    }

    private static DecisionException wrongKind(final Question question, final String expectedKind) {
        return invalid("Answer to question '" + question.name() + "' is not a " + expectedKind + " answer");
    }

    private static DecisionException invalid(final String message) {
        return new DecisionException("Invalid decision response: " + message, false);
    }
}
