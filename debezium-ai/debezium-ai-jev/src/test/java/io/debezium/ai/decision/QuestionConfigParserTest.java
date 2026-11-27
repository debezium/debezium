/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.decision;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import io.debezium.ai.decision.api.ChoiceQuestion;
import io.debezium.ai.decision.api.ScoreQuestion;
import io.debezium.ai.decision.api.YesNoQuestion;
import io.debezium.config.Configuration;

/**
 * Tests of {@link QuestionConfigParser}.
 *
 * @author Jiri Pechanec
 */
public class QuestionConfigParserTest {

    private static Configuration config(final String... keysAndValues) {
        final Map<String, String> props = new HashMap<>();
        for (var i = 0; i < keysAndValues.length; i += 2) {
            props.put(keysAndValues[i], keysAndValues[i + 1]);
        }
        return Configuration.from(props);
    }

    @Test
    public void shouldParseChoice() {
        final var questions = QuestionConfigParser.parse(config(
                "questions", "category",
                "questions.category.type", "choice",
                "questions.category.instructions", "Which team?",
                "questions.category.criteria.SAFETY", "Hazards",
                "questions.category.criteria.SHIPPING", ""));

        assertThat(questions).hasSize(1);
        final var question = (ChoiceQuestion) questions.get(0);
        assertThat(question.name()).isEqualTo("category");
        assertThat(question.instructions()).isEqualTo("Which team?");
        assertThat(question.options()).containsEntry("SAFETY", "Hazards").containsEntry("SHIPPING", "");
        assertThat(question.options().keySet()).containsExactly("SAFETY", "SHIPPING");
    }

    @Test
    public void shouldParseScoreInIndexOrder() {
        final var questions = QuestionConfigParser.parse(config(
                "questions", "severity",
                "questions.severity.type", "SCORE",
                "questions.severity.instructions", "How severe?",
                "questions.severity.criteria.2", "high",
                "questions.severity.criteria.0", "low",
                "questions.severity.criteria.1", "medium"));

        assertThat(((ScoreQuestion) questions.get(0)).levels()).containsExactly("low", "medium", "high");
    }

    @Test
    public void shouldParseYesNoAndItsAlias() {
        final var questions = QuestionConfigParser.parse(config(
                "questions", " emergency , billing ",
                "questions.emergency.type", "yesno",
                "questions.emergency.instructions", "Is it an emergency?",
                "questions.emergency.criteria.true", "Immediate danger",
                "questions.billing.type", "noul",
                "questions.billing.instructions", "Is it about billing?"));

        assertThat(questions).hasSize(2);
        final var emergency = (YesNoQuestion) questions.get(0);
        assertThat(emergency.yesDescription()).isEqualTo("Immediate danger");
        assertThat(emergency.noDescription()).isNull();
        final var billing = (YesNoQuestion) questions.get(1);
        assertThat(billing.name()).isEqualTo("billing");
        assertThat(billing.yesDescription()).isNull();
    }

    @Test
    public void shouldRejectMissingQuestions() {
        assertThatThrownBy(() -> QuestionConfigParser.parse(config())).isInstanceOf(ConfigException.class).hasMessageContaining("questions");
        assertThatThrownBy(() -> QuestionConfigParser.parse(config("questions", " "))).isInstanceOf(ConfigException.class).hasMessageContaining("questions");
    }

    @Test
    public void shouldRejectDuplicateNames() {
        assertThatThrownBy(() -> QuestionConfigParser.parse(config("questions", "a,b,a"))).isInstanceOf(ConfigException.class)
                .hasMessageContaining("duplicate").hasMessageContaining("'a'");
    }

    @Test
    public void shouldRejectInvalidNames() {
        assertThatThrownBy(() -> QuestionConfigParser.parse(config("questions", "a.b"))).isInstanceOf(ConfigException.class).hasMessageContaining("a.b");
        assertThatThrownBy(() -> QuestionConfigParser.parse(config("questions", "a,,b"))).isInstanceOf(ConfigException.class).hasMessageContaining("invalid");
        assertThatThrownBy(() -> QuestionConfigParser.parse(config("questions", "9x"))).isInstanceOf(ConfigException.class).hasMessageContaining("9x");
    }

    @Test
    public void shouldRejectReservedNames() {
        for (final var reserved : new String[]{ "model", "status", "error" }) {
            assertThatThrownBy(() -> QuestionConfigParser.parse(config("questions", reserved))).isInstanceOf(ConfigException.class)
                    .hasMessageContaining("reserved").hasMessageContaining(reserved);
        }
    }

    @Test
    public void shouldRejectMissingOrUnknownType() {
        assertThatThrownBy(() -> QuestionConfigParser.parse(config("questions", "q", "questions.q.instructions", "text")))
                .isInstanceOf(ConfigException.class).hasMessageContaining("questions.q.type");
        assertThatThrownBy(() -> QuestionConfigParser.parse(config("questions", "q", "questions.q.type", "rank", "questions.q.instructions", "text")))
                .isInstanceOf(ConfigException.class).hasMessageContaining("questions.q.type").hasMessageContaining("rank");
    }

    @Test
    public void shouldRejectMissingOrBlankInstructions() {
        assertThatThrownBy(() -> QuestionConfigParser.parse(config("questions", "q", "questions.q.type", "yesno")))
                .isInstanceOf(ConfigException.class).hasMessageContaining("questions.q.instructions");
        assertThatThrownBy(() -> QuestionConfigParser.parse(config("questions", "q", "questions.q.type", "yesno", "questions.q.instructions", "  ")))
                .isInstanceOf(ConfigException.class).hasMessageContaining("questions.q.instructions");
    }

    @Test
    public void shouldRejectChoiceWithoutOptions() {
        assertThatThrownBy(() -> QuestionConfigParser.parse(config("questions", "q", "questions.q.type", "choice", "questions.q.instructions", "text")))
                .isInstanceOf(ConfigException.class).hasMessageContaining("questions.q.criteria");
    }

    @Test
    public void shouldRejectBadOptionName() {
        assertThatThrownBy(() -> QuestionConfigParser.parse(config("questions", "q", "questions.q.type", "choice", "questions.q.instructions", "text",
                "questions.q.criteria.a.b", "x")))
                .isInstanceOf(ConfigException.class).hasMessageContaining("questions.q.criteria.a.b");
    }

    @Test
    public void shouldRejectScoreGap() {
        assertThatThrownBy(() -> QuestionConfigParser.parse(config("questions", "q", "questions.q.type", "score", "questions.q.instructions", "text",
                "questions.q.criteria.0", "a", "questions.q.criteria.1", "b", "questions.q.criteria.3", "d")))
                .isInstanceOf(ConfigException.class).hasMessageContaining("questions.q.criteria.2").hasMessageContaining("missing");
    }

    @Test
    public void shouldRejectScoreNonNumericKey() {
        assertThatThrownBy(() -> QuestionConfigParser.parse(config("questions", "q", "questions.q.type", "score", "questions.q.instructions", "text",
                "questions.q.criteria.0", "a", "questions.q.criteria.high", "b")))
                .isInstanceOf(ConfigException.class).hasMessageContaining("questions.q.criteria.high");
    }

    @Test
    public void shouldRejectScoreWithOneLevel() {
        assertThatThrownBy(() -> QuestionConfigParser.parse(config("questions", "q", "questions.q.type", "score", "questions.q.instructions", "text",
                "questions.q.criteria.0", "a")))
                .isInstanceOf(ConfigException.class).hasMessageContaining("at least 2");
    }

    @Test
    public void shouldRejectYesNoUnknownCriteria() {
        assertThatThrownBy(() -> QuestionConfigParser.parse(config("questions", "q", "questions.q.type", "yesno", "questions.q.instructions", "text",
                "questions.q.criteria.maybe", "x")))
                .isInstanceOf(ConfigException.class).hasMessageContaining("questions.q.criteria.maybe");
    }

    @Test
    public void shouldRejectKeysOfUnlistedQuestion() {
        assertThatThrownBy(() -> QuestionConfigParser.parse(config("questions", "q", "questions.q.type", "yesno", "questions.q.instructions", "text",
                "questions.x.type", "yesno")))
                .isInstanceOf(ConfigException.class).hasMessageContaining("questions.x.type").hasMessageContaining("not listed");
    }

    @Test
    public void shouldRejectUnknownSubKey() {
        assertThatThrownBy(() -> QuestionConfigParser.parse(config("questions", "q", "questions.q.type", "yesno", "questions.q.instructions", "text",
                "questions.q.instruction", "typo")))
                .isInstanceOf(ConfigException.class).hasMessageContaining("questions.q.instruction");
        assertThatThrownBy(() -> QuestionConfigParser.parse(config("questions", "q", "questions.q.type", "yesno", "questions.q.instructions", "text",
                "questions.q", "typo")))
                .isInstanceOf(ConfigException.class).hasMessageContaining("questions.q");
    }
}
