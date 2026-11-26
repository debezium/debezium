/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.decision.systemone;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SequencedMap;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import io.debezium.ai.decision.api.ChoiceQuestion;
import io.debezium.ai.decision.api.Question;
import io.debezium.ai.decision.api.ScoreQuestion;
import io.debezium.ai.decision.jev.JevDecisionServiceFactory;
import io.debezium.config.Configuration;

/**
 * Tests of {@link JevDecisionServiceFactory} and {@link SystemOneSettings}.
 *
 * @author Jiri Pechanec
 */
public class JevDecisionServiceFactoryTest {

    private static final String API_KEY = "super-secret-key-9876";

    private final SystemOneDecisionServiceFactory factory = new JevDecisionServiceFactory();

    private static Configuration config(final String... keyValues) {
        final Map<String, String> values = new HashMap<>();
        values.put("jev.api.key", API_KEY);
        for (var i = 0; i < keyValues.length; i += 2) {
            values.put(keyValues[i], keyValues[i + 1]);
        }
        return Configuration.from(values);
    }

    private static Configuration withoutKey(final String... keyValues) {
        final Map<String, String> values = new HashMap<>();
        for (var i = 0; i < keyValues.length; i += 2) {
            values.put(keyValues[i], keyValues[i + 1]);
        }
        return Configuration.from(values);
    }

    @Test
    public void shouldBeNamedJev() {
        assertThat(factory.name()).isEqualTo("jev");
        assertThat(factory.getConfigFields().asArray()).extracting(field -> field.name()).allMatch(name -> name.startsWith("jev."));
    }

    @Test
    public void shouldApplyDefaults() {
        final var settings = factory.settings(config());

        assertThat(settings.apiKey()).isEqualTo(API_KEY);
        assertThat(settings.endpoint()).hasToString("https://api.typesafe.ai/v1/systemone");
        assertThat(settings.model()).isEqualTo("jev-latest");
        assertThat(settings.longContext()).isFalse();
        assertThat(settings.requestTimeout()).isEqualTo(Duration.ofSeconds(10));
        assertThat(settings.connectTimeout()).isEqualTo(Duration.ofSeconds(5));
        assertThat(settings.maxAttempts()).isEqualTo(3);
        assertThat(settings.initialDelay()).isEqualTo(Duration.ofMillis(500));
        assertThat(settings.maxDelay()).isEqualTo(Duration.ofSeconds(10));
    }

    @Test
    public void shouldReadAllOptions() {
        final var settings = factory.settings(config(
                "jev.base.url", "http://localhost:8080/api/",
                "jev.model", "jev-1.0",
                "jev.long.context", "true",
                "jev.request.timeout.ms", "1500",
                "jev.connect.timeout.ms", "700",
                "jev.retry.max.attempts", "5",
                "jev.retry.initial.delay.ms", "0",
                "jev.retry.max.delay.ms", "250"));

        assertThat(settings.endpoint()).hasToString("http://localhost:8080/api/v1/systemone");
        assertThat(settings.model()).isEqualTo("jev-1.0");
        assertThat(settings.longContext()).isTrue();
        assertThat(settings.requestTimeout()).isEqualTo(Duration.ofMillis(1500));
        assertThat(settings.connectTimeout()).isEqualTo(Duration.ofMillis(700));
        assertThat(settings.maxAttempts()).isEqualTo(5);
        assertThat(settings.initialDelay()).isEqualTo(Duration.ZERO);
        assertThat(settings.maxDelay()).isEqualTo(Duration.ofMillis(250));
    }

    @Test
    public void shouldRejectMissingOrBlankKey() {
        assertThatThrownBy(() -> factory.validateConfiguration(withoutKey())).isInstanceOf(ConfigException.class).hasMessageContaining("jev.api.key");
        assertThatThrownBy(() -> factory.validateConfiguration(withoutKey("jev.api.key", "  "))).isInstanceOf(ConfigException.class).hasMessageContaining("jev.api.key");
        assertThatThrownBy(() -> factory.create(withoutKey(), List.of())).isInstanceOf(ConfigException.class).hasMessageContaining("jev.api.key");
    }

    @Test
    public void shouldRejectBlankModel() {
        assertThatThrownBy(() -> factory.validateConfiguration(config("jev.model", " "))).isInstanceOf(ConfigException.class).hasMessageContaining("jev.model");
    }

    @Test
    public void shouldRejectUnsupportedUrlSchemes() {
        for (final var url : List.of("file:///etc/passwd", "ftp://example.com", "jar:file:/x.jar!/", "example.com", "/relative/path")) {
            assertThatThrownBy(() -> factory.validateConfiguration(config("jev.base.url", url)))
                    .as(url)
                    .isInstanceOf(ConfigException.class)
                    .hasMessageContaining("jev.base.url")
                    .hasMessageNotContaining(API_KEY);
        }
    }

    @Test
    public void shouldRejectInvalidUrl() {
        assertThatThrownBy(() -> factory.validateConfiguration(config("jev.base.url", "http://exa mple.com")))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("jev.base.url");
        assertThatThrownBy(() -> factory.validateConfiguration(config("jev.base.url", "http://")))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("jev.base.url");
    }

    @Test
    public void shouldRejectBaseUrlsWithQueriesOrFragments() {
        for (final var url : List.of("https://example.com/proxy?tenant=blue", "https://example.com/proxy#fragment", "https://example.com/proxy?",
                "https://example.com/proxy#")) {
            assertThatThrownBy(() -> factory.validateConfiguration(config("jev.base.url", url)))
                    .as(url)
                    .isInstanceOf(ConfigException.class)
                    .hasMessageContaining("jev.base.url")
                    .hasMessageContaining("query or fragment");
        }
    }

    @Test
    public void shouldRejectInvalidNumbers() {
        for (final var option : List.of("jev.request.timeout.ms", "jev.connect.timeout.ms", "jev.retry.max.attempts")) {
            assertThatThrownBy(() -> factory.validateConfiguration(config(option, "0"))).as(option).isInstanceOf(ConfigException.class).hasMessageContaining(option);
            assertThatThrownBy(() -> factory.validateConfiguration(config(option, "-5"))).as(option).isInstanceOf(ConfigException.class).hasMessageContaining(option);
            assertThatThrownBy(() -> factory.validateConfiguration(config(option, "abc"))).as(option).isInstanceOf(ConfigException.class).hasMessageContaining(option);
        }
        for (final var option : List.of("jev.retry.initial.delay.ms", "jev.retry.max.delay.ms")) {
            assertThatThrownBy(() -> factory.validateConfiguration(config(option, "-1"))).as(option).isInstanceOf(ConfigException.class).hasMessageContaining(option);
        }
    }

    @Test
    public void shouldCreateServiceWithinTheLimits() {
        final SequencedMap<String, String> options = new LinkedHashMap<>();
        for (var i = 0; i < SystemOneRequestMapper.MAX_CHOICE_OPTIONS; i++) {
            options.put("option" + i, "");
        }
        final List<String> levels = new ArrayList<>();
        for (var i = 0; i < 10; i++) {
            levels.add("level " + i);
        }
        final List<Question> questions = List.of(new ChoiceQuestion("choice", "Which one?", options), new ScoreQuestion("score", "How much?", levels));

        try (var service = factory.create(config(), questions)) {
            assertThat(service).isInstanceOf(SystemOneDecisionService.class);
        }
    }

    @Test
    public void shouldRejectTooManyScoreLevels() {
        final List<String> levels = new ArrayList<>();
        for (var i = 0; i < 11; i++) {
            levels.add("level " + i);
        }

        assertThatThrownBy(() -> factory.create(config(), List.of(new ScoreQuestion("score", "How much?", levels))))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("score")
                .hasMessageContaining("11 levels")
                .hasMessageContaining("at most 10");
    }

    @Test
    public void shouldRejectTooManyChoiceOptions() {
        final SequencedMap<String, String> options = new LinkedHashMap<>();
        for (var i = 0; i <= SystemOneRequestMapper.MAX_CHOICE_OPTIONS; i++) {
            options.put("option" + i, "");
        }

        assertThatThrownBy(() -> factory.create(config(), List.of(new ChoiceQuestion("choice", "Which one?", options))))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("choice")
                .hasMessageContaining("256 options")
                .hasMessageContaining("at most 255");
    }

    @Test
    public void shouldNeverExposeTheKey() {
        final var settings = factory.settings(config());
        assertThat(settings.toString()).doesNotContain(API_KEY);

        try (var service = factory.create(config(), List.of(SystemOneRequestMapperTest.emergencyQuestion()))) {
            assertThat(service.toString()).doesNotContain(API_KEY);
        }

        assertThatThrownBy(() -> factory.validateConfiguration(config("jev.request.timeout.ms", "0"))).hasMessageNotContaining(API_KEY);
        assertThatThrownBy(() -> factory.validateConfiguration(config("jev.base.url", "ftp://x"))).hasMessageNotContaining(API_KEY);
    }
}
