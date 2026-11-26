/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.decision.systemone;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;
import java.util.List;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import io.debezium.ai.decision.api.ScoreQuestion;
import io.debezium.ai.decision.kev.KevDecisionServiceFactory;
import io.debezium.config.Configuration;

/**
 * Tests the defaults and question limits of the Kev provider.
 *
 * @author Jiri Pechanec
 */
public class KevDecisionServiceFactoryTest {

    private final SystemOneDecisionServiceFactory factory = new KevDecisionServiceFactory();

    @Test
    public void shouldDefaultToTheLocalEndpointWithoutCredentials() {
        final var settings = factory.settings(Configuration.create().build());
        assertThat(settings.endpoint()).hasToString("http://127.0.0.1:8009/v1/systemone");
        assertThat(settings.model()).isEqualTo("kev-latest");
        assertThat(settings.apiKey()).isNull();
        assertThat(settings.longContext()).isFalse();
    }

    @Test
    public void shouldSupportUpTo255ScoreLevels() {
        final var config = Configuration.create().build();
        final var supported = new ScoreQuestion("score", "How severe is the defect?", Collections.nCopies(255, "level"));
        final var unsupported = new ScoreQuestion("score", "How severe is the defect?", Collections.nCopies(256, "level"));

        try (var service = factory.create(config, List.of(supported))) {
            assertThat(service).isInstanceOf(SystemOneDecisionService.class);
        }
        assertThatThrownBy(() -> factory.create(config, List.of(unsupported)))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Kev supports at most 255");
    }
}
