/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.embeddings;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;

import dev.langchain4j.model.embedding.EmbeddingModel;

/**
 * Unit tests for {@link OpenAiModelFactory}.
 *
 * @author bhuvan-somisetty
 */
public class OpenAiModelFactoryTest {

    @FixFor("debezium/dbz#2627")
    @Test
    public void testMissingApiKeyThrowsCleanConfigException() {
        OpenAiModelFactory factory = new OpenAiModelFactory();
        factory.configure(Configuration.from(Map.of(
                "openai.model.name", "text-embedding-3-small")));

        assertThatThrownBy(factory::validateConfiguration)
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("'openai.api.key' must be set to non-empty value.");
    }

    @FixFor("debezium/dbz#2627")
    @Test
    public void testMissingModelNameThrowsCleanConfigException() {
        OpenAiModelFactory factory = new OpenAiModelFactory();
        factory.configure(Configuration.from(Map.of(
                "openai.api.key", "sk-test-key")));

        assertThatThrownBy(factory::validateConfiguration)
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("'openai.model.name' must be set to non-empty value.");
    }

    @FixFor("debezium/dbz#2627")
    @Test
    public void testGetModelWithDefaultAndCustomLogging() {
        OpenAiModelFactory factory = new OpenAiModelFactory();
        factory.configure(Configuration.from(Map.of(
                "openai.api.key", "sk-test-key",
                "openai.model.name", "text-embedding-3-small",
                "openai.log.requests", "false",
                "openai.log.responses", "false")));
        factory.validateConfiguration();

        EmbeddingModel model = factory.getModel();
        assertThat(model).isNotNull();
    }

    @FixFor("debezium/dbz#2627")
    @Test
    public void testBaseUrlAliasSupport() {
        OpenAiModelFactory factory = new OpenAiModelFactory();
        factory.configure(Configuration.from(Map.of(
                "openai.api.key", "sk-test-key",
                "openai.model.name", "text-embedding-3-small",
                "openai.base.url", "https://custom-openai-proxy.example.com/v1")));
        factory.validateConfiguration();

        EmbeddingModel model = factory.getModel();
        assertThat(model).isNotNull();
    }
}
