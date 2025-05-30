/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.embeddings;

import static io.debezium.ai.embeddings.FieldToEmbedding.EMBEDDINGS_PREFIX;
import static java.lang.String.format;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;

import io.debezium.config.Configuration;
import io.debezium.config.Field;

import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.ollama.OllamaEmbeddingModel;

/**
 * {@link EmbeddingsModelFactory}  for {@link FieldToEmbedding} SMT, which uses <a href="https://ollama.com/">Ollama</a> for serving embedding models.
 *
 * @author vjuranek
 */
public class OllamaModelFactory<R extends ConnectRecord<R>> implements EmbeddingsModelFactory {

    public static final String OLLAMA_PREFIX = EMBEDDINGS_PREFIX + "ollama.";

    private static final Field OLLAMA_BASE_URL = Field.create(OLLAMA_PREFIX + "url")
            .withDisplayName("Ollama server URL.")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Base URL of Ollama server.")
            .required();

    private static final Field MODEL_NAME = Field.create(OLLAMA_PREFIX + "model.name")
            .withDisplayName("Model name.")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Name of the model which should be served by Ollama server.")
            .required();

    public static final Field.Set ALL_FIELDS = Field.setOf(OLLAMA_BASE_URL, MODEL_NAME);

    private String baseUrl;
    private String modelName;

    @Override
    public Field.Set getConfigFields() {
        return ALL_FIELDS;
    }

    @Override
    public void configure(Configuration config) {
        baseUrl = config.getString(OLLAMA_BASE_URL);
        modelName = config.getString(MODEL_NAME);
    }

    @Override
    public void validateConfiguration() {
        if (baseUrl == null || baseUrl.isBlank()) {
            throw new ConfigException(format("'%s' must be set to non-empty value.", OLLAMA_BASE_URL));
        }
        if (modelName.isBlank() || modelName.isBlank()) {
            throw new ConfigException(format("'%s' must be set to non-empty value.", MODEL_NAME));
        }
    }

    @Override
    public EmbeddingModel getModel() {
        return OllamaEmbeddingModel.builder()
                .baseUrl(baseUrl)
                .modelName(modelName)
                .build();
    }
}
