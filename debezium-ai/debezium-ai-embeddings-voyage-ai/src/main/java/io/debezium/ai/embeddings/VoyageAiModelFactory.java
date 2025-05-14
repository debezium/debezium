/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.embeddings;

import static io.debezium.ai.embeddings.FieldToEmbedding.EMBEDDINGS_PREFIX;
import static java.lang.String.format;

import java.time.Duration;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.util.Strings;

import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.voyageai.VoyageAiEmbeddingModel;

/**
 * {@link EmbeddingsModelFactory}  for {@link FieldToEmbedding} SMT, which uses <a href="https://docs.voyageai.com/docs/embeddings">Voyage AI</a>
 * for serving embedding models.
 *
 * @author vjuranek
 */
public class VoyageAiModelFactory<R extends ConnectRecord<R>> implements EmbeddingsModelFactory {

    private static final int DEFAULT_OPERATION_TIMEOUT = 15_000;

    public static final String VOYAGE_AI_PREFIX = EMBEDDINGS_PREFIX + "voyageai.";

    private static final Field VOYAGE_AI_BASE_URL = Field.create(VOYAGE_AI_PREFIX + "baseUrl")
            .withDisplayName("Hugging Face base URL.")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Base URL for Hugging Face client. If not provided, default Hugging Face client URL will be used.");

    private static final Field VOYAGE_AI_ACCESS_TOKEN = Field.create(VOYAGE_AI_PREFIX + "access.token")
            .withDisplayName("Hugging Face API access token.")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Access token for Hugging Face API.")
            .required();

    private static final Field MODEL_NAME = Field.create(VOYAGE_AI_PREFIX + "model.name")
            .withDisplayName("Model name.")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Name of the Hugging Face model which should be used.")
            .required();

    private static final Field OPERATION_TIMEOUT = Field.create(VOYAGE_AI_PREFIX + "operation.timeout.ms")
            .withDisplayName("Operation timeout.")
            .withType(ConfigDef.Type.INT)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDefault(DEFAULT_OPERATION_TIMEOUT)
            .withDescription("Milliseconds to wait for Hugging Face calculations to finish (defaults to %s).".formatted(DEFAULT_OPERATION_TIMEOUT));

    public static final Field.Set ALL_FIELDS = Field.setOf(VOYAGE_AI_BASE_URL, VOYAGE_AI_ACCESS_TOKEN, MODEL_NAME, OPERATION_TIMEOUT);

    private String baseUrl;
    private String accessToken;
    private String modelName;
    private int operationTimeout;

    @Override
    public Field.Set getConfigFields() {
        return ALL_FIELDS;
    }

    @Override
    public void configure(Configuration config) {
        baseUrl = config.getString(VOYAGE_AI_BASE_URL);
        accessToken = config.getString(VOYAGE_AI_ACCESS_TOKEN);
        modelName = config.getString(MODEL_NAME);
        operationTimeout = config.getInteger(OPERATION_TIMEOUT);
    }

    @Override
    public void validateConfiguration() {
        if (accessToken == null || accessToken.isBlank()) {
            throw new ConfigException(format("'%s' must be set to non-empty value.", VOYAGE_AI_ACCESS_TOKEN));
        }
        if (modelName.isBlank() || modelName.isBlank()) {
            throw new ConfigException(format("'%s' must be set to non-empty value.", MODEL_NAME));
        }
    }

    @Override
    public EmbeddingModel getModel() {
        VoyageAiEmbeddingModel.Builder builder = VoyageAiEmbeddingModel.builder()
                .apiKey(accessToken)
                .modelName(modelName)
                .timeout(Duration.ofMillis(operationTimeout))
                .logRequests(true)
                .logResponses(true);
        if (!Strings.isNullOrBlank(baseUrl)) {
            builder.baseUrl(baseUrl);
        }
        return builder.build();
    }
}
