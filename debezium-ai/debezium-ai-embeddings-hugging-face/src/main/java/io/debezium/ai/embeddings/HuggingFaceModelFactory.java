/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.embeddings;

import static java.lang.String.format;

import java.time.Duration;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.util.Strings;

import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.huggingface.HuggingFaceEmbeddingModel;

/**
 * {@link EmbeddingsModelFactory}  for {@link FieldToEmbedding} SMT, which uses
 * <a href="https://huggingface.co/docs/inference-providers/providers/hf-inference/">Hugging Face serverless inference</a> for serving embedding models.
 *
 * @author vjuranek
 */
public class HuggingFaceModelFactory<R extends ConnectRecord<R>> implements EmbeddingsModelFactory {

    private static final int DEFAULT_OPERATION_TIMEOUT = 15_000;

    public static final String HUGGING_FACE_PREFIX = "huggingface.";

    private static final Field HUGGING_FACE_BASE_URL = Field.create(HUGGING_FACE_PREFIX + "baseUrl")
            .withDisplayName("Hugging Face base URL.")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Base URL for Hugging Face client. If not provided, default Hugging Face client URL will be used.");

    private static final Field HUGGING_FACE_ACCESS_TOKEN = Field.create(HUGGING_FACE_PREFIX + "access.token")
            .withDisplayName("Hugging Face API access token.")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Access token for Hugging Face API.")
            .required();

    private static final Field MODEL_NAME = Field.create(HUGGING_FACE_PREFIX + "model.name")
            .withDisplayName("Model name.")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Name of the Hugging Face model which should be used.")
            .required();

    private static final Field OPERATION_TIMEOUT = Field.create(HUGGING_FACE_PREFIX + "operation.timeout.ms")
            .withDisplayName("Operation timeout.")
            .withType(ConfigDef.Type.INT)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDefault(DEFAULT_OPERATION_TIMEOUT)
            .withDescription("Milliseconds to wait for Hugging Face calculations to finish (defaults to %s).".formatted(DEFAULT_OPERATION_TIMEOUT));

    public static final Field.Set ALL_FIELDS = Field.setOf(HUGGING_FACE_BASE_URL, HUGGING_FACE_ACCESS_TOKEN, MODEL_NAME, OPERATION_TIMEOUT);

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
        baseUrl = config.getString(HUGGING_FACE_BASE_URL);
        accessToken = config.getString(HUGGING_FACE_ACCESS_TOKEN);
        modelName = config.getString(MODEL_NAME);
        operationTimeout = config.getInteger(OPERATION_TIMEOUT);
    }

    @Override
    public void validateConfiguration() {
        if (Strings.isNullOrBlank(accessToken)) {
            throw new ConfigException(format("'%s' must be set to non-empty value.", HUGGING_FACE_ACCESS_TOKEN));
        }
        if (Strings.isNullOrBlank(modelName)) {
            throw new ConfigException(format("'%s' must be set to non-empty value.", MODEL_NAME));
        }
    }

    @Override
    public EmbeddingModel getModel() {
        HuggingFaceEmbeddingModel.HuggingFaceEmbeddingModelBuilder builder = HuggingFaceEmbeddingModel.builder()
                .accessToken(accessToken)
                .modelId(modelName)
                .waitForModel(true)
                .timeout(Duration.ofMillis(operationTimeout));
        if (!Strings.isNullOrBlank(baseUrl)) {
            builder.baseUrl(baseUrl);
        }
        return builder.build();
    }
}
