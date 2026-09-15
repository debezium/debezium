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
import dev.langchain4j.model.openai.OpenAiEmbeddingModel;

/**
 * {@link EmbeddingsModelFactory}  for {@link FieldToEmbedding} SMT, which uses <a href="https://platform.openai.com/">OpenAI</a>
 * for serving embedding models.
 *
 * @author kmos
 */
public class OpenAiModelFactory<R extends ConnectRecord<R>> implements EmbeddingsModelFactory {

    private static final int DEFAULT_OPERATION_TIMEOUT = 15_000;

    public static final String OPENAI_PREFIX = "openai.";

    private static final Field OPENAI_BASE_URL = Field.create(OPENAI_PREFIX + "baseUrl")
            .withDisplayName("OpenAI base URL.")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Base URL for OpenAI client. If not provided, default OpenAI client URL will be used.")
            .withDeprecatedAliases(OPENAI_PREFIX + "base.url");

    private static final Field OPENAI_API_KEY = Field.create(OPENAI_PREFIX + "api.key")
            .withDisplayName("OpenAI API key.")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("API key for OpenAI API.")
            .required();

    private static final Field OPENAI_ORGANIZATION_ID = Field.create(OPENAI_PREFIX + "organization.id")
            .withDisplayName("OpenAI organization ID.")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Organization ID for OpenAI API.");

    private static final Field MODEL_NAME = Field.create(OPENAI_PREFIX + "model.name")
            .withDisplayName("Model name.")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Name of the OpenAI embedding model which should be used.")
            .required();

    private static final Field OPERATION_TIMEOUT = Field.create(OPENAI_PREFIX + "operation.timeout.ms")
            .withDisplayName("Operation timeout.")
            .withType(ConfigDef.Type.INT)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDefault(DEFAULT_OPERATION_TIMEOUT)
            .withDescription("Milliseconds to wait for OpenAI calculations to finish (defaults to %s).".formatted(DEFAULT_OPERATION_TIMEOUT))
            .withValidation(Field::isNonNegativeInteger);

    private static final Field LOG_REQUESTS = Field.create(OPENAI_PREFIX + "log.requests")
            .withDisplayName("Log OpenAI requests.")
            .withType(ConfigDef.Type.BOOLEAN)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(false)
            .withDescription("Whether to log requests sent to OpenAI API. Should be used with caution in production as it may expose sensitive data.");

    private static final Field LOG_RESPONSES = Field.create(OPENAI_PREFIX + "log.responses")
            .withDisplayName("Log OpenAI responses.")
            .withType(ConfigDef.Type.BOOLEAN)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(false)
            .withDescription("Whether to log responses received from OpenAI API. Should be used with caution in production as it may expose sensitive data.");

    public static final Field.Set ALL_FIELDS = Field.setOf(OPENAI_BASE_URL, OPENAI_API_KEY, OPENAI_ORGANIZATION_ID, MODEL_NAME, OPERATION_TIMEOUT, LOG_REQUESTS,
            LOG_RESPONSES);

    private String baseUrl;
    private String apiKey;
    private String organizationId;
    private String modelName;
    private int operationTimeout;
    private boolean logRequests;
    private boolean logResponses;

    @Override
    public Field.Set getConfigFields() {
        return ALL_FIELDS;
    }

    @Override
    public void configure(Configuration config) {
        baseUrl = config.getString(OPENAI_BASE_URL);
        apiKey = config.getString(OPENAI_API_KEY);
        organizationId = config.getString(OPENAI_ORGANIZATION_ID);
        modelName = config.getString(MODEL_NAME);
        operationTimeout = config.getInteger(OPERATION_TIMEOUT);
        logRequests = config.getBoolean(LOG_REQUESTS);
        logResponses = config.getBoolean(LOG_RESPONSES);
    }

    @Override
    public void validateConfiguration() {
        if (Strings.isNullOrBlank(apiKey)) {
            throw new ConfigException(format("'%s' must be set to non-empty value.", OPENAI_API_KEY));
        }
        if (Strings.isNullOrBlank(modelName)) {
            throw new ConfigException(format("'%s' must be set to non-empty value.", MODEL_NAME));
        }
    }

    @Override
    public EmbeddingModel getModel() {
        OpenAiEmbeddingModel.OpenAiEmbeddingModelBuilder builder = OpenAiEmbeddingModel.builder()
                .apiKey(apiKey)
                .modelName(modelName)
                .timeout(Duration.ofMillis(operationTimeout))
                .logRequests(logRequests)
                .logResponses(logResponses);
        if (!Strings.isNullOrBlank(baseUrl)) {
            builder.baseUrl(baseUrl);
        }
        if (!Strings.isNullOrBlank(organizationId)) {
            builder.organizationId(organizationId);
        }
        return builder.build();
    }
}