/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.kafka;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.testing.system.tools.ConfigProperties;
import io.debezium.testing.system.tools.certificateutil.CertUtil;
import io.debezium.testing.system.tools.databases.mongodb.sharded.OcpMongoCertGenerator;
import io.strimzi.api.kafka.model.connector.KafkaConnector;
import io.strimzi.api.kafka.model.connector.KafkaConnectorBuilder;

/**
 *
 * @author Jakub Cechacek
 */
public class ConnectorConfigBuilder {
    private final Map<String, Object> config;
    private final ObjectMapper mapper;
    private final String connectorName;

    public ConnectorConfigBuilder(String connectorName) {
        this.mapper = new ObjectMapper();
        this.config = new HashMap<>();
        this.connectorName = connectorName;
    }

    public String getConnectorName() {
        return connectorName;
    }

    public String getDbServerName() {
        return connectorName.replace('-', '_');
    }

    public ConnectorConfigBuilder put(String key, Object value) {
        config.put(key, value);
        return this;
    }

    public Map<String, Object> get() {
        return config;
    }

    public String getAsString(String key) {
        return String.valueOf(config.get(key));
    }

    public ConnectorConfigBuilder addApicurioAvroSupport(String apicurioUrl) {
        config.put("key.converter", "io.apicurio.registry.utils.converter.AvroConverter");
        config.put("key.converter.apicurio.registry.url", apicurioUrl);
        config.put("key.converter.apicurio.registry.auto-register", true);
        config.put("key.converter.apicurio.registry.find-latest", true);

        config.put("value.converter", "io.apicurio.registry.utils.converter.AvroConverter");
        config.put("value.converter.apicurio.registry.url", apicurioUrl);
        config.put("value.converter.apicurio.registry.auto-register", true);
        config.put("value.converter.apicurio.registry.find-latest", true);

        config.put("schema.name.adjustment.mode", "avro");

        return this;
    }

    public ConnectorConfigBuilder addContentBasedRouter(String expression, String topicNamePattern) {
        config.put("transforms", "route");
        config.put("transforms.route.type", "io.debezium.transforms.ContentBasedRouter");
        config.put("transforms.route.language", "jsr223.groovy");
        config.put("transforms.route.topic.expression", expression);
        config.put("transforms.route.predicate", "TopicPredicate");
        config.put("predicates", "TopicPredicate");
        config.put("predicates.TopicPredicate.type", "org.apache.kafka.connect.transforms.predicates.TopicNameMatches");
        config.put("predicates.TopicPredicate.pattern", topicNamePattern);

        return this;
    }

    public ConnectorConfigBuilder addJdbcUnwrapSMT() {
        addUnwrapSMT();
        config.put("transforms.unwrap.type", "io.debezium.transforms.ExtractNewRecordState");
        config.put("transforms.unwrap.drop.tombstones", "false");
        config.put("transforms.unwrap.delete.handling.mode", "rewrite");
        config.put("transforms.unwrap.add.fields", "table,lsn");
        return this;
    }

    public ConnectorConfigBuilder addMongoUnwrapSMT() {
        addUnwrapSMT();
        config.put("transforms.unwrap.type", "io.debezium.connector.mongodb.transforms.ExtractNewDocumentState");
        config.put("transforms.unwrap.drop.tombstones", "false");
        config.put("transforms.unwrap.delete.handling.mode", "drop");
        config.put("transforms.unwrap.add.fields", "collection");
        return this;
    }

    private ConnectorConfigBuilder addUnwrapSMT() {
        String current = config.get("transforms").toString();
        if (current.isEmpty()) {
            config.put("transforms", "unwrap");
        }
        else {
            config.put("transforms", current + ",unwrap");
        }

        return this;
    }

    public ConnectorConfigBuilder addOperationRouter(String op, String targetTopicName, String sourceTopicPattern) {
        return addContentBasedRouter("value.op == '" + op + "' ? '" + targetTopicName + "' : null", sourceTopicPattern);
    }

    public ConnectorConfigBuilder addOperationRouterForTable(String op, String tableName) {
        String serverName = getDbServerName();
        String targetTopicName = serverName + "." + op + "." + tableName;
        return addOperationRouter(op, targetTopicName, serverName + ".*\\." + tableName);
    }

    public ConnectorConfigBuilder addMongoDbzUser() {
        if (ConfigProperties.DATABASE_MONGO_USE_TLS) {
            this
                    .put("mongodb.ssl.enabled", true)
                    .put("mongodb.ssl.keystore",
                            "/opt/kafka/external-configuration/" + OcpMongoCertGenerator.KEYSTORE_CONFIGMAP + "/" + OcpMongoCertGenerator.KEYSTORE_SUBPATH)
                    .put("mongodb.ssl.keystore.password", CertUtil.KEYSTORE_PASSWORD)
                    .put("mongodb.ssl.truststore",
                            "/opt/kafka/external-configuration/" + OcpMongoCertGenerator.TRUSTSTORE_CONFIGMAP + "/" + OcpMongoCertGenerator.TRUSTSTORE_SUBPATH)
                    .put("mongodb.ssl.truststore.password", CertUtil.KEYSTORE_PASSWORD);
        }
        else {
            this
                    .put("mongodb.user", ConfigProperties.DATABASE_MONGO_DBZ_USERNAME)
                    .put("mongodb.password", ConfigProperties.DATABASE_MONGO_DBZ_PASSWORD);
        }
        return this;
    }

    /**
     * Get configuration as JSON string
     * @return JSON string of connector config
     */
    public String getJsonString() {
        try {
            return mapper.writeValueAsString(config);
        }
        catch (JsonProcessingException e) {
            throw new IllegalStateException("Unable to convert connector config to JSON String");
        }
    }

    /**
     * Get configuration as OpenShift CR of type {@link KafkaConnector}
     * @return Connector CR
     */
    public KafkaConnector getCustomResource() {
        Map<String, Object> crConfig = new HashMap<>(config);

        KafkaConnectorBuilder connectorBuilder = new KafkaConnectorBuilder();
        return connectorBuilder
                .withNewMetadata()
                .withLabels(new HashMap<>())
                .withName(connectorName)
                .endMetadata()
                .withNewSpec()
                .withClassName((String) crConfig.remove("connector.class"))
                .withTasksMax((Integer) crConfig.remove("task.max"))
                .withConfig(crConfig)
                .endSpec()
                .build();
    }
}
