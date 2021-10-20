/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.kcrestextension;

import java.lang.Runtime.Version;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.ServletContext;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.health.ConnectClusterState;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.health.ConnectClusterStateImpl;
import org.apache.kafka.connect.runtime.isolation.PluginDesc;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.predicates.HasHeaderKey;
import org.apache.kafka.connect.transforms.predicates.RecordIsTombstone;
import org.apache.kafka.connect.transforms.predicates.TopicNameMatches;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.kcrestextension.entities.TransformsInfo;

/**
 * A JAX-RS Resource class defining endpoints that enable some advanced features
 * over Kafka Connect's REST interface:
 *   + report available transformations and their configuration
 *   + return if topic auto-creation is available and enabled
 *
 */
@Path("/debezium")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class DebeziumResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumResource.class);

    // TODO: This should not be so long. However, due to potentially long rebalances that may have to wait a full
    // session timeout to complete, during which we cannot serve some requests. Ideally we could reduce this, but
    // we need to consider all possible scenarios this could fail. It might be ok to fail with a timeout in rare cases,
    // but currently a worker simply leaving the group can take this long as well.
    public static final Duration REQUEST_TIMEOUT_MS = Duration.ofSeconds(90);
    // Mutable for integration testing; otherwise, some tests would take at least REQUEST_TIMEOUT_MS
    // to run
    private static Duration requestTimeoutMs = REQUEST_TIMEOUT_MS;

    private final List<TransformsInfo> transforms;
    private final Boolean isTopicCreationEnabled;
    private final Herder herder;
    private final Map<String, ?> config;

    private static final Pattern VERSION_PATTERN = Pattern
            .compile("([1-9][0-9]*(?:(?:\\.0)*\\.[1-9][0-9]*)*)(?:-([a-zA-Z0-9]+))?(?:(\\+)(0|[1-9][0-9]*)?)?(?:-([-a-zA-Z0-9.]+))?");
    private static final Version TOPIC_CREATION_KAFKA_VERSION = parseVersion("2.6.0");

    @javax.ws.rs.core.Context
    private ServletContext context;

    public DebeziumResource(ConnectClusterState clusterState, Map<String, ?> config) {
        Field herderField;
        try {
            herderField = ConnectClusterStateImpl.class.getDeclaredField("herder");
        }
        catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
        herderField.setAccessible(true);
        try {
            this.herder = (Herder) herderField.get(clusterState);
        }
        catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        this.transforms = new ArrayList<>();
        this.config = config;
        this.isTopicCreationEnabled = isTopicCreationEnabled();
    }

    // For testing purposes only
    public static void setRequestTimeout(long requestTimeoutMs) {
        DebeziumResource.requestTimeoutMs = Duration.ofMillis(requestTimeoutMs);
    }

    public static Version parseVersion(String version) {
        Matcher m = VERSION_PATTERN.matcher(version);
        if (m.matches()) {
            return Version.parse(version);
        }
        else if (m.lookingAt()) {
            return Version.parse(m.group());
        }
        throw new IllegalArgumentException("Invalid version string: \"" + version + "\"");
    }

    public static void resetRequestTimeout() {
        DebeziumResource.requestTimeoutMs = REQUEST_TIMEOUT_MS;
    }

    @GET
    @Path("/transforms")
    public List<TransformsInfo> listTransforms() {
        return this.getTransforms();
    }

    private synchronized List<TransformsInfo> getTransforms() {
        if (this.transforms.isEmpty()) {
            for (PluginDesc<Transformation<?>> plugin : herder.plugins().transformations()) {
                if ("org.apache.kafka.connect.runtime.PredicatedTransformation".equals(plugin.className())) {
                    this.transforms.add(new TransformsInfo(HasHeaderKey.class.getName(), (new HasHeaderKey<>().config())));
                    this.transforms.add(new TransformsInfo(RecordIsTombstone.class.getName(), (new RecordIsTombstone<>().config())));
                    this.transforms.add(new TransformsInfo(TopicNameMatches.class.getName(), (new TopicNameMatches<>().config())));
                }
                else {
                    this.transforms.add(new TransformsInfo(plugin));
                }
            }
        }

        return Collections.unmodifiableList(this.transforms);
    }

    @GET
    @Path("/topic-creation")
    public boolean getTopicCreationEnabled() {
        return this.isTopicCreationEnabled;
    }

    private synchronized Boolean isTopicCreationEnabled() {
        Version kafkaConnectVersion = parseVersion(AppInfoParser.getVersion());
        String topicCreationProperty = (String) config.get("topic.creation.enable");
        if (null == topicCreationProperty) { // when config is not set, default to true
            topicCreationProperty = "true";
        }
        return TOPIC_CREATION_KAFKA_VERSION.compareTo(kafkaConnectVersion) <= 0
                && Boolean.parseBoolean(topicCreationProperty);
    }

}
