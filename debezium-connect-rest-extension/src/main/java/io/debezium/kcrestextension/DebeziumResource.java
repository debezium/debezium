/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.kcrestextension;

import java.lang.Runtime.Version;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.apache.kafka.connect.runtime.isolation.PluginDesc;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.predicates.Predicate;

import io.debezium.DebeziumException;
import io.debezium.kcrestextension.entities.PredicateDefinition;
import io.debezium.kcrestextension.entities.TransformDefinition;
import io.debezium.metadata.ConnectorDescriptor;

/**
 * A JAX-RS Resource class defining endpoints that enable some advanced features
 * over Kafka Connect's REST interface:
 *   + report available transformations and their configuration
 *   + return if topic auto-creation is available and enabled
 *
 */
@Path(DebeziumResource.BASE_PATH)
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class DebeziumResource {

    public static final String BASE_PATH = "/debezium";
    public static final String CONNECTOR_PLUGINS_ENDPOINT = "/connector-plugins";
    public static final String TRANSFORMS_ENDPOINT = "/transforms";
    public static final String PREDICATES_ENDPOINT = "/predicates";
    public static final String TOPIC_CREATION_ENDPOINT = "/topic-creation-enabled";

    public static final Set<String> SUPPORTED_CONNECTORS = new HashSet<>(Arrays.asList(
            "io.debezium.connector.mongodb.MongoDbConnector",
            "io.debezium.connector.mysql.MySqlConnector",
            "io.debezium.connector.oracle.OracleConnector",
            "io.debezium.connector.postgresql.PostgresConnector",
            "io.debezium.connector.sqlserver.SqlServerConnector"));

    private final ConnectClusterState connectClusterState;
    private Herder herder = null;
    private final Boolean isTopicCreationEnabled;
    private List<TransformDefinition> transforms = null;
    private List<PredicateDefinition> predicates = null;
    private List<ConnectorDescriptor> availableConnectorPlugins = null;

    private static final Pattern VERSION_PATTERN = Pattern
            .compile("([1-9][0-9]*(?:(?:\\.0)*\\.[1-9][0-9]*)*)(?:-([a-zA-Z0-9]+))?(?:(\\+)(0|[1-9][0-9]*)?)?(?:-([-a-zA-Z0-9.]+))?");
    private static final Version TOPIC_CREATION_KAFKA_VERSION = parseVersion("2.6.0");

    @javax.ws.rs.core.Context
    private ServletContext context;

    public DebeziumResource(ConnectClusterState connectClusterState, Map<String, ?> config) {
        this.connectClusterState = connectClusterState;
        this.isTopicCreationEnabled = isTopicCreationEnabled(config);
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

    private static <T> void addConnectorPlugins(Map<String, ConnectorDescriptor> connectorPlugins, Collection<PluginDesc<T>> plugins) {
        plugins.stream()
                .filter(p -> SUPPORTED_CONNECTORS.contains(p.pluginClass().getName()))
                .forEach(p -> connectorPlugins.put(p.pluginClass().getName() + "#" + p.version(), new ConnectorDescriptor(p.pluginClass().getName(), p.version())));
    }

    private synchronized void initConnectorPlugins() {
        if (null == this.availableConnectorPlugins || this.availableConnectorPlugins.isEmpty()) {
            // TODO: improve once plugins are allowed to be added/removed during runtime by Kafka Connect, @see org.apache.kafka.connect.runtime.rest.resources.ConnectorPluginsResource
            final Map<String, ConnectorDescriptor> connectorPlugins = new HashMap<>();
            Herder herder = getHerder();
            addConnectorPlugins(connectorPlugins, herder.plugins().sinkConnectors());
            addConnectorPlugins(connectorPlugins, herder.plugins().sourceConnectors());
            this.availableConnectorPlugins = Collections.unmodifiableList(new ArrayList<>(connectorPlugins.values()));
        }
    }

    private synchronized void initTransformsAndPredicates() {
        if (null == this.transforms || this.transforms.isEmpty()) {
            final List<TransformDefinition> transformPlugins = new ArrayList<>();
            final List<PredicateDefinition> predicatePlugins = new ArrayList<>();
            Herder herder = getHerder();
            for (PluginDesc<Transformation<?>> transformPlugin : herder.plugins().transformations()) {
                TransformDefinition transformDefinition = TransformDefinition.fromPluginDesc(transformPlugin);
                if (null != transformDefinition) {
                    transformPlugins.add(transformDefinition);
                }
            }
            for (PluginDesc<Predicate<?>> predicate : herder.plugins().predicates()) {
                PredicateDefinition predicateDefinition = PredicateDefinition.fromPluginDesc(predicate);
                if (null != predicateDefinition) {
                    predicatePlugins.add(predicateDefinition);
                }
            }
            this.predicates = Collections.unmodifiableList(predicatePlugins);
            this.transforms = Collections.unmodifiableList(transformPlugins);
        }
    }

    private synchronized Boolean isTopicCreationEnabled(Map<String, ?> config) {
        Version kafkaConnectVersion = parseVersion(AppInfoParser.getVersion());
        String topicCreationProperty = (String) config.get("topic.creation.enable");
        if (null == topicCreationProperty) { // when config is not set, default to true
            topicCreationProperty = "true";
        }
        return TOPIC_CREATION_KAFKA_VERSION.compareTo(kafkaConnectVersion) <= 0
                && Boolean.parseBoolean(topicCreationProperty);
    }

    private synchronized Herder getHerder() {
        if (null == this.herder) {
            Field herderField;
            try {
                herderField = this.connectClusterState.getClass().getDeclaredField("herder");
            }
            catch (NoSuchFieldException e) {
                throw new DebeziumException(e);
            }
            herderField.setAccessible(true);
            try {
                this.herder = (Herder) herderField.get(this.connectClusterState);
            }
            catch (IllegalAccessException e) {
                throw new DebeziumException(e);
            }
        }
        return this.herder;
    }

    @GET
    @Path(CONNECTOR_PLUGINS_ENDPOINT)
    @Produces(MediaType.APPLICATION_JSON)
    public List<ConnectorDescriptor> availableDebeziumConnectors() {
        initConnectorPlugins();
        return this.availableConnectorPlugins;
    }

    @GET
    @Path(TRANSFORMS_ENDPOINT)
    @Produces(MediaType.APPLICATION_JSON)
    public List<TransformDefinition> listTransforms() {
        initTransformsAndPredicates();
        return this.transforms;
    }

    @GET
    @Path(PREDICATES_ENDPOINT)
    @Produces(MediaType.APPLICATION_JSON)
    public List<PredicateDefinition> listPredicates() {
        initTransformsAndPredicates();
        return this.predicates;
    }

    @GET
    @Path(TOPIC_CREATION_ENDPOINT)
    @Produces(MediaType.APPLICATION_JSON)
    public boolean getTopicCreationEnabled() {
        return this.isTopicCreationEnabled;
    }

    @GET
    @Path("/version")
    @Produces(MediaType.APPLICATION_JSON)
    public String getDebeziumVersion() {
        return Module.version();
    }

}
