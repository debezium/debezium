/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.artifacts;

import static io.debezium.testing.system.tools.WaitConditions.scaled;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.system.tools.OpenShiftUtils;
import io.debezium.testing.system.tools.WaitConditions;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.openshift.client.OpenShiftClient;
import io.strimzi.api.kafka.model.connect.build.Artifact;
import io.strimzi.api.kafka.model.connect.build.JarArtifactBuilder;
import io.strimzi.api.kafka.model.connect.build.Plugin;
import io.strimzi.api.kafka.model.connect.build.PluginBuilder;
import io.strimzi.api.kafka.model.connect.build.ZipArtifactBuilder;

import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;

public class OcpArtifactServerController {

    public static final Logger LOGGER = LoggerFactory.getLogger(OcpArtifactServerController.class);

    private final Deployment deployment;
    private final String project;
    private final Service service;
    private final OpenShiftClient ocp;
    private final OkHttpClient http;

    private Map<String, HttpUrl> artifacts;
    private final OpenShiftUtils ocpUtils;

    public OcpArtifactServerController(Deployment deployment, Service service, OpenShiftClient ocp, OkHttpClient http) throws IOException {
        this.deployment = deployment;
        this.project = deployment.getMetadata().getNamespace();
        this.service = service;
        this.ocp = ocp;
        this.ocpUtils = new OpenShiftUtils(ocp);
        this.http = http;
    }

    public HttpUrl getBaseUrl() {
        return new HttpUrl.Builder().scheme("http")
                .host(service.getMetadata().getName() + "." + service.getMetadata().getNamespace() + ".svc.cluster.local")
                .port(8080)
                .build();
    }

    private HttpUrl createArtifactUrl(String link) {
        return getBaseUrl().resolve(link);
    }

    public Optional<HttpUrl> geArtifactUrl(String name) {
        return Optional.ofNullable(artifacts.get(name));
    }

    public Optional<String> getArtifactUrlAsString(String name) {
        return geArtifactUrl(name).map(HttpUrl::toString);
    }

    private Artifact createArtifact(String url) {
        Objects.requireNonNull(url);
        String type = url.substring(url.lastIndexOf('.') + 1);

        switch (type) {
            case "zip":
                return new ZipArtifactBuilder().withUrl(url).build();
            case "jar":
                return new JarArtifactBuilder().withUrl(url).build();
            default:
                throw new IllegalStateException("Unsupported artifact type: " + type);
        }
    }

    public Plugin createPlugin(String name, List<String> artifacts) {
        List<Artifact> pluginArtifacts = artifacts.stream()
                .map(this::getArtifactUrlAsString)
                .map(a -> a.orElseThrow(() -> new IllegalStateException("Missing artifact for plugin'" + name + "'")))
                .map(this::createArtifact)
                .collect(toList());

        return new PluginBuilder().withName(name).withArtifacts(pluginArtifacts).build();
    }

    public Plugin createDebeziumPlugin(String database) {
        return createDebeziumPlugin(database, List.of());
    }

    public Plugin createDebeziumPlugin(String database, List<String> extraArtifacts) {
        List<String> commonArtifacts = List.of(
                "debezium-connector-" + database,
                "debezium-scripting",
                "connect-converter",
                "groovy/groovy",
                "groovy/groovy-json",
                "groovy/groovy-jsr223",
                "jackson/jackson-dataformat-csv",
                "jackson/jackson-datatype-jsr310",
                "jackson/jackson-jaxrs-base",
                "jackson/jackson-jaxrs-json-provider",
                "jackson/jackson-module-jaxb-annotations",
                "jackson/jackson-module-scala_2.13");
        List<String> artifacts = Stream.concat(commonArtifacts.stream(), extraArtifacts.stream()).collect(toList());
        return createPlugin("debezium-connector-" + database, artifacts);
    }

    public List<String> readArtifactListing() throws IOException {
        return await()
                .pollInterval(5, TimeUnit.SECONDS)
                .atMost(scaled(1), TimeUnit.MINUTES)
                .ignoreExceptions()
                .until(this::tryReadingArtifactListing, result -> !result.isEmpty());
    }

    private List<String> tryReadingArtifactListing() throws IOException {
        LOGGER.info("Trying to read listing from artifact server");
        Pod pod = ocpUtils.podsForDeployment(deployment).get(0);

        try (InputStream is = ocp.pods()
                .inNamespace(project)
                .withName(pod.getMetadata().getName())
                .inContainer("debezium-artifact-server")
                .file("/opt/plugins/artifacts.txt")
                .read()) {
            String listing = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            return listing.lines().collect(toList());
        }
    }

    public Map<String, HttpUrl> listArtifacts() throws IOException {
        List<String> listing = readArtifactListing();

        return listing.stream().map(l -> l.split("::", 2)).collect(toMap(e -> e[0], e -> createArtifactUrl(e[1])));
    }

    public void waitForServer() throws IOException {
        LOGGER.info("Waiting for Artifact Server");
        ocp.pods()
                .inNamespace(project)
                .withLabel("app", "debezium-artifact-server")
                .waitUntilReady(scaled(5), TimeUnit.MINUTES);

        ocp.apps()
                .deployments()
                .inNamespace(project)
                .withName(deployment.getMetadata().getName())
                .waitUntilCondition(WaitConditions::deploymentAvailableCondition, scaled(5), TimeUnit.MINUTES);
        this.artifacts = listArtifacts();
    }
}
