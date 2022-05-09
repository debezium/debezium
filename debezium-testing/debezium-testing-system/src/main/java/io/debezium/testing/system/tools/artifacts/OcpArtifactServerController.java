/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.artifacts;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import io.debezium.testing.system.tools.OpenShiftUtils;
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

    private final Deployment deployment;
    private final String project;
    private final Service service;
    private final OpenShiftClient ocp;
    private final OkHttpClient http;

    private final Map<String, HttpUrl> artifacts;
    private final OpenShiftUtils ocpUtils;

    public OcpArtifactServerController(Deployment deployment, Service service, OpenShiftClient ocp, OkHttpClient http) throws IOException {
        this.deployment = deployment;
        this.project = deployment.getMetadata().getNamespace();
        this.service = service;
        this.ocp = ocp;
        this.ocpUtils = new OpenShiftUtils(ocp);
        this.http = http;
        this.artifacts = listArtifacts();
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
        List<String> commonArtifacts = List.of("debezium-connector-" + database, "debezium-scripting", "connect-converter");
        List<String> artifacts = Stream.concat(commonArtifacts.stream(), extraArtifacts.stream()).collect(toList());

        return createPlugin("debezium-connector-" + database, artifacts);
    }

    public List<String> readArtifactListing() throws IOException {
        Pod pod = ocpUtils.podsForDeployment(deployment).get(0);

        try (InputStream is = ocp.pods()
                .inNamespace(project)
                .withName(pod.getMetadata().getName())
                .inContainer("debezium-artifact-server")
                .file("/opt/plugins/artifacts.txt")
                .read()) {

            return new BufferedReader(new InputStreamReader(is)).lines().collect(toList());
        }
    }

    public Map<String, HttpUrl> listArtifacts() throws IOException {
        List<String> listing = readArtifactListing();

        return listing.stream().map(l -> l.split("::", 2)).collect(toMap(e -> e[0], e -> createArtifactUrl(e[1])));
    }
}
