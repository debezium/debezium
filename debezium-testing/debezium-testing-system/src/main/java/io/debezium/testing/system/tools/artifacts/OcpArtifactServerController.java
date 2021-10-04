/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.artifacts;

import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.openshift.client.OpenShiftClient;

import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;

public class OcpArtifactServerController {

    private final Deployment deployment;
    private final Service service;
    private final OpenShiftClient ocp;
    private final OkHttpClient http;

    public OcpArtifactServerController(Deployment deployment, Service service, OpenShiftClient ocp, OkHttpClient http) {
        this.deployment = deployment;
        this.service = service;
        this.ocp = ocp;
        this.http = http;
    }

    public HttpUrl getBaseUrl() {
        return new HttpUrl.Builder()
                .scheme("http")
                .host(service.getMetadata().getName() + "." + service.getMetadata().getNamespace() + ".svc.cluster.local")
                .port(8080)
                .build();
    }
}
