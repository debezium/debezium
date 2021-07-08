/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools;

import io.fabric8.openshift.client.OpenShiftClient;

import okhttp3.OkHttpClient;

/**
 * Base class for Deployers with OCP as  target runtime
 * @param <T>
 */
public abstract class AbstractOcpDeployer<T> implements Deployer<T> {
    protected final OpenShiftClient ocp;
    protected final OkHttpClient http;
    protected final String project;

    public AbstractOcpDeployer(String project, OpenShiftClient ocp, OkHttpClient http) {
        this.project = project;
        this.ocp = ocp;
        this.http = http;
    }
}
