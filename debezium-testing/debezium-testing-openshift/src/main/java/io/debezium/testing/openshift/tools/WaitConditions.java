/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.openshift.tools;

import java.util.stream.Stream;

import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentCondition;
import io.fabric8.kubernetes.api.model.apps.DeploymentStatus;
import io.strimzi.api.kafka.model.status.HasStatus;
import io.strimzi.api.kafka.model.status.Status;

/**
 *
 * @author Jakub Cechacek
 */
public class WaitConditions {

    /**
     * Wait condition for readiness of Strimzi resources
     * @param resource resource instance
     * @param <T> resource type
     * @return true if resource is ready
     */
    public static <T extends Status> boolean kafkaReadyCondition(HasStatus<T> resource) {
        T status = resource.getStatus();
        if (status == null) {
            return false;
        }
        return status.getConditions().stream().anyMatch(c -> c.getType().equalsIgnoreCase("Ready") && c.getStatus().equalsIgnoreCase("True"));
    }

    /**
     * Wait condition for deployments
     * @param resource deployment resource
     * @return true when deployment becomes available
     */
    public static boolean deploymentAvailableCondition(Deployment resource) {
        DeploymentStatus status = resource.getStatus();
        if (status == null) {
            return false;
        }
        Stream<DeploymentCondition> conditions = status.getConditions().stream();
        return conditions.anyMatch(c -> c.getType().equalsIgnoreCase("Available") && c.getStatus().equalsIgnoreCase("True"));
    }
}
