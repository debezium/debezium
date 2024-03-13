/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools;

import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.apicurio.registry.operator.api.v1.model.ApicurioRegistryStatus;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentCondition;
import io.fabric8.kubernetes.api.model.apps.DeploymentStatus;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.openshift.api.model.DeploymentConfig;
import io.fabric8.openshift.api.model.DeploymentConfigStatus;
import io.strimzi.api.kafka.model.kafka.Status;

/**
 *
 * @author Jakub Cechacek
 */
public class WaitConditions {

    private static final Logger LOGGER = LoggerFactory.getLogger(WaitConditions.class);

    /**
     * Wait condition for readiness of Strimzi resources
     * @param resource resource instance
     * @param <T> resource type
     * @return true if resource is ready
     */
    public static <T extends Status> boolean kafkaReadyCondition(CustomResource<?, T> resource) {
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

    /**
     * Wait condition for deployment configs
     * @param resource dc resource
     * @return true when dc becomes available
     */
    public static boolean deploymentAvailableCondition(DeploymentConfig resource) {
        DeploymentConfigStatus status = resource.getStatus();
        if (status == null) {
            return false;
        }
        Stream<io.fabric8.openshift.api.model.DeploymentCondition> conditions = status.getConditions().stream();
        return conditions.anyMatch(c -> c.getType().equalsIgnoreCase("Available") && c.getStatus().equalsIgnoreCase("True"));
    }

    /**
     * Wait condition for resource deletion
     * @param resource dc resource
     * @return true when resource is deleted
     */
    public static <T extends Status> boolean resourceDeleted(CustomResource<?, T> resource) {
        T status = resource.getStatus();
        return status == null;
    }

    /**
     * Wait condition for apicurio resource deletion
     * @param resource dc resource
     * @return true when resource is deleted
     */
    public static <T extends ApicurioRegistryStatus> boolean apicurioResourceDeleted(CustomResource<?, T> resource) {
        T status = resource.getStatus();
        return status == null;
    }

    public static long scaled(long amount) {
        long scaled = ConfigProperties.WAIT_SCALE_FACTOR * amount;
        LOGGER.debug("Waiting amount: " + scaled);
        return scaled;
    }
}
