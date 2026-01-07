/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.operatorutil;

import static io.debezium.testing.system.tools.ConfigProperties.PRODUCT_BUILD;

import io.debezium.testing.system.tools.ConfigProperties;

/**
 * Constants for Apicurio and Strimzi operators
 */
public enum OpenshiftOperatorEnum {
    APICURIO(PRODUCT_BUILD ? "Service Registry" : "Apicurio",
            "apicurio-registry-operator",
            ConfigProperties.APICURIO_OPERATOR_CHANNEL,
            ConfigProperties.OCP_PROJECT_REGISTRY + "-opgroup",
            "apicurio-registry-operator.v" + ConfigProperties.APICURIO_OPERATOR_VERSION),
    STRIMZI(PRODUCT_BUILD ? "AMQ Streams" : "Strimzi",
            PRODUCT_BUILD ? "amq-streams" : "strimzi",
            ConfigProperties.STRIMZI_OPERATOR_CHANNEL,
            ConfigProperties.OCP_PROJECT_DBZ + "-opgroup",
            "strimzi-cluster-operator.v" + ConfigProperties.STRIMZI_OPERATOR_VERSION);

    /** Human name for usage in logging and prints */
    private final String name;
    /** Name for identifying operator deployment */
    private final String deploymentNamePrefix;
    /** Update channel to by used in subscription */
    private final String subscriptionUpdateChannel;
    /** Name of operator group to install operator in */
    private final String operatorGroupName;
    /** Starting CSV version to be used when installing the operator */
    private final String startingCSV;

    OpenshiftOperatorEnum(String name, String deploymentName, String channel, String operatorGroupName, String startingCSV) {
        this.name = name;
        this.deploymentNamePrefix = deploymentName;
        this.subscriptionUpdateChannel = channel;
        this.operatorGroupName = operatorGroupName;
        this.startingCSV = startingCSV;
    }

    public String getName() {
        return name;
    }

    public String getDeploymentNamePrefix() {
        return deploymentNamePrefix;
    }

    public String getOperatorGroupName() {
        return operatorGroupName;
    }

    public String getSubscriptionUpdateChannel() {
        return subscriptionUpdateChannel;
    }

    public String getStartingCSV() {
        return startingCSV;
    }
}
