/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.deployment.engine;

import java.util.UUID;

import io.quarkus.arc.processor.BeanInfo;

public record InvokerMetaData(UUID id, String invokerClassName, BeanInfo mediator) {

    public String getShortIdentifier() {
        return id.toString().split("-")[0].replaceAll("\\D", "");
    }
}
