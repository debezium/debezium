/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.deployment.engine;

import io.quarkus.arc.processor.BeanInfo;

public record InvokerMetaData(String generatedClassName, BeanInfo delegate) { }
