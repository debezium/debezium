/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.neo4j;

import java.util.List;
import java.util.Map;

public record CudNodeEvent(Operation op, List<String> labels, Map<String, Object> ids, Map<String, Object> properties, Boolean detach) implements CudEvent {

    @Override
    public String type() {
        return "node";
    }
}
