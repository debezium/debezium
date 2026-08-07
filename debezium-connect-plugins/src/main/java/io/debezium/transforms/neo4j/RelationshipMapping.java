/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.neo4j;

import java.util.List;

public record RelationshipMapping(String fkColumn, String type, Direction direction, String targetLabel, String targetId, CudEvent.Operation targetNodeOp,
        List<String> properties) {

    public enum Direction {
        OUTGOING,
        INCOMING
    }
}
