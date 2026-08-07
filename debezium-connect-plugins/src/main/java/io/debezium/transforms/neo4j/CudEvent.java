/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.neo4j;

public interface CudEvent {

    String type();

    Operation op();

    enum Operation {
        MERGE("merge"),
        MATCH("match"),
        DELETE("delete");

        private final String value;

        Operation(String value) {
            this.value = value;
        }

        public String value() {
            return value;
        }
    }
}
