/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.openshift;

/**
 *
 * @author Jakub Cechacek
 */
public class TestUtils {

    /**
     * Generates unique identifier
     * @return unique id
     */
    public String getUniqueId() {
        return String.valueOf(System.currentTimeMillis());
    }
}
