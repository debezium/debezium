/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system;

import org.apache.maven.artifact.versioning.ComparableVersion;

import io.debezium.testing.system.tools.ConfigProperties;

/**
 * Utility functions used in tests
 * @author Jakub Cechacek
 */
public final class TestUtils {

    private TestUtils() {
        // intentionally private
    }

    /**
     * Generates unique identifier
     * @return unique id
     */
    public static String getUniqueId() {
        return String.valueOf(System.currentTimeMillis());
    }

    /**
     * Checks if Kafka should be use with KRaft mode or not
     * @return kraft being used
     */
    public static boolean shouldKRaftBeUsed() {
        ComparableVersion kafkaVersion = new ComparableVersion(ConfigProperties.VERSION_KAFKA);
        return ConfigProperties.FORCE_KRAFT || kafkaVersion.compareTo(new ComparableVersion("4.0.0")) >= 0;
    }
}
