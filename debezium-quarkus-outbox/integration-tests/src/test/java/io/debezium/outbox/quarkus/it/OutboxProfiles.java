/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.quarkus.it;

import java.util.Collections;
import java.util.Map;

import io.quarkus.test.junit.QuarkusTestProfile;

/**
 * Specifies a variety of test profiles to be used for the Debezium Outbox extension test suite.
 *
 * @author Chris Cranford
 */
public class OutboxProfiles {
    /**
     * Uses the values directly from {@code application.properties} without changes.
     */
    public static class Default implements QuarkusTestProfile {
    }

    /**
     * Disables OpenTracing support that is enabled by default.
     */
    public static class OpenTracingDisabled implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Collections.singletonMap("quarkus.debezium-outbox.tracing.enabled", "false");
        }
    }
}
