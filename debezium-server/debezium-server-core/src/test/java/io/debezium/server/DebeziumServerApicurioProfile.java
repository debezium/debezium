/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server;

import java.util.Collections;
import java.util.List;

import io.debezium.testing.testcontainers.ApicurioTestResourceLifeCycleManager;
import io.quarkus.test.junit.QuarkusTestProfile;

public class DebeziumServerApicurioProfile implements QuarkusTestProfile {

    @Override
    public List<TestResourceEntry> testResources() {
        return Collections.singletonList(new TestResourceEntry(ApicurioTestResourceLifeCycleManager.class));
    }
}
