/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.sample.app;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;
import org.junit.platform.suite.api.SuiteDisplayName;

import io.quarkus.sample.app.events.HeartbeatEventIT;
import io.quarkus.sample.app.events.LifecycleEventIT;
import io.quarkus.sample.app.general.CapturingIT;
import io.quarkus.sample.app.general.EngineIT;
import io.quarkus.sample.app.general.NotificationIT;

@Suite
@SuiteDisplayName("Debezium Extensions for Quarkus - Integration Test Suite")
@SelectClasses({ CapturingIT.class, EngineIT.class, NotificationIT.class, HeartbeatEventIT.class, LifecycleEventIT.class })
public interface QuarkusDebeziumIntegrationTestSuite {
}
