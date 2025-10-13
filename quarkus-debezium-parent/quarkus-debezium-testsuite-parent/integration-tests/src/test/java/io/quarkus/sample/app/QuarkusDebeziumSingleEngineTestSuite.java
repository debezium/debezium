/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.sample.app;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;
import org.junit.platform.suite.api.SuiteDisplayName;

import io.quarkus.sample.app.events.single.HeartbeatEventSingleEngineIT;
import io.quarkus.sample.app.events.single.LifecycleEventSingleEngineIT;
import io.quarkus.sample.app.general.single.CapturingSingleEngineIT;
import io.quarkus.sample.app.general.single.NotificationSingleEngineIT;
import io.quarkus.sample.app.general.single.SingleEngineIT;

@Suite
@SuiteDisplayName("Debezium Extensions for Quarkus - Integration Test Suite for Single Engine")
@SelectClasses({ CapturingSingleEngineIT.class, SingleEngineIT.class, NotificationSingleEngineIT.class, HeartbeatEventSingleEngineIT.class,
        LifecycleEventSingleEngineIT.class })
public interface QuarkusDebeziumSingleEngineTestSuite {
}
