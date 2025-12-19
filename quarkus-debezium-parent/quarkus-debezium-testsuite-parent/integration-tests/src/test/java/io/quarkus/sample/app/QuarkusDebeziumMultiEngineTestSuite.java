/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.sample.app;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;
import org.junit.platform.suite.api.SuiteDisplayName;

import io.quarkus.sample.app.events.multi.HeartbeatEventMultiEngineIT;
import io.quarkus.sample.app.events.multi.LifecycleEventMultiEngineIT;
import io.quarkus.sample.app.general.multi.CapturingMultiEngineIT;
import io.quarkus.sample.app.general.multi.MultiEngineIT;
import io.quarkus.sample.app.general.multi.NotificationMultiEngineIT;

@Suite
@SuiteDisplayName("Debezium Extensions for Quarkus - Integration Test Suite for Multi Engine")
@SelectClasses({ CapturingMultiEngineIT.class, MultiEngineIT.class, NotificationMultiEngineIT.class, HeartbeatEventMultiEngineIT.class,
        LifecycleEventMultiEngineIT.class })
public interface QuarkusDebeziumMultiEngineTestSuite {
}
