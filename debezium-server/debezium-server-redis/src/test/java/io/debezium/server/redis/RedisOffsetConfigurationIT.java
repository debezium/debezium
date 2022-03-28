/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;

/**
 * Integration test that verifies reading and writing offsets from Redis key value store
 *
 * @author Oren Elias
 */
@QuarkusIntegrationTest
@TestProfile(RedisOffsetConfigurationTestProfile.class)
@QuarkusTestResource(RedisTestResourceLifecycleManager.class)

public class RedisOffsetConfigurationIT extends RedisOffsetIT {

}
