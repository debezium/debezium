/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.util.HashMap;
import java.util.Map;

import com.codahale.metrics.health.HealthCheck;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class CassandraConnectorTaskHealthCheck extends HealthCheck {
    private static final ObjectMapper mapper = new ObjectMapper();
    private final CassandraConnectorTask.ProcessorGroup processorGroup;
    private final CassandraClient cassandraClient;

    public CassandraConnectorTaskHealthCheck(CassandraConnectorTask.ProcessorGroup processorGroup, CassandraClient cassandraClient) {
        this.processorGroup = processorGroup;
        this.cassandraClient = cassandraClient;
    }

    @Override
    protected Result check() throws Exception {
        if (isHealthy()) {
            return Result.healthy(getMessage());
        }
        else {
            return Result.unhealthy(getMessage());
        }
    }

    private boolean isHealthy() {
        if (!processorGroup.isRunning()) {
            return false;
        }
        return cassandraClient.isQueryable();
    }

    private String getMessage() throws JsonProcessingException {
        Map<String, Boolean> health = new HashMap<>();
        health.put(processorGroup.getClass().getName(), processorGroup.isRunning());
        health.put(cassandraClient.getClass().getName(), cassandraClient.isQueryable());
        return mapper.writeValueAsString(health);
    }
}
