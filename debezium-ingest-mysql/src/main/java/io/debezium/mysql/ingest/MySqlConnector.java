/*
 * Copyright 2015 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.mysql.ingest;

import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

/**
 * A Kafka Connect source connector that creates tasks that read the MySQL binary log and generate the corresponding
 * data change events.
 * 
 * @author Randall Hauch
 */
public class MySqlConnector extends SourceConnector {

    public MySqlConnector() {
    }

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public void start(Map<String, String> props) {
    }

    @Override
    public Class<? extends Task> taskClass() {
        return null;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        return null;
    }

    @Override
    public void stop() {
    }

}
