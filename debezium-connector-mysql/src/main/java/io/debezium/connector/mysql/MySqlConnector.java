/*
 * Copyright Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;

/**
 * A Kafka Connect source connector that creates tasks that read the MySQL binary log and generate the corresponding
 * data change events.
 * <h2>Configuration</h2>
 * <p>
 * This connector is configured with the set of properties described in {@link MySqlConnectorConfig}.
 * 
 * 
 * @author Randall Hauch
 */
public class MySqlConnector extends SourceConnector {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private Map<String, String> props;

    public MySqlConnector() {
    }

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public Class<? extends Task> taskClass() {
        return LogReader.class;
    }

    @Override
    public void start(Map<String, String> props) {
        this.props = props;
        Configuration.from(props).validate(MySqlConnectorConfig.ALL_FIELDS, logger::error);
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        return props == null ? Collections.emptyList() : Collections.singletonList(new HashMap<String, String>(props));
    }

    @Override
    public void stop() {
        this.props = null;
    }

}
