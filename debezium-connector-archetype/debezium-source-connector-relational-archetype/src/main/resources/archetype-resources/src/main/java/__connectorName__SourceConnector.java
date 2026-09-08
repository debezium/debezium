/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package ${package};

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;

import io.debezium.config.Configuration;
import io.debezium.connector.common.RelationalBaseSourceConnector;

/**
 * The top-level Kafka Connect source connector for ${connectorName}.
 *
 * <p>Registers the connector with Kafka Connect and always runs exactly one
 * {@link ${connectorName}ConnectorTask}. Extends {@link RelationalBaseSourceConnector}, which
 * validates the configured fields and then the database connection.
 */
public class ${connectorName}SourceConnector extends RelationalBaseSourceConnector {

    private Map<String, String> properties;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public void start(Map<String, String> props) {
        this.properties = props;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return ${connectorName}ConnectorTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        return Collections.singletonList(properties);
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return ${connectorName}ConnectorConfig.configDef();
    }

    @Override
    protected Map<String, ConfigValue> validateAllFields(Configuration config) {
        return config.validate(${connectorName}ConnectorConfig.ALL_FIELDS);
    }

    @Override
    protected void validateConnection(Map<String, ConfigValue> configValues, Configuration config) {
        // TODO: open a ${connectorName}Connection with the supplied configuration and, if it
        // fails, attach the error to the relevant field (for example the database.hostname
        // ConfigValue) so it surfaces during connector validation.
    }
}
