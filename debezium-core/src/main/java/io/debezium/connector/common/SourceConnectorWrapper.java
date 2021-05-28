package io.debezium.connector.common;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;

public interface SourceConnectorWrapper<Config> {

    Config validate(Map<String, String> connectorConfigs);

    String version();

    void start(Map<String, String> props);

    Class<? extends TaskWrapper> taskClass();

    List<Map<String, String>> taskConfigs(int maxTasks);

    void stop();

    ConfigDef config();

}
