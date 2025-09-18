package io.quarkus.debezium.configuration;

import io.debezium.runtime.configuration.QuarkusDatasourceConfiguration;

import java.util.Map;

public class MongoDbDatasourceConfiguration implements QuarkusDatasourceConfiguration {

    @Override
    public Map<String, String> asDebezium() {
        return Map.of();
    }

    @Override
    public boolean isDefault() {
        return false;
    }
}
