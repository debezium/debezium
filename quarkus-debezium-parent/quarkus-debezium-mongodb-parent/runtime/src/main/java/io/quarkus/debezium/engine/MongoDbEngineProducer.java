package io.quarkus.debezium.engine;

import io.debezium.runtime.ConnectorProducer;
import io.debezium.runtime.Debezium;
import io.debezium.runtime.configuration.DebeziumEngineConfiguration;

public class MongoDbEngineProducer implements ConnectorProducer {
    @Override
    public Debezium engine(DebeziumEngineConfiguration debeziumEngineConfiguration) {
        return null;
    }
}
