package io.debezium.server.pravega;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine.ChangeConsumer;

public interface PravegaSink extends ChangeConsumer<ChangeEvent<Object, Object>>, AutoCloseable {

}
