package io.debezium.server.pravega;

import io.debezium.server.TestConfigSource;

public class PravegaTestConfigSource extends TestConfigSource {

    public PravegaTestConfigSource() {
        super();
        config.put("debezium.sink.type", "pravega");
        config.put("debezium.sink.pravega.scope", PravegaIT.STREAM_NAME);
    }

}
