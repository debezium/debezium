package io.debezium.server.pravega;

import io.debezium.server.TestConfigSource;

public class PravegaTxnTestConfigSource extends TestConfigSource {

    public PravegaTxnTestConfigSource() {
        super();
        config.put("debezium.sink.type", "pravega-txn");
        config.put("debezium.sink.pravega.scope", PravegaTxnIT.STREAM_NAME);
    }

}
