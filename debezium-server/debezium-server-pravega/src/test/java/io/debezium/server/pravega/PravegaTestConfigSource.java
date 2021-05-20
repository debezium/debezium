/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.pravega;

import io.debezium.server.TestConfigSource;

public class PravegaTestConfigSource extends TestConfigSource {

    public PravegaTestConfigSource() {
        super();
        config.put("debezium.sink.type", "pravega");
        config.put("debezium.sink.pravega.scope", PravegaIT.STREAM_NAME);
    }

}
