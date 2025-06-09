/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage;

import io.openlineage.client.transports.Transport;
import io.openlineage.client.transports.TransportBuilder;
import io.openlineage.client.transports.TransportConfig;

public class DebeziumTestTransportBuilder implements TransportBuilder {

    public static final DebeziumTestTransport DEBEZIUM_TEST_TRANSPORT = new DebeziumTestTransport();

    @Override
    public String getType() {
        return "debezium";
    }

    @Override
    public TransportConfig getConfig() {
        return new DebeziumTransportConfig();
    }

    @Override
    public Transport build(TransportConfig config) {
        return DEBEZIUM_TEST_TRANSPORT;
    }

}
