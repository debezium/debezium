/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.common;

import static io.debezium.connector.common.DebeziumHeaders.DEBEZIUM_CONNECTOR_LOGICAL_NAME_HEADER;
import static io.debezium.connector.common.DebeziumHeaders.DEBEZIUM_CONNECTOR_NAME_HEADER;
import static io.debezium.connector.common.DebeziumHeaders.DEBEZIUM_TASK_ID_HEADER;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;

import io.debezium.service.Service;

public class DebeziumHeaderProducer implements Service {

    private final CdcSourceTaskContext sourceTaskContext;

    public DebeziumHeaderProducer(CdcSourceTaskContext sourceTaskContext) {
        this.sourceTaskContext = sourceTaskContext;
    }

    public ConnectHeaders contextHeaders() {

        var debeziumContextHeaders = new ConnectHeaders();
        debeziumContextHeaders.add(DEBEZIUM_CONNECTOR_LOGICAL_NAME_HEADER, sourceTaskContext.getConnectorLogicalName(), Schema.STRING_SCHEMA);
        debeziumContextHeaders.add(DEBEZIUM_TASK_ID_HEADER, sourceTaskContext.getTaskId(), Schema.STRING_SCHEMA);
        debeziumContextHeaders.add(DEBEZIUM_CONNECTOR_NAME_HEADER, sourceTaskContext.getConnectorName(), Schema.STRING_SCHEMA);

        return debeziumContextHeaders;
    }

}
