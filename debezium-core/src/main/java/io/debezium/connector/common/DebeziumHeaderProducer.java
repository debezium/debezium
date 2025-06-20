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

<<<<<<< HEAD
/**
 * A service implementation that produces Debezium-specific headers for CDC (Change Data Capture) events.
 * <p>
 * This class is responsible for creating standardized headers containing metadata about the
 * Debezium connector context, which can be attached to Kafka Connect records to provide
 * traceability and identification information.
 * </p>
 *
 * @author Mario Fiore Vitale
 */
=======
>>>>>>> 77778c0c6 (DBZ-9020 Add support for multi task connectors)
public class DebeziumHeaderProducer implements Service {

    private final CdcSourceTaskContext sourceTaskContext;

<<<<<<< HEAD
    /**
     * Constructs a new DebeziumHeaderProducer with the specified source task context.
     *
     * @param sourceTaskContext the CDC source task context containing connector metadata;
     *                         must not be null
     */
=======
>>>>>>> 77778c0c6 (DBZ-9020 Add support for multi task connectors)
    public DebeziumHeaderProducer(CdcSourceTaskContext sourceTaskContext) {
        this.sourceTaskContext = sourceTaskContext;
    }

<<<<<<< HEAD
    /**
     * Creates and returns a set of standardized Debezium context headers.
     * <p>
     * The returned headers include:
     * </p>
     * <ul>
     *   <li><strong>Connector Logical Name</strong> - The logical name of the Debezium connector</li>
     *   <li><strong>Task ID</strong> - The unique identifier of the connector task</li>
     *   <li><strong>Connector Name</strong> - The name of the Debezium connector</li>
     * </ul>
     * <p>
     * These headers provide essential metadata for tracking and identifying the source
     * of CDC events in downstream processing systems.
     * </p>
     *
     * @return a {@link ConnectHeaders} object containing the Debezium context headers;
     *         never returns null
     * @throws RuntimeException if there's an error accessing the source task context
     */
=======
>>>>>>> 77778c0c6 (DBZ-9020 Add support for multi task connectors)
    public ConnectHeaders contextHeaders() {

        var debeziumContextHeaders = new ConnectHeaders();
        debeziumContextHeaders.add(DEBEZIUM_CONNECTOR_LOGICAL_NAME_HEADER, sourceTaskContext.getConnectorLogicalName(), Schema.STRING_SCHEMA);
        debeziumContextHeaders.add(DEBEZIUM_TASK_ID_HEADER, sourceTaskContext.getTaskId(), Schema.STRING_SCHEMA);
<<<<<<< HEAD
        debeziumContextHeaders.add(DEBEZIUM_CONNECTOR_NAME_HEADER, sourceTaskContext.getConnectorPluginName(), Schema.STRING_SCHEMA);
=======
        debeziumContextHeaders.add(DEBEZIUM_CONNECTOR_NAME_HEADER, sourceTaskContext.getConnectorName(), Schema.STRING_SCHEMA);
>>>>>>> 77778c0c6 (DBZ-9020 Add support for multi task connectors)

        return debeziumContextHeaders;
    }

}
