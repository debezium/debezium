/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.spi;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.converters.spi.RecordParser;

/**
 * A {@link java.util.ServiceLoader} interface that connectors should implement to have a common way to parse event.
 *
 * @author Mario Fiore Vitale
 */
public interface RecordParserProvider {
    /**
     * The connector name specified in the record's source info block.
     *
     * @return the provider name
     */
    String getName();

    /**
     * Create a concrete parser of a change record for the connector.
     *
     * @param schema the schema of the record
     * @param record the value of the record
     * @return a concrete parser
     */
    RecordParser createParser(Schema schema, Struct record);
}
