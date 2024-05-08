/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.converters.spi;

import io.debezium.converters.recordandmetadata.RecordAndMetadata;

/**
 * A {@link java.util.ServiceLoader} interface that connectors should implement if they wish to provide
 * a way to emit change events using the CloudEvents converter and format.
 *
 * @author Chris Cranford
 */
public interface CloudEventsProvider {
    /**
     * The connector name specified in the record's source info block.
     *
     * @return the provider name
     */
    String getName();

    /**
     * Create a concrete CloudEvents maker using the outputs of a record parser. Also need to specify the data content
     * type (that is the serialization format of the data attribute).
     *
     * @param recordAndMetadata a structure containing the record and its metadata
     * @param contentType       the data content type of CloudEvents
     * @param dataSchemaUriBase the URI of the schema in case of Avro; may be null
     * @return a concrete CloudEvents maker
     */
    CloudEventsMaker createMaker(RecordAndMetadata recordAndMetadata, SerializerType contentType, String dataSchemaUriBase,
                                 String cloudEventsSchemaName);
}
