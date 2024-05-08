/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import io.debezium.data.Envelope;
import io.debezium.data.Envelope.FieldName;

/**
 * Field names specific to MongoDB change event {@link Envelope}s.
 *
 * @author Gunnar Morling
 * @see FieldName
 */
public class MongoDbFieldName {

    // Change Streams fields
    public static final String UPDATE_DESCRIPTION = "updateDescription";
    public static final String REMOVED_FIELDS = "removedFields";
    public static final String UPDATED_FIELDS = "updatedFields";
    public static final String TRUNCATED_ARRAYS = "truncatedArrays";
    public static final String ARRAY_FIELD_NAME = "field";
    public static final String ARRAY_NEW_SIZE = "size";
}
