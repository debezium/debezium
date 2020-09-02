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

    public static final String PATCH = "patch";
    public static final String FILTER = "filter";
}
