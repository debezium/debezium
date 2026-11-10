/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type;

import io.debezium.metadata.CollectionId;

/**
 * The context of a default value in the target database.
 *
 * @param collectionId the table in the target database the column belongs to, may be {@code null}
 * @param columnName the name of the column in the target database, may be {@code null}
 */
public record DefaultValueContext(CollectionId collectionId, String columnName) {
}
