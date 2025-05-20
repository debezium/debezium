/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type;

import java.sql.Types;

import org.apache.kafka.connect.data.Schema;

/**
 * An abstract base class for all temporal date implementations of {@link Type}.
 *
 * @author Chris Cranford
 */
public abstract class AbstractDateType extends AbstractTemporalType {
    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        return getDialect().getJdbcTypeName(Types.DATE);
    }
}
