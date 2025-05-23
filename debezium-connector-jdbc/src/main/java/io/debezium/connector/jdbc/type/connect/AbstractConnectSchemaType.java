/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.connect;

import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.connector.jdbc.type.JdbcType;

/**
 * An abstract implementation of {@link JdbcType} that all Kafka Connect based schema types should be derived.
 *
 * This abstract implementation is used as a marker object to designate types that are operating on the
 * raw {@link org.apache.kafka.connect.data.Schema.Type} values rather than custom schema types that are
 * contributed by Kafka Connect, such as Date or Time, or other third parties such as Debezium.
 *
 * @author Chris Cranford
 */
public abstract class AbstractConnectSchemaType extends AbstractType {

}
