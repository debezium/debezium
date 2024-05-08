/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import io.debezium.pipeline.ConnectorEvent;

/**
 * An event that implies that a connection was lost or with the source database.
 *
 * @author Chris Cranford
 */
public class DisconnectEvent implements ConnectorEvent {
}
