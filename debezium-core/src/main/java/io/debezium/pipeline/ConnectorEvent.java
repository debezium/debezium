/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline;

/**
 * A marker interface for an event with the connector that isn't dispatched to the change event stream but
 * instead is potentially of interest to other parts of the framework such as metrics.
 *
 * @author Chris Cranford
 */
public interface ConnectorEvent {
}
