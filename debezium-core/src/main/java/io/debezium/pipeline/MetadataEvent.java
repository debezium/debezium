/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline;

/**
 * Contract that describes a metadata event isn't necessarily dispatched into the change event stream but
 * is potentially of interest to other parts of the framework such as for capture by metrics.
 *
 * @author Chris Cranford
 */
public interface MetadataEvent {
}
