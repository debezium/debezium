/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.common;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.source.SourceConnector;

import io.debezium.config.Configuration;
import io.debezium.rest.model.DataCollection;

/**
 * Base class for Debezium's CDC {@link SourceConnector} implementations.
 * Provides functionality common to all CDC connectors, such as validation.
 */
public abstract class BaseSourceConnector extends SourceConnector {

    protected abstract Map<String, ConfigValue> validateAllFields(Configuration config);

    public abstract List<DataCollection> getMatchingCollections(Configuration config);
}
