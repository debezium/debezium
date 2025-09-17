/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage.dataset;

import io.debezium.openlineage.dataset.DatasetMetadata.DataStore;

/**
 * Factory interface for creating dataset namespace resolvers.
 * <p>
 * This factory provides methods to create appropriate namespace resolvers for both input and output
 * datasets based on the connector name. Namespace resolvers are responsible for determining the
 * correct namespace or schema context for datasets in lineage tracking scenarios.
 * </p>
 * <p>
 * Implementations of this factory should be able to create resolvers that are specific to different
 * types of connectors and their respective namespace requirements.
 * </p>
 */
public interface DatasetNamespaceResolverFactory {

    DatasetNamespaceResolver create(DataStore dataStore, String connectorName);

}
