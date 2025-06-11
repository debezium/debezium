/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage.dataset;

import java.util.Map;

/**
 * Resolver interface for determining the namespace of output datasets in lineage tracking.
 * <p>
 * This interface is responsible for resolving the appropriate namespace identifier for output datasets
 * based on connector configuration. The namespace is used to uniquely identify the destination
 * of data in lineage tracking systems and follows OpenLineage specifications.
 * </p>
 * <p>
 * Implementations should construct namespace identifiers that conform to the OpenLineage dataset
 * naming conventions, typically in the format of a URI that identifies the data destination location
 * and connection details.
 * </p>
 *
 * @see <a href="https://openlineage.io/docs/spec/naming#dataset-naming">OpenLineage Dataset Naming Convention</a>
 */
public interface OutputDatasetNamespaceResolver {

    String resolve(Map<String, String> configuration);
}
