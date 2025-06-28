/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.runtime.configuration;

import java.util.Map;

/**
 * Contains the Configuration of the Datasource that can be taken from DevServices or Quarkus datasource
 */
public interface QuarkusDatasourceConfiguration {

    /**
     * return the configuration compatible with Debezium
     */
    Map<String, String> asDebezium();

    /**
     * Identify the default definition of datasource
     */
    boolean isDefault();
}
