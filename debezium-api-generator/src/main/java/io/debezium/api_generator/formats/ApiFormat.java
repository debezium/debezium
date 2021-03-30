/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.api_generator.formats;

import java.util.List;
import java.util.Map;

import org.eclipse.microprofile.openapi.models.media.Schema;

public interface ApiFormat {

    ApiFormatDescriptor getDescriptor();

    void configure(Map<String, Object> config);

    String getSpec(List<Schema> connectorSchemas);

}
