/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schemagenerator.schema;

import java.util.Map;

import io.debezium.config.Field;

public interface Schema {

    SchemaDescriptor getDescriptor();

    void configure(Map<String, Object> config);

    String getSpec(org.eclipse.microprofile.openapi.models.media.Schema connectorSchema);

    /**
     * Returns a filter to be applied to the fields of the schema. Only matching
     * fields will be part of the serialized API spec. Defaults to including all the
     * fields of the schema.
     */
    default FieldFilter getFieldFilter() {
        return f -> true;
    }

    interface FieldFilter {
        boolean include(Field field);
    }

}
