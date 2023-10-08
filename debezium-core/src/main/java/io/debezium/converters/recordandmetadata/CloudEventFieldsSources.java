/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.converters.recordandmetadata;

import io.debezium.converters.CloudEventsConverterConfig.IdSource;
import io.debezium.converters.CloudEventsConverterConfig.MetadataLocation;
import io.debezium.converters.CloudEventsConverterConfig.TypeSource;

/**
 * The structure containing information from where to retrieve value for some CloudEvents fields
 *
 * @author Roman Kudryashov
 */
public class CloudEventFieldsSources {

    private final IdSource idSource;
    private final TypeSource typeSource;
    private final MetadataLocation metadataSource;

    public CloudEventFieldsSources(IdSource idSource, TypeSource typeSource, MetadataLocation metadataSource) {
        this.idSource = idSource;
        this.typeSource = typeSource;
        this.metadataSource = metadataSource;
    }

    public IdSource getIdSource() {
        return idSource;
    }

    public TypeSource getTypeSource() {
        return typeSource;
    }

    public MetadataLocation getMetadataSource() {
        return metadataSource;
    }
}
