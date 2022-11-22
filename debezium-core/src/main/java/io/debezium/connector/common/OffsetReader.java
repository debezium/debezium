/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.common;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;

/**
 * Provides access to the partition offsets stored by connectors.
 */
public class OffsetReader<P extends Partition, O extends OffsetContext, L extends OffsetContext.Loader<O>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(OffsetReader.class);

    private final OffsetStorageReader reader;
    private final L loader;

    public OffsetReader(OffsetStorageReader reader, L loader) {
        this.reader = reader;
        this.loader = loader;
    }

    /**
     * Given the collection of connector-specific task partitions, returns their respective connector-specific offsets.
     *
     * If there is no offset stored for a given partition, the corresponding key will be mapped to a null.
     */
    public Map<P, O> offsets(Set<P> partitions) {
        Set<Map<String, String>> sourcePartitions = partitions.stream()
                .map(Partition::getSupportedFormats)
                .flatMap(List::stream)
                .collect(Collectors.toCollection(HashSet::new));

        Map<Map<String, String>, Map<String, Object>> sourceOffsets = reader.offsets(sourcePartitions);

        Map<P, O> offsets = new LinkedHashMap<>();
        partitions.forEach(partition -> {
            Optional<Map<String, Object>> sourceOffset = partition.getSupportedFormats().stream()
                    .map(sourceOffsets::get)
                    .filter(Objects::nonNull)
                    .findFirst();

            O offset = null;
            if (sourceOffset.isPresent()) {
                offset = loader.load(sourceOffset.get());
            }
            offsets.put(partition, offset);
        });

        return offsets;
    }
}
