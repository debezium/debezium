/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schemagenerator.source.kafkaconnect;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.config.Field;
import io.debezium.metadata.ComponentMetadata;
import io.debezium.schemagenerator.source.ComponentSource;

/**
 * Discovers Kafka Connect component metadata using Jandex-based bytecode scanning.
 *
 * <p>This source discovers all standard Kafka Connect components:
 * <ul>
 *   <li>Transformations (SMTs)</li>
 *   <li>Converters</li>
 *   <li>Header Converters</li>
 *   <li>Predicates</li>
 * </ul>
 *
 * <p>The discovery process:
 * <ol>
 *   <li>Use {@link KafkaConnectDiscoveryService} to find all KC component classes</li>
 *   <li>Extract {@link ConfigDef} from each class using {@link ConfigDefExtractor}</li>
 *   <li>Convert ConfigDef to Debezium {@link Field.Set} using {@link ConfigDefAdapter}</li>
 *   <li>Create {@link ComponentMetadata} for each component</li>
 * </ol>
 *
 * <p>Only KC components from {@code org.apache.kafka.*} packages are discovered
 * to avoid picking up third-party or Debezium implementations.
 *
 * @see KafkaConnectDiscoveryService
 * @see ConfigDefExtractor
 * @see ConfigDefAdapter
 */
public class KafkaConnectComponentSource implements ComponentSource {

    private static final Logger LOGGER = System.getLogger(KafkaConnectComponentSource.class.getName());

    private final KafkaConnectDiscoveryService discoveryService;
    private final ConfigDefExtractor configDefExtractor;
    private final ConfigDefAdapter configDefAdapter;

    /**
     * Creates a Kafka Connect component source.
     *
     * @param discoveryService the discovery service
     * @param configDefExtractor the config extractor
     * @param configDefAdapter the config adapter
     */
    public KafkaConnectComponentSource(
                                       KafkaConnectDiscoveryService discoveryService,
                                       ConfigDefExtractor configDefExtractor,
                                       ConfigDefAdapter configDefAdapter) {
        this.discoveryService = discoveryService;
        this.configDefExtractor = configDefExtractor;
        this.configDefAdapter = configDefAdapter;
    }

    @Override
    public List<ComponentMetadata> discoverComponents() {

        LOGGER.log(Level.INFO, "Discovering Kafka Connect components...");

        Map<ComponentType, List<Class<?>>> components = discoveryService.discoverKafkaConnectComponents();

        List<ComponentMetadata> allMetadata = components.entrySet().stream()
                .peek(entry -> LOGGER.log(Level.INFO,
                        "Processing " + entry.getValue().size() + " " + entry.getKey().getDisplayName() + "(s)"))
                .flatMap(entry -> entry.getValue().stream()
                        .flatMap(componentClass -> {
                            try {
                                return createComponentMetadata(componentClass).stream();
                            }
                            catch (Exception e) {
                                LOGGER.log(Level.WARNING,
                                        "Failed to create metadata for " + componentClass.getName(), e);
                                return Optional.<ComponentMetadata> empty().stream();
                            }
                        }))
                .collect(Collectors.toList());

        LOGGER.log(Level.INFO,
                "Discovered " + allMetadata.size() + " Kafka Connect component(s)");

        return allMetadata;
    }

    @Override
    public String getName() {
        return "Kafka Connect Components";
    }

    /**
     * Creates ComponentMetadata for a single KC component class.
     *
     * @param componentClass the component class
     * @return Optional containing ComponentMetadata, or empty if no ConfigDef could be extracted
     */
    private Optional<ComponentMetadata> createComponentMetadata(Class<?> componentClass) {

        Optional<ConfigDef> configDefOpt = configDefExtractor.extractConfigDef(componentClass);

        if (configDefOpt.isEmpty()) {
            LOGGER.log(Level.DEBUG,
                    "No ConfigDef found for " + componentClass.getName() + ", skipping");
            return Optional.empty();
        }

        Field.Set fields = configDefAdapter.adapt(configDefOpt.get());

        LOGGER.log(Level.DEBUG,
                "Created metadata for " + componentClass.getName() +
                        " with " + fields.asArray().length + " field(s)");

        return Optional.of(new KafkaConnectComponentMetadata(componentClass, fields));
    }
}
