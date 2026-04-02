/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schemagenerator;

import java.io.File;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.ServiceLoader.Provider;
import java.util.Set;
import java.util.stream.Collectors;

import org.reflections.Reflections;
import org.reflections.scanners.Scanners;
import org.reflections.util.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.metadata.ComponentMetadata;
import io.debezium.metadata.ConfigDescriptor;
import io.debezium.schemagenerator.schema.Schema;
import io.debezium.schemagenerator.schema.SchemaName;
import io.debezium.schemagenerator.source.ComponentSource;
import io.debezium.schemagenerator.source.DebeziumComponentSource;
import io.debezium.schemagenerator.source.kafkaconnect.ConfigDefAdapter;
import io.debezium.schemagenerator.source.kafkaconnect.ConfigDefExtractor;
import io.debezium.schemagenerator.source.kafkaconnect.KafkaConnectComponentSource;
import io.debezium.schemagenerator.source.kafkaconnect.KafkaConnectDiscoveryService;

public class SchemaGenerator {

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaGenerator.class);
    private static SchemaWriter schemaWriter;

    public static void main(String[] args) {
        if (args.length != 5 && args.length != 6) {
            LOGGER.info("There were {} arguments:", args.length);
            for (int i = 0; i < args.length; ++i) {
                LOGGER.info("  Argument #[{}]: {}", i, args[i]);
            }
            throw new IllegalArgumentException(
                    "Usage: SchemaGenerator <format-name> <output-directory> <groupDirectoryPerComponent> <filenamePrefix> <filenameSuffix> [projectArtifactPath]");
        }

        String formatName = args[0].trim();
        Path outputDirectory = new File(args[1]).toPath();
        boolean groupDirectoryPerComponent = Boolean.parseBoolean(args[2]);
        String filenamePrefix = args[3];
        String filenameSuffix = args[4];
        Path projectArtifactPath = args.length == 6 ? new File(args[5]).toPath() : null;

        Schema schema = getSchemaFormat(formatName);
        LOGGER.info("Using schema format: {}", schema.getDescriptor().getName());

        SchemaGeneratorConfig config = new SchemaGeneratorConfig(
                schema,
                outputDirectory,
                groupDirectoryPerComponent,
                filenamePrefix,
                filenameSuffix,
                projectArtifactPath);

        schemaWriter = new SchemaWriter(config);

        new SchemaGenerator().run(config);
    }

    private void run(SchemaGeneratorConfig config) {
        processDebeziumComponents(config);
        processKafkaConnectComponents(config);
    }

    private void processDebeziumComponents(SchemaGeneratorConfig config) {

        ComponentSource componentSource = new DebeziumComponentSource(config.projectArtifactPath());

        LOGGER.info("Discovering components from: {}", componentSource.getName());
        List<ComponentMetadata> allMetadata = componentSource.discoverComponents();
        LOGGER.info("  Found {} component(s)", allMetadata.size());

        if (allMetadata.isEmpty()) {
            throw new RuntimeException("No connectors found in classpath. Exiting!");
        }

        validateDescriptorRegistration(allMetadata, config.projectArtifactPath());

        generateSchemas(allMetadata, config);
    }

    private void processKafkaConnectComponents(SchemaGeneratorConfig config) {

        ComponentSource componentSource = new KafkaConnectComponentSource(
                new KafkaConnectDiscoveryService(),
                new ConfigDefExtractor(),
                new ConfigDefAdapter());

        LOGGER.info("Discovering components from: {}", componentSource.getName());
        List<ComponentMetadata> allMetadata = componentSource.discoverComponents();
        LOGGER.info("  Found {} component(s)", allMetadata.size());

        if (allMetadata.isEmpty()) {
            LOGGER.info("No Kafka Connect components found, skipping");
            return;
        }

        generateSchemas(allMetadata, config);
    }

    private void generateSchemas(List<ComponentMetadata> allMetadata, SchemaGeneratorConfig config) {

        LOGGER.info("Generating {} schema(s)...", allMetadata.size());

        for (ComponentMetadata componentMetadata : allMetadata) {
            LOGGER.debug("Creating \"{}\" schema for component: {}...",
                    config.schema().getDescriptor().getName(),
                    componentMetadata.getComponentDescriptor().getDisplayName());

            String spec = config.schema().getSpec(componentMetadata);
            schemaWriter.writeSchema(componentMetadata, spec);
        }

        LOGGER.info("Successfully generated {} schema(s)", allMetadata.size());
    }

    /**
     * Returns the {@link Schema} with the given name, specified via the {@link SchemaName} annotation.
     */
    private static Schema getSchemaFormat(String formatName) {
        ServiceLoader<Schema> schemaFormats = ServiceLoader.load(Schema.class);

        if (schemaFormats.stream().findAny().isEmpty()) {
            throw new RuntimeException("No schema formats found!");
        }

        LOGGER.debug("Registered schemas: {}",
                schemaFormats.stream().map(schemaFormat -> schemaFormat.get().getDescriptor().getId()).collect(Collectors.joining(", ")));

        Optional<Provider<Schema>> format = schemaFormats
                .stream()
                .filter(p -> p.type().getAnnotation(SchemaName.class).value().equals(formatName))
                .findFirst();

        return format.orElseThrow().get();
    }

    /**
     * Validates that all ConfigDescriptor implementations in the project are properly registered
     * in a ComponentMetadataProvider. This prevents accidentally forgetting to register new
     * transforms, converters, or connectors.
     *
     * @param allMetadata the metadata from all registered providers
     * @param projectArtifactPath the path to the current project artifact
     */
    private void validateDescriptorRegistration(List<ComponentMetadata> allMetadata, Path projectArtifactPath) {

        if (projectArtifactPath == null) {
            LOGGER.debug("Skipping ConfigDescriptor registration validation (no project artifact path)");
            return;
        }

        try {
            Set<String> allDescriptors = findConfigDescriptorImplementations(projectArtifactPath);

            if (allDescriptors.isEmpty()) {
                LOGGER.debug("No ConfigDescriptor implementations found in this module");
                return;
            }

            Set<String> registeredDescriptors = allMetadata.stream()
                    .map(m -> m.getComponentDescriptor().getClassName())
                    .collect(Collectors.toSet());

            Set<String> unregistered = new HashSet<>(allDescriptors);
            unregistered.removeAll(registeredDescriptors);

            if (!unregistered.isEmpty()) {
                LOGGER.error("");
                LOGGER.error("========================================");
                LOGGER.error("ConfigDescriptor Registration Validation FAILED!");
                LOGGER.error("========================================");
                LOGGER.error("The following ConfigDescriptor implementations are not registered:");
                unregistered.stream().sorted().forEach(className -> LOGGER.error("  - {}", className));
                LOGGER.error("");
                LOGGER.error("Please add them to the appropriate ComponentMetadataProvider:");
                LOGGER.error("  - For transforms: TransformsMetadataProvider");
                LOGGER.error("  - For converters: ConverterMetadataProvider");
                LOGGER.error("  - For connectors: Create a connector-specific MetadataProvider");
                LOGGER.error("========================================");
                LOGGER.error("");
                throw new RuntimeException("ConfigDescriptor registration validation failed. "
                        + unregistered.size() + " unregistered implementation(s) found.");
            }

            LOGGER.info("ConfigDescriptor registration validation passed: All {} implementation(s) are properly registered.",
                    allDescriptors.size());
        }
        catch (RuntimeException e) {
            throw e;
        }
        catch (Exception e) {
            LOGGER.warn("Could not validate ConfigDescriptor registration: {}", e.getMessage(), e);
        }
    }

    /**
     * Finds all concrete classes implementing ConfigDescriptor in the given project artifact.
     * Excludes deprecated classes as they are typically backward-compatibility wrappers.
     *
     * @param projectArtifactPath the path to the JAR or classes directory
     * @return set of fully qualified class names
     */
    private Set<String> findConfigDescriptorImplementations(Path projectArtifactPath) throws Exception {

        URL url = projectArtifactPath.toUri().toURL();

        Reflections reflections = new Reflections(new ConfigurationBuilder()
                .setUrls(url)
                .setScanners(Scanners.SubTypes));

        Set<Class<? extends ConfigDescriptor>> descriptorClasses = reflections.getSubTypesOf(ConfigDescriptor.class);

        return descriptorClasses.stream()
                .filter(cls -> !cls.isInterface())
                .filter(cls -> !Modifier.isAbstract(cls.getModifiers()))
                .filter(cls -> !cls.isAnnotationPresent(Deprecated.class))
                .map(Class::getName)
                .collect(Collectors.toSet());
    }
}
