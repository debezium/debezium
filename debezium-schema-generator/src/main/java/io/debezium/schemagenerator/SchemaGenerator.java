/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schemagenerator;

import java.io.File;
import java.io.IOException;
import java.lang.System.Logger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.ServiceLoader.Provider;
import java.util.stream.Collectors;

import io.debezium.metadata.ComponentMetadata;
import io.debezium.metadata.ComponentMetadataProvider;
import io.debezium.schemagenerator.schema.Schema;
import io.debezium.schemagenerator.schema.SchemaName;

public class SchemaGenerator {

    private static final Logger LOGGER = System.getLogger(SchemaGenerator.class.getName());

    public static void main(String[] args) {
        if (args.length != 5 && args.length != 6) {
            LOGGER.log(Logger.Level.INFO, "There were " + args.length + " arguments:");
            for (int i = 0; i < args.length; ++i) {
                LOGGER.log(Logger.Level.INFO, "  Argument #[" + i + "]: " + args[i]);
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

        new SchemaGenerator().run(formatName, outputDirectory, groupDirectoryPerComponent, filenamePrefix, filenameSuffix, projectArtifactPath);
    }

    private void run(String formatName, Path outputDirectory, boolean groupDirectoryPerComponent, String filenamePrefix, String filenameSuffix,
                     Path projectArtifactPath) {
        List<ComponentMetadata> allMetadata = getMetadata(projectArtifactPath);

        Schema format = getSchemaFormat(formatName);
        LOGGER.log(Logger.Level.INFO, "Using schema format: " + format.getDescriptor().getName());

        if (allMetadata.isEmpty()) {
            throw new RuntimeException("No connectors found in classpath. Exiting!");
        }
        for (ComponentMetadata componentMetadata : allMetadata) {
            LOGGER.log(Logger.Level.INFO, "Creating \"" + format.getDescriptor().getName()
                    + "\" schema for connector: "
                    + componentMetadata.getComponentDescriptor().getDisplayName() + "...");
            String spec = format.getSpec(componentMetadata);

            try {
                String schemaFilename = "";
                if (groupDirectoryPerComponent) {
                    schemaFilename += componentMetadata.getComponentDescriptor().getType() + File.separator;
                }
                if (null != filenamePrefix && !filenamePrefix.isEmpty()) {
                    schemaFilename += filenamePrefix;
                }
                schemaFilename += componentMetadata.getComponentDescriptor().getId();
                if (null != filenameSuffix && !filenameSuffix.isEmpty()) {
                    schemaFilename += filenameSuffix;
                }
                schemaFilename += ".json";
                Path schemaFilePath = outputDirectory.resolve(schemaFilename);
                schemaFilePath.getParent().toFile().mkdirs();
                Files.write(schemaFilePath, spec.getBytes(StandardCharsets.UTF_8));
            }
            catch (IOException e) {
                throw new RuntimeException("Couldn't write file", e);
            }
        }
    }

    private List<ComponentMetadata> getMetadata(Path projectArtifactPath) {
        ServiceLoader<ComponentMetadataProvider> metadataProviders = ServiceLoader.load(ComponentMetadataProvider.class);

        return metadataProviders.stream()
                .filter(p -> isFromProject(p, projectArtifactPath))
                .map(p -> p.get().getConnectorMetadata())
                .collect(Collectors.toList());
    }

    /**
     * Checks if a ServiceLoader provider comes from the current project being built,
     * rather than from a dependency JAR. This ensures that each module only generates
     * schemas for its own metadata providers, not for those inherited from dependencies.
     *
     * @param provider the ServiceLoader provider
     * @param projectArtifactPath path to the project's artifact (JAR or classes directory)
     * @return true if the provider is from the current project, false otherwise
     */
    private boolean isFromProject(ServiceLoader.Provider<ComponentMetadataProvider> provider, Path projectArtifactPath) {
        if (projectArtifactPath == null) {
            // No filtering - include all providers (for backwards compatibility)
            return true;
        }

        try {
            Class<?> providerClass = provider.type();
            String classLocation = providerClass.getProtectionDomain().getCodeSource().getLocation().getPath();
            Path classLocationPath = new File(classLocation).toPath().toAbsolutePath();
            Path normalizedProjectPath = projectArtifactPath.toAbsolutePath();

            boolean isFromProject = classLocationPath.equals(normalizedProjectPath);

            if (!isFromProject) {
                LOGGER.log(Logger.Level.DEBUG, "Skipping metadata provider " + providerClass.getName() +
                        " (from " + classLocationPath + ", not from project " + normalizedProjectPath + ")");
            }

            return isFromProject;
        }
        catch (Exception e) {
            LOGGER.log(Logger.Level.WARNING, "Could not determine location of provider " + provider.type().getName() +
                    ", including it by default", e);
            return true;
        }
    }

    /**
     * Returns the {@link Schema} with the given name, specified via the {@link SchemaName} annotation.
     */
    private Schema getSchemaFormat(String formatName) {
        ServiceLoader<Schema> schemaFormats = ServiceLoader.load(Schema.class);

        if (0 == schemaFormats.stream().count()) {
            throw new RuntimeException("No schema formats found!");
        }

        LOGGER.log(Logger.Level.INFO, "Registered schemas: " +
                schemaFormats.stream().map(schemaFormat -> schemaFormat.get().getDescriptor().getId()).collect(Collectors.joining(", ")));

        Optional<Provider<Schema>> format = schemaFormats
                .stream()
                .filter(p -> p.type().getAnnotation(SchemaName.class).value().equals(formatName))
                .findFirst();

        return format.orElseThrow().get();
    }
}
