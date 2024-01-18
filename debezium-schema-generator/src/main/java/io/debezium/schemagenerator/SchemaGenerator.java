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

import io.debezium.metadata.ConnectorMetadata;
import io.debezium.metadata.ConnectorMetadataProvider;
import io.debezium.schemagenerator.schema.Schema;
import io.debezium.schemagenerator.schema.SchemaName;

public class SchemaGenerator {

    private static final Logger LOGGER = System.getLogger(SchemaGenerator.class.getName());

    public static void main(String[] args) {
        if (args.length != 5) {
            LOGGER.log(Logger.Level.INFO, "There were " + args.length + " arguments:");
            for (int i = 0; i < args.length; ++i) {
                LOGGER.log(Logger.Level.INFO, "  Argument #[" + i + "]: " + args[i]);
            }
            throw new IllegalArgumentException("Usage: SchemaGenerator <format-name> <output-directory> <groupDirectoryPerConnector> <filenamePrefix> <filenameSuffix>");
        }

        String formatName = args[0].trim();
        Path outputDirectory = new File(args[1]).toPath();
        boolean groupDirectoryPerConnector = Boolean.parseBoolean(args[2]);
        String filenamePrefix = args[3];
        String filenameSuffix = args[4];

        new SchemaGenerator().run(formatName, outputDirectory, groupDirectoryPerConnector, filenamePrefix, filenameSuffix);
    }

    private void run(String formatName, Path outputDirectory, boolean groupDirectoryPerConnector, String filenamePrefix, String filenameSuffix) {
        List<ConnectorMetadata> allMetadata = getMetadata();

        Schema format = getSchemaFormat(formatName);
        LOGGER.log(Logger.Level.INFO, "Using schema format: " + format.getDescriptor().getName());

        if (allMetadata.isEmpty()) {
            throw new RuntimeException("No connectors found in classpath. Exiting!");
        }
        for (ConnectorMetadata connectorMetadata : allMetadata) {
            LOGGER.log(Logger.Level.INFO, "Creating \"" + format.getDescriptor().getName()
                    + "\" schema for connector: "
                    + connectorMetadata.getConnectorDescriptor().getDisplayName() + "...");
            JsonSchemaCreatorService jsonSchemaCreatorService = new JsonSchemaCreatorService(connectorMetadata, format.getFieldFilter());
            org.eclipse.microprofile.openapi.models.media.Schema buildConnectorSchema = jsonSchemaCreatorService.buildConnectorSchema();
            String spec = format.getSpec(buildConnectorSchema);

            try {
                String schemaFilename = "";
                if (groupDirectoryPerConnector) {
                    schemaFilename += connectorMetadata.getConnectorDescriptor().getId() + File.separator;
                }
                if (null != filenamePrefix && !filenamePrefix.isEmpty()) {
                    schemaFilename += filenamePrefix;
                }
                schemaFilename += connectorMetadata.getConnectorDescriptor().getId();
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

    private List<ConnectorMetadata> getMetadata() {
        ServiceLoader<ConnectorMetadataProvider> metadataProviders = ServiceLoader.load(ConnectorMetadataProvider.class);

        return metadataProviders.stream()
                .map(p -> p.get().getConnectorMetadata())
                .collect(Collectors.toList());
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
