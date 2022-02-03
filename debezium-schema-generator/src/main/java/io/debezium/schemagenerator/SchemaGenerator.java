/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schemagenerator;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.ServiceLoader.Provider;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

import io.debezium.metadata.ConnectorMetadata;
import io.debezium.metadata.ConnectorMetadataProvider;
import io.debezium.schemagenerator.schema.Schema;
import io.debezium.schemagenerator.schema.SchemaName;

public class SchemaGenerator {

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaGenerator.class);

    public static void main(String[] args) {
        if (args.length != 5) {
            throw new IllegalArgumentException("Usage: SchemaGenerator <format-name> <output-directory> <groupDirectoryPerConnector> <filenamePrefix> <filenameSuffix>");
        }

        String formatName = args[0].trim();
        Path outputDirectory = new File(args[1]).toPath();
        boolean groupDirectoryPerConnector = Boolean.parseBoolean(args[2]);
        String filenamePrefix = args[3];
        String filenameSuffix = args[4];

        new SchemaGenerator().run(formatName, outputDirectory, groupDirectoryPerConnector, filenamePrefix, filenameSuffix);
    }

    public void run(String formatName, Path outputDirectory, Boolean groupDirectoryPerConnector, String filenamePrefix, String filenameSuffix) {
        List<ConnectorMetadata> allMetadata = getMetadata();

        Schema format = getSchemaFormat(formatName);
        LOGGER.info("Using schema format: {}", format.getDescriptor().getName());

        if (allMetadata.isEmpty()) {
            throw new RuntimeException("No connectors found in classpath. Exiting!");
        }
        for (ConnectorMetadata connectorMetadata : allMetadata) {
            LOGGER.info("Creating \"{}\" schema for connector: {}...", format.getDescriptor().getName(), connectorMetadata.getConnectorDescriptor().getName());
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
                Files.write(spec.getBytes(Charsets.UTF_8), schemaFilePath.toFile());
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

        LOGGER.info("Registered schemas: {}", schemaFormats.stream().map(schemaFormat -> schemaFormat.get().getDescriptor().getId()).collect(Collectors.joining(", ")));

        Optional<Provider<Schema>> format = schemaFormats
                .stream()
                .filter(p -> p.type().getAnnotation(SchemaName.class).value().equals(formatName))
                .findFirst();

        return format.orElseThrow().get();
    }
}
