/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schemagenerator;

import java.io.File;
import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.file.Files;
import java.nio.file.Path;

import io.debezium.DebeziumException;
import io.debezium.metadata.ComponentMetadata;

/**
 * Handles writing schema specifications to files.
 *
 * <p>This class is responsible for:
 * <ul>
 *   <li>Computing the output file path based on configuration and component metadata</li>
 *   <li>Creating necessary parent directories</li>
 *   <li>Writing the schema specification to disk</li>
 * </ul>
 */
public class SchemaWriter {

    private static final Logger LOGGER = System.getLogger(SchemaWriter.class.getName());
    public static final String JSON_FILE_EXTENSION = ".json";

    private final SchemaGeneratorConfig config;

    /**
     * Creates a schema writer with the given configuration.
     *
     * @param config the schema generator configuration
     */
    public SchemaWriter(SchemaGeneratorConfig config) {
        this.config = config;
    }

    /**
     * Writes a schema specification to a file.
     *
     * @param componentMetadata metadata about the component
     * @param schemaSpec the schema specification content to write
     * @throws SchemaWriteException if writing fails
     */
    public void writeSchema(ComponentMetadata componentMetadata, String schemaSpec) {
        try {
            Path schemaFilePath = buildFilePath(componentMetadata);
            Files.writeString(schemaFilePath, schemaSpec);
            LOGGER.log(Level.DEBUG, "Wrote schema to: " + schemaFilePath);
        }
        catch (IOException e) {
            throw new SchemaWriteException("Failed to write schema for " +
                    componentMetadata.getComponentDescriptor().getClassName(), e);
        }
    }

    /**
     * Builds the file path for a component's schema file.
     *
     * @param componentMetadata the component metadata
     * @return the complete file path
     */
    private Path buildFilePath(ComponentMetadata componentMetadata) {
        String schemaFilename = "";

        if (config.groupDirectoryPerComponent()) {
            schemaFilename += componentMetadata.getComponentDescriptor().getType() + File.separator;
        }

        if (config.filenamePrefix() != null && !config.filenamePrefix().isEmpty()) {
            schemaFilename += config.filenamePrefix();
        }

        schemaFilename += componentMetadata.getComponentDescriptor().getId();

        if (config.filenameSuffix() != null && !config.filenameSuffix().isEmpty()) {
            schemaFilename += config.filenameSuffix();
        }

        schemaFilename += JSON_FILE_EXTENSION;

        Path schemaFilePath = config.outputDirectory().resolve(schemaFilename);
        schemaFilePath.getParent().toFile().mkdirs();

        return schemaFilePath;
    }

    /**
     * Exception thrown when schema writing fails.
     */
    public static class SchemaWriteException extends DebeziumException {

        public SchemaWriteException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
