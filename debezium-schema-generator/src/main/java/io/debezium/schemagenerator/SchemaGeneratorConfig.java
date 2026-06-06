/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schemagenerator;

import java.nio.file.Path;

import io.debezium.schemagenerator.schema.Schema;

/**
 * Configuration for schema generation.
 *
 * @param schema the schema format to use for generation
 * @param outputDirectory directory where generated schemas will be written
 * @param groupDirectoryPerComponent whether to create subdirectories per component type
 * @param filenamePrefix prefix to add to generated schema filenames
 * @param filenameSuffix suffix to add to generated schema filenames
 * @param projectArtifactPath path to the project artifact (for filtering Debezium components)
 */
public record SchemaGeneratorConfig(
        Schema schema,
        Path outputDirectory,
        boolean groupDirectoryPerComponent,
        String filenamePrefix,
        String filenameSuffix,
        Path projectArtifactPath) {
}
