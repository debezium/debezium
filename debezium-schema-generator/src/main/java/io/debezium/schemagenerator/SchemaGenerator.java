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

import org.eclipse.microprofile.openapi.models.media.Schema;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

import io.debezium.metadata.ConnectorMetadata;
import io.debezium.metadata.ConnectorMetadataProvider;
import io.debezium.schemagenerator.formats.ApiFormat;
import io.debezium.schemagenerator.formats.ApiFormatName;

public class SchemaGenerator {

    public static void main(String[] args) {
        if (args.length != 2) {
            throw new IllegalArgumentException("Usage: SchemaGenerator <format-name> <output-directory>");
        }

        String formatName = args[0].trim();
        Path outputDirectory = new File(args[1]).toPath();

        new SchemaGenerator().run(formatName, outputDirectory);
    }

    private void run(String formatName, Path outputDirectory) {
        List<ConnectorMetadata> allMetadata = getMetadata();

        ApiFormat format = getApiFormat(formatName);

        for (ConnectorMetadata connectorMetadata : allMetadata) {
            JsonSchemaCreatorService jsonSchemaCreatorService = new JsonSchemaCreatorService(connectorMetadata, format.getFieldFilter());
            Schema buildConnectorSchema = jsonSchemaCreatorService.buildConnectorSchema();
            String spec = format.getSpec(buildConnectorSchema);

            try {
                Files.write(spec.getBytes(Charsets.UTF_8), outputDirectory.resolve(connectorMetadata.getConnectorDescriptor().getId() + ".json").toFile());
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
     * Returns the {@link ApiFormat} with the given name, specified via the {@link ApiFormatName} annotation.
     */
    private ApiFormat getApiFormat(String formatName) {
        Optional<Provider<ApiFormat>> format = ServiceLoader.load(ApiFormat.class)
                .stream()
                .filter(p -> p.type().getAnnotation(ApiFormatName.class).value().equals(formatName))
                .findFirst();

        return format.orElseThrow().get();
    }
}
