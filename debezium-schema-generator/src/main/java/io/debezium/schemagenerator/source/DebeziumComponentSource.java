/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schemagenerator.source;

import java.io.File;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.file.Path;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

import io.debezium.metadata.ComponentMetadata;
import io.debezium.metadata.ComponentMetadataProvider;

/**
 * Discovers Debezium component metadata using the ServiceLoader mechanism.
 *
 * <p>This source loads all {@link ComponentMetadataProvider} implementations
 * registered via {@code META-INF/services} and collects their metadata.
 *
 * <p>When a {@code projectArtifactPath} is provided, only metadata providers
 * from that specific project artifact are included. This ensures each module
 * generates schemas only for its own components, not inherited dependencies.
 *
 * @see ComponentMetadataProvider
 */
public class DebeziumComponentSource implements ComponentSource {

    private static final Logger LOGGER = System.getLogger(DebeziumComponentSource.class.getName());

    private final Path projectArtifactPath;

    /**
     * Creates a Debezium component source.
     *
     * @param projectArtifactPath path to the project's artifact (JAR or classes directory),
     *                           or null to include all providers without filtering
     */
    public DebeziumComponentSource(Path projectArtifactPath) {
        this.projectArtifactPath = projectArtifactPath;
    }

    @Override
    public List<ComponentMetadata> discoverComponents() {

        ServiceLoader<ComponentMetadataProvider> metadataProviders = ServiceLoader.load(ComponentMetadataProvider.class);

        return metadataProviders.stream()
                .filter(this::isFromProject)
                .flatMap(p -> p.get().getConnectorMetadata().stream())
                .collect(Collectors.toList());
    }

    @Override
    public String getName() {
        return "Debezium Components";
    }

    /**
     * Checks if a ServiceLoader provider comes from the current project being built,
     * rather than from a dependency JAR. This ensures that each module only generates
     * schemas for its own metadata providers, not for those inherited from dependencies.
     *
     * @param provider the ServiceLoader provider
     * @return true if the provider is from the current project, false otherwise
     */
    private boolean isFromProject(ServiceLoader.Provider<ComponentMetadataProvider> provider) {

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
                LOGGER.log(Level.DEBUG, "Skipping metadata provider " + providerClass.getName() +
                        " (from " + classLocationPath + ", not from project " + normalizedProjectPath + ")");
            }

            return isFromProject;
        }
        catch (Exception e) {
            LOGGER.log(Level.WARNING, "Could not determine location of provider " + provider.type().getName() +
                    ", including it by default", e);
            return true;
        }
    }
}
