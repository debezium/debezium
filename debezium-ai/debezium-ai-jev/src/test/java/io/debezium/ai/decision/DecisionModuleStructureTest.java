/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.decision;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.connect.transforms.Transformation;
import org.junit.jupiter.api.Test;

import io.debezium.ai.decision.metadata.DecisionMetadataProvider;
import io.debezium.ai.decision.spi.DecisionServiceFactory;
import io.debezium.metadata.ComponentMetadataProvider;

/**
 * Verifies the service registrations and the dependency direction between the packages of the module:
 * {@code api} imports nothing from the module, {@code spi} only {@code api}, {@code decision} only {@code api} and {@code spi},
 * {@code systemone} uses the API and SPI, and provider factories use the shared System One implementation.
 *
 * @author Jiri Pechanec
 */
public class DecisionModuleStructureTest {

    private static final String BASE = "io.debezium.ai.decision";
    private static final Path SOURCES = Path.of("src", "main", "java", "io", "debezium", "ai", "decision");
    private static final Pattern IMPORT = Pattern.compile("^import\\s+(?:static\\s+)?([\\w.]+);", Pattern.MULTILINE);

    @Test
    public void shouldDiscoverTheDecisionFactoriesByName() {
        final var names = ServiceLoader.load(DecisionServiceFactory.class).stream()
                .map(provider -> provider.get().name())
                .collect(Collectors.toList());

        assertThat(names).contains("jev", "kev", "stub");
    }

    @Test
    public void shouldRegisterTransformationAndMetadataProvider() {
        assertThat(ServiceLoader.load(Transformation.class).stream().map(ServiceLoader.Provider::type))
                .anyMatch(DecisionToHeaders.class::equals);
        assertThat(ServiceLoader.load(ComponentMetadataProvider.class).stream().map(ServiceLoader.Provider::type))
                .anyMatch(DecisionMetadataProvider.class::equals);
    }

    @Test
    public void apiShouldNotDependOnKafkaDebeziumOrOtherPackages() throws IOException {
        final var violations = new ArrayList<String>();
        for (final var entry : importsOf("api")) {
            if (entry.imported().startsWith("org.apache.kafka") || entry.imported().startsWith("io.debezium") && !entry.imported().startsWith(BASE + ".api")) {
                violations.add(entry.toString());
            }
        }
        assertThat(violations).isEmpty();
    }

    @Test
    public void spiShouldDependOnlyOnApi() throws IOException {
        assertThat(forbiddenImports("spi", BASE + ".DecisionToHeaders", BASE + ".systemone", BASE + ".jev", BASE + ".kev", BASE + ".metadata")).isEmpty();
    }

    @Test
    public void neutralPackagesShouldNotDependOnProviders() throws IOException {
        for (final var dir : List.of("", "metadata", "systemone")) {
            assertThat(forbiddenImports(dir, BASE + ".jev", BASE + ".kev")).isEmpty();
        }
        for (final var dir : List.of("", "metadata")) {
            assertThat(forbiddenImports(dir, BASE + ".systemone")).isEmpty();
        }
    }

    @Test
    public void providersShouldDependOnlyOnSystemOne() throws IOException {
        for (final var dir : List.of("jev", "kev")) {
            assertThat(importsOf(dir)).allMatch(entry -> entry.imported().startsWith(BASE + ".systemone."));
        }
    }

    private static List<String> forbiddenImports(final String directory, final String... forbiddenPrefixes) throws IOException {
        final var violations = new ArrayList<String>();
        for (final var entry : importsOf(directory)) {
            for (final var prefix : forbiddenPrefixes) {
                if (entry.imported().equals(prefix) || entry.imported().startsWith(prefix + ".")) {
                    violations.add(entry.toString());
                }
            }
        }
        return violations;
    }

    private static List<Import> importsOf(final String directory) throws IOException {
        final var root = directory.isEmpty() ? SOURCES : SOURCES.resolve(directory);
        assertThat(root).isDirectory();
        final var result = new ArrayList<Import>();
        try (var files = Files.list(root)) {
            for (final var file : files.filter(f -> f.getFileName().toString().endsWith(".java")).toList()) {
                final var matcher = IMPORT.matcher(Files.readString(file));
                while (matcher.find()) {
                    result.add(new Import(file.getFileName().toString(), matcher.group(1)));
                }
            }
        }
        assertThat(result).as("imports found in %s", root).isNotEmpty();
        return result;
    }

    private record Import(String file, String imported) {
        @Override
        public String toString() {
            return file + " imports " + imported;
        }
    }
}
