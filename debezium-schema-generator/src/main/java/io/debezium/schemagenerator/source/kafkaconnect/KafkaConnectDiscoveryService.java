/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schemagenerator.source.kafkaconnect;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.jar.JarFile;
import java.util.stream.Collectors;

import org.jboss.jandex.DotName;
import org.jboss.jandex.Index;
import org.jboss.jandex.IndexReader;
import org.jboss.jandex.Indexer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Discovers Kafka Connect component classes using Jandex bytecode indexing.
 *
 * <p>This service scans Kafka Connect JARs on the classpath and builds a Jandex index
 * to find all implementations of KC component interfaces. It filters results to:
 * <ul>
 *   <li>Only include {@code org.apache.kafka.*} classes</li>
 *   <li>Exclude abstract classes (cannot be instantiated)</li>
 * </ul>
 *
 * <p><b>Discovery Process:</b>
 * <ol>
 *   <li>Find KC JARs by looking for META-INF/services files</li>
 *   <li>Create Jandex index for each JAR (or use pre-built index if available)</li>
 *   <li>Query index for all implementations of each component interface</li>
 *   <li>Filter to concrete, instantiable classes only</li>
 * </ol>
 *
 * @see ComponentType
 */
public class KafkaConnectDiscoveryService {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConnectDiscoveryService.class);

    /**
     * Mapping of component types to their fully qualified interface names.
     */
    private static final Map<ComponentType, String> COMPONENT_INTERFACES = Map.of(
            ComponentType.TRANSFORMATION, "org.apache.kafka.connect.transforms.Transformation",
            ComponentType.CONVERTER, "org.apache.kafka.connect.storage.Converter",
            ComponentType.HEADER_CONVERTER, "org.apache.kafka.connect.storage.HeaderConverter",
            ComponentType.PREDICATE, "org.apache.kafka.connect.transforms.predicates.Predicate");
    public static final String META_INF_SERVICES_FORLDER = "META-INF/services/";
    public static final String JAR_FILE = "jar:file:";
    public static final String ORG_APACHE_KAFKA = "org.apache.kafka.";
    public static final String CLASS_EXTENSION = ".class";

    /**
     * Discovers all Kafka Connect components grouped by type.
     *
     * @return map of component type to list of discovered component classes, never null
     */
    public Map<ComponentType, List<Class<?>>> discoverKafkaConnectComponents() {

        Map<String, Index> jarIndices = indexKafkaConnectJars();

        if (jarIndices.isEmpty()) {
            LOGGER.warn("No Kafka Connect JARs found. Ensure connect-* JARs are on classpath.");
            return Map.of();
        }

        LOGGER.debug("Indexed {} Kafka Connect JAR(s): {}",
                jarIndices.size(), String.join(", ", jarIndices.keySet()));

        return COMPONENT_INTERFACES.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> {
                            List<Class<?>> components = discoverComponentType(jarIndices, entry.getValue());
                            LOGGER.debug("Discovered {} {} component(s)",
                                    components.size(), entry.getKey().getDisplayName());
                            return components;
                        },
                        (a, b) -> a,
                        () -> new EnumMap<>(ComponentType.class)));
    }

    /**
     * Finds and indexes all Kafka Connect JARs on the classpath.
     *
     * <p>JARs are located by searching for META-INF/services files for each
     * component interface. This ensures we only index relevant KC JARs.
     *
     * @return map of JAR filename to Jandex index
     */
    private Map<String, Index> indexKafkaConnectJars() {

        Map<String, Index> indices = new HashMap<>();

        COMPONENT_INTERFACES.values().forEach(interfaceName -> {
            String serviceFile = META_INF_SERVICES_FORLDER + interfaceName;

            try {
                Enumeration<URL> resources = getClass().getClassLoader().getResources(serviceFile);

                Collections.list(resources).stream()
                        .map(URL::toString)
                        .filter(urlString -> urlString.startsWith(JAR_FILE))
                        .map(this::extractJarPath)
                        .forEach(jarPath -> {
                            String jarName = Paths.get(jarPath).getFileName().toString();

                            if (!indices.containsKey(jarName)) {
                                LOGGER.debug("Indexing: {}", jarName);
                                indexJarFile(jarPath).ifPresent(index -> indices.put(jarName, index));
                            }
                        });
            }
            catch (IOException e) {
                LOGGER.warn("Error scanning for {}", interfaceName, e);
            }
        });

        return indices;
    }

    /**
     * Discovers all concrete implementations of a specific component interface.
     *
     * @param indices map of JAR indices to search
     * @param interfaceName fully qualified interface name
     * @return list of discovered component classes
     */
    private List<Class<?>> discoverComponentType(Map<String, Index> indices, String interfaceName) {

        // TODO with KC 4.3, KIP-1273 introduced a common ConnectPlugin interface that can be used to discover all configurable components
        DotName interfaceDotName = DotName.createSimple(interfaceName);

        return indices.values().stream()
                .map(index -> index.getAllKnownImplementors(interfaceDotName))
                .flatMap(implementations -> implementations.stream()
                        .filter(classInfo -> classInfo.name().toString().startsWith(ORG_APACHE_KAFKA))
                        .filter(classInfo -> !Modifier.isAbstract(classInfo.flags()))
                        .map(classInfo -> {
                            try {
                                return Optional.of(Class.forName(classInfo.name().toString()));
                            }
                            catch (ClassNotFoundException e) {
                                LOGGER.warn("Could not load class: {}", classInfo.name(), e);
                            }
                            return Optional.<Class<?>> empty();
                        }))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toUnmodifiableList());
    }

    /**
     * Creates a Jandex index for a JAR file.
     *
     * <p>Tries to use a pre-built index (META-INF/jandex.idx) if available,
     * otherwise creates a new index by scanning all .class files in the JAR.
     *
     * @param jarPath absolute path to the JAR file
     * @return Optional containing Jandex index, or empty if indexing fails
     */
    private Optional<Index> indexJarFile(String jarPath) {

        Optional<Index> prebuiltIndex = tryLoadPrebuiltIndex(jarPath);
        if (prebuiltIndex.isPresent()) {
            LOGGER.debug("Using pre-built index for {}", jarPath);
            return prebuiltIndex;
        }

        try {
            return Optional.of(createIndexFromJarClasses(jarPath));
        }
        catch (Exception e) {
            LOGGER.warn("Could not index JAR: {}", jarPath, e);
            return Optional.empty();
        }
    }

    /**
     * Attempts to load a pre-built Jandex index from META-INF/jandex.idx.
     *
     * @param jarPath path to the JAR file
     * @return Optional containing pre-built index, or empty if not found
     */
    private Optional<Index> tryLoadPrebuiltIndex(String jarPath) {

        try {
            Path path = Paths.get(jarPath);
            String jarUrl = JAR_FILE + path.toAbsolutePath() + "!/META-INF/jandex.idx";

            try (InputStream indexStream = new URL(jarUrl).openStream()) {
                IndexReader reader = new IndexReader(indexStream);
                return Optional.of(reader.read());
            }
        }
        catch (Exception e) {
            return Optional.empty();
        }
    }

    /**
     * Creates a new Jandex index by scanning all .class files in a JAR.
     *
     * @param jarPath path to the JAR file
     * @return newly created index
     * @throws Exception if indexing fails
     */
    private Index createIndexFromJarClasses(String jarPath) throws Exception {

        Indexer indexer = new Indexer();
        Path path = Paths.get(jarPath);

        try (JarFile jarFile = new JarFile(path.toFile())) {
            long classCount = Collections.list(jarFile.entries()).stream()
                    .filter(entry -> entry.getName().endsWith(CLASS_EXTENSION))
                    .mapToLong(entry -> {
                        try (InputStream classStream = jarFile.getInputStream(entry)) {
                            indexer.index(classStream);
                            return 1;
                        }
                        catch (Exception e) {
                            LOGGER.debug("Could not index class {}", entry.getName(), e);
                            return 0;
                        }
                    })
                    .sum();

            LOGGER.debug("Indexed {} classes from {}", classCount, path.getFileName());
        }

        return indexer.complete();
    }

    /**
     * Extracts the file system path from a JAR URL.
     *
     * @param urlString JAR URL in format "jar:file:/path/to/file.jar!/..."
     * @return file system path to the JAR
     */
    private String extractJarPath(String urlString) {
        return urlString.substring(
                JAR_FILE.length(),
                urlString.indexOf("!"));
    }
}
