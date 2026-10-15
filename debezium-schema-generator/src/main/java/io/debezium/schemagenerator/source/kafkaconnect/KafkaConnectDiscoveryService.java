/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schemagenerator.source.kafkaconnect;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.jar.JarFile;
import java.util.stream.Collectors;

import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.CompositeIndex;
import org.jboss.jandex.DotName;
import org.jboss.jandex.Index;
import org.jboss.jandex.IndexReader;
import org.jboss.jandex.IndexView;
import org.jboss.jandex.Indexer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Discovers Kafka Connect component classes using Jandex bytecode indexing.
 *
 * <p>This service scans Kafka Connect JARs on the classpath and builds a Jandex index
 * to find all implementations of the ConnectPlugin interface (introduced in KC 4.3, KIP-1273).
 * It filters results to:
 * <ul>
 *   <li>Only include {@code org.apache.kafka.*} classes</li>
 *   <li>Exclude abstract classes (cannot be instantiated)</li>
 *   <li>Only keep the component kinds listed in {@link #COMPONENT_INTERFACES}</li>
 * </ul>
 *
 * <p><b>Discovery Process:</b>
 * <ol>
 *   <li>Find KC JARs by scanning for org.apache.kafka.connect packages</li>
 *   <li>Create Jandex index for each JAR (or use pre-built index if available)</li>
 *   <li>Combine the per-JAR indexes into a single composite view</li>
 *   <li>Query the composite view for all ConnectPlugin implementations</li>
 *   <li>Filter to concrete, instantiable classes only</li>
 *   <li>Keep those assignable to one of the supported component interfaces</li>
 * </ol>
 */
public class KafkaConnectDiscoveryService {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConnectDiscoveryService.class);

    /**
     * The Kafka Connect component kinds this generator produces descriptors for.
     *
     * <p>{@code ConnectPlugin} also covers connectors, config providers and policies, so
     * discovered plugins are narrowed down to these interfaces. The component type recorded in
     * the descriptor is resolved separately by {@code ComponentDescriptor} from the class itself.
     */
    private static final Set<String> COMPONENT_INTERFACES = Set.of(
            "org.apache.kafka.connect.transforms.Transformation",
            "org.apache.kafka.connect.transforms.predicates.Predicate",
            "org.apache.kafka.connect.storage.Converter",
            "org.apache.kafka.connect.storage.HeaderConverter");

    /**
     * Kafka Connect 4.3+ unified plugin interface (KIP-1273).
     */
    private static final String CONNECT_PLUGIN_INTERFACE = "org.apache.kafka.connect.components.ConnectPlugin";

    public static final String JAR = "jar:";
    public static final String JAR_FILE = JAR + "file:";
    public static final String ORG_APACHE_KAFKA = "org.apache.kafka.";
    public static final String CLASS_EXTENSION = ".class";

    /**
     * Discovers all supported Kafka Connect components on the classpath.
     *
     * @return list of discovered component classes, never null
     */
    public List<Class<?>> discoverKafkaConnectComponents() {

        Map<String, IndexView> jarIndices = indexKafkaConnectJars();

        if (jarIndices.isEmpty()) {
            LOGGER.warn("No Kafka Connect JARs found. Ensure connect-* JARs are on classpath.");
            return List.of();
        }

        LOGGER.debug("Indexed {} Kafka Connect JAR(s): {}",
                jarIndices.size(), String.join(", ", jarIndices.keySet()));

        // The per-JAR indexes must be combined into a single view before querying. A component such as
        // Cast$Key (connect-transforms) reaches ConnectPlugin only through Transformation (connect-api),
        // so the hierarchy spans JARs and cannot be resolved against an individual JAR's index.
        IndexView connectIndex = CompositeIndex.create(jarIndices.values());

        List<ClassInfo> connectPlugins = findConnectPlugins(connectIndex);

        if (connectPlugins.isEmpty()) {
            LOGGER.warn("No {} implementations found; Kafka Connect 4.3+ (KIP-1273) is required.", CONNECT_PLUGIN_INTERFACE);
            return List.of();
        }

        LOGGER.debug("Found {} concrete Kafka Connect plugin(s)", connectPlugins.size());

        List<Class<?>> components = selectSupportedComponents(connectPlugins);

        LOGGER.debug("Discovered {} Kafka Connect component(s)", components.size());

        return components;
    }

    /**
     * Finds all concrete, instantiable Kafka Connect plugins.
     *
     * <p>Since Kafka Connect 4.3 (KIP-1273) every configurable component implements the common
     * {@code ConnectPlugin} interface, so a single query replaces one query per component interface.
     * Each plugin is reported once, so the result needs no de-duplication.
     *
     * @param connectIndex combined index of all Kafka Connect JARs
     * @return the classes implementing {@code ConnectPlugin}
     */
    private List<ClassInfo> findConnectPlugins(IndexView connectIndex) {

        return connectIndex.getAllKnownImplementations(DotName.createSimple(CONNECT_PLUGIN_INTERFACE)).stream()
                .filter(classInfo -> classInfo.name().toString().startsWith(ORG_APACHE_KAFKA))
                .filter(classInfo -> !Modifier.isAbstract(classInfo.flags()))
                .toList();
    }

    /**
     * Keeps the plugins that are assignable to one of the {@link #COMPONENT_INTERFACES}.
     *
     * <p>The check is made on the loaded class rather than on the indexed metadata: a component
     * usually inherits its component interface, e.g. {@code Cast$Key} extends {@code Cast} which
     * implements {@code Transformation}, so the interfaces a class declares itself are not enough.
     *
     * @param connectPlugins the concrete {@code ConnectPlugin} implementations
     * @return the matching component classes, ordered by class name
     */
    private List<Class<?>> selectSupportedComponents(List<ClassInfo> connectPlugins) {

        List<Class<?>> componentInterfaces = COMPONENT_INTERFACES.stream()
                .map(this::loadClass)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toUnmodifiableList());

        return connectPlugins.stream()
                .map(classInfo -> classInfo.name().toString())
                .sorted()
                .map(this::loadClass)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .filter(pluginClass -> componentInterfaces.stream()
                        .anyMatch(componentInterface -> componentInterface.isAssignableFrom(pluginClass)))
                .collect(Collectors.toUnmodifiableList());
    }

    /**
     * Loads a class by name.
     *
     * @param className name of the class to load
     * @return the loaded class, or empty if it is not available on the classpath
     */
    private Optional<Class<?>> loadClass(String className) {

        try {
            return Optional.of(Class.forName(className));
        }
        // Unlike indexing, loading resolves the supertypes of a class and runs its static
        // initializer, so a missing transitive dependency surfaces as a LinkageError rather than
        // a ClassNotFoundException. Skip such a component instead of failing the whole generation.
        catch (ClassNotFoundException | LinkageError e) {
            LOGGER.warn("Could not load class: {}", className, e);
            return Optional.empty();
        }
    }

    /**
     * Finds and indexes all Kafka Connect JARs on the classpath.
     *
     * <p>JARs are located by scanning for classes in the org.apache.kafka.connect package.
     * This approach works with Kafka Connect 4.3+ which uses the unified ConnectPlugin interface.
     *
     * @return map of JAR filename to Jandex index
     */
    private Map<String, IndexView> indexKafkaConnectJars() {

        Map<String, IndexView> indices = new HashMap<>();

        try {
            // Scan for any class in org.apache.kafka.connect package to identify KC JARs
            String packagePath = "org/apache/kafka/connect";
            Enumeration<URL> resources = getClass().getClassLoader().getResources(packagePath);

            Collections.list(resources).stream()
                    .map(URL::toString)
                    .filter(urlString -> urlString.startsWith(JAR_FILE))
                    .map(this::extractJarPath)
                    .distinct()
                    .forEach(jarPath -> {
                        String jarName = Paths.get(jarPath).getFileName().toString();

                        if (!indices.containsKey(jarName)) {
                            LOGGER.debug("Indexing Kafka Connect JAR: {}", jarName);
                            indexJarFile(jarPath).ifPresent(index -> indices.put(jarName, index));
                        }
                    });
        }
        catch (IOException e) {
            LOGGER.warn("Error scanning for Kafka Connect packages", e);
        }

        return indices;
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
        final String fileUrl = urlString.substring(JAR.length(), urlString.indexOf("!"));
        return Paths.get(URI.create(fileUrl)).toString();
    }
}
