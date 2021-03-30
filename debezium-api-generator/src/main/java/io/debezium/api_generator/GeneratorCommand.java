/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.api_generator;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.ServiceLoader;
import java.util.Set;

import org.eclipse.microprofile.openapi.models.media.Schema;

import io.debezium.api_generator.formats.ApiFormat;
import io.debezium.api_generator.formats.OpenApiFormat;
import io.debezium.connector.mongodb.MongoDbConnectorMetadata;
import io.debezium.connector.mysql.MySqlConnectorMetadata;
import io.debezium.connector.postgresql.PostgresConnectorMetadata;
import io.debezium.metadata.AbstractConnectorMetadata;

import picocli.CommandLine;

@CommandLine.Command
public class GeneratorCommand implements Runnable {

    @CommandLine.Option(names = { "-c", "--connector" }, description = "The connector type to generate.", defaultValue = "all")
    String connector;

    @CommandLine.Option(names = { "-d", "--plugin-dir" }, description = "The plugin directory providing jars in other formats.", defaultValue = ".")
    String pluginDir;

    @CommandLine.Option(names = { "-f", "--format" }, description = "The desired output format.")
    String outputFormat;

    @CommandLine.Option(names = { "-p", "--include-properties-file" }, description = "A file that specifies which properties to include.")
    String propertyIncludeFile;

    // CHECKSTYLE IGNORE RegexpSinglelineJava FOR NEXT 2 LINES
    private static final PrintStream outStream = System.out;
    private static final PrintStream errorStream = System.err;

    private Map<String, Set<String>> buildPropertyIncludeLists() {
        final Map<String, Set<String>> propertyIncludeLists = new HashMap<>();
        propertyIncludeLists.put("postgres", new HashSet<>());
        propertyIncludeLists.put("mysql", new HashSet<>());
        propertyIncludeLists.put("mongodb", new HashSet<>());

        if (null != propertyIncludeFile) {
            final File file = new File(propertyIncludeFile);
            final Scanner fileScanner;
            try {
                fileScanner = new Scanner(file);
            }
            catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }

            while (fileScanner.hasNext()) {
                final String line = fileScanner.next();
                int cnt = 0;
                for (String type : propertyIncludeLists.keySet()) {
                    if (line.startsWith(type + ".")) {
                        propertyIncludeLists.get(type).add(line.substring(type.length() + 1));
                        ++cnt;
                    }
                }
                if (0 == cnt) {
                    for (String type : propertyIncludeLists.keySet()) {
                        propertyIncludeLists.get(type).add(line);
                    }
                }
            }
            fileScanner.close();
        }
        return propertyIncludeLists;
    }

    private Map<String, ApiFormat> allApiFormats() {
        File pluginDirectory = new File(pluginDir);
        File[] jarFiles = pluginDirectory.listFiles((dir, name) -> name.toLowerCase().endsWith(".jar"));
        if (null == jarFiles) {
            return Collections.emptyMap();
        }
        errorStream.println("Loading ApiFormats from plugin directory: " + pluginDirectory.getAbsolutePath());
        List<File> jars = Arrays.asList(jarFiles);
        URL[] urls = new URL[jars.size()];
        for (int i = 0; i < jars.size(); i++) {
            try {
                urls[i] = jars.get(i).toURI().toURL();
            }
            catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
        URLClassLoader childClassLoader = new URLClassLoader(urls, ApiFormat.class.getClassLoader());
        final Map<String, ApiFormat> apiFormats = new HashMap<>();
        ServiceLoader.load(ApiFormat.class, childClassLoader)
                .forEach(apiFormat -> apiFormats.put(apiFormat.getDescriptor().getId(), apiFormat));
        ServiceLoader.load(ApiFormat.class)
                .forEach(apiFormat -> apiFormats.put(apiFormat.getDescriptor().getId(), apiFormat));
        errorStream.println("Available formats: " + String.join(", ", apiFormats.keySet()));
        return apiFormats;
    }

    @Override
    public void run() {
        final Map<String, ApiFormat> apiFormats = allApiFormats();
        final Map<String, Set<String>> propertyIncludeLists = buildPropertyIncludeLists();

        final Map<String, AbstractConnectorMetadata> connectors = new HashMap<>();
        if (connector.equals("postgres") || connector.equals("all")) {
            connectors.put("postgres", new PostgresConnectorMetadata());
        }
        if (connector.equals("mysql") || connector.equals("all")) {
            connectors.put("mysql", new MySqlConnectorMetadata());
        }
        if (connector.equals("mongodb") || connector.equals("all")) {
            connectors.put("mongodb", new MongoDbConnectorMetadata());
        }

        final ApiFormat apiFormat;
        if (null != outputFormat && apiFormats.containsKey(outputFormat)) {
            apiFormat = apiFormats.get(outputFormat);
        }
        else {
            errorStream.println("Specified output format \"" + outputFormat
                    + "\" not registered. Falling back to default. [Available formats: "
                    + String.join(", ", apiFormats.keySet()) + "]");
            apiFormat = new OpenApiFormat();
        }

        errorStream.println("Generating API Spec Format: "
                + apiFormat.getDescriptor().getName()
                + " " + apiFormat.getDescriptor().getVersion());

        errorStream.println("Generating connectors: " + String.join(", ", connectors.keySet()));

        final List<String> errors = new ArrayList<>();
        final List<Schema> connectorSchemas = new ArrayList<>();
        connectors.forEach((connectorBaseName, connectorMetadata) -> {
            JsonSchemaCreatorService jsonSchemaCreatorService = new JsonSchemaCreatorService(connectorBaseName, connectorMetadata,
                    propertyIncludeLists.get(connectorBaseName));
            connectorSchemas.add(jsonSchemaCreatorService.buildConnectorSchema());
            errors.addAll(jsonSchemaCreatorService.getErrors());
        });

        outStream.println(apiFormat.getSpec(connectorSchemas));

        if (!errors.isEmpty()) {
            errorStream.println("Warnings and errors during parsing:");
            errors.forEach(System.err::println);
        }
        else {
            errorStream.println("No errors during parsing. \uD83D\uDE0A");
        }
    }
}
