/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.mongodb.sharded.freemarker;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

import freemarker.template.Configuration;

public class FreemarkerConfiguration {
    private static Configuration configuration;

    public Configuration getFreemarkerConfiguration() {
        if (configuration == null) {
            configuration = new Configuration(Configuration.VERSION_2_3_32);
            try {
                Path templatePath = Paths.get(getClass().getResource("/database-resources/mongodb/sharded/command-templates").toURI());
                configuration.setDirectoryForTemplateLoading(templatePath.toFile());
            }
            catch (IOException | URISyntaxException e) {
                throw new RuntimeException(e);
            }
        }
        return configuration;
    }
}
