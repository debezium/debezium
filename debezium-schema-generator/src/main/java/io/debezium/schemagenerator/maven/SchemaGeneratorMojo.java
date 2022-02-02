/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schemagenerator.maven;

import java.io.File;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;

import io.debezium.schemagenerator.SchemaGenerator;

/**
 * Generates the API spec for the connector(s) in a project.
 */
@Mojo(name = "generate-api-spec", defaultPhase = LifecyclePhase.PREPARE_PACKAGE)
public class SchemaGeneratorMojo extends AbstractMojo {

    @Parameter(defaultValue = "openapi", property = "schema.format")
    private String format;

    @Parameter(defaultValue = "${project.build.directory}/generated-sources", required = true)
    private File outputDirectory;

    @Parameter(defaultValue = "false")
    private Boolean groupDirectoryPerConnector;

    @Parameter(defaultValue = "")
    private String filenamePrefix;

    @Parameter(defaultValue = "")
    private String filenameSuffix;

    /**
     * Gives access to the Maven project information.
     */
    @Parameter(defaultValue = "${project}", required = true, readonly = true)
    private MavenProject project;

    @Override
    public void execute() {
        new SchemaGenerator().run(format, outputDirectory.toPath(), groupDirectoryPerConnector, filenamePrefix, filenameSuffix);
        getLog().info("Generated API schema at " + outputDirectory.getAbsolutePath());
    }
}
