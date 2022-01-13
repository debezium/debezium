/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schemagenerator.maven;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.maven.artifact.Artifact;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;
import org.eclipse.microprofile.openapi.models.OpenAPI;
import org.jboss.jandex.DotName;

import com.fasterxml.jackson.annotation.JsonView;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Charsets;

import io.debezium.DebeziumException;
import io.debezium.schemagenerator.SchemaGenerator;
import io.smallrye.openapi.runtime.io.Format;

/**
 * Generates the API spec for the connector(s) in a project.
 */
@Mojo(name = "generate-api-spec", defaultPhase = LifecyclePhase.PREPARE_PACKAGE)
public class SchemaGeneratorMojo extends AbstractMojo {

    @Parameter(defaultValue = "openapi", property = "schema.format")
    private String format;

    @Parameter(defaultValue = "${project.build.directory}/generated-sources", required = true)
    private File outputDirectory;

    /**
     * Gives access to the Maven project information.
     */
    @Parameter(defaultValue = "${project}", required = true, readonly = true)
    private MavenProject project;

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        String classPath = getClassPath();

        try {
            int result = exec(SchemaGenerator.class.getName(), classPath, Collections.emptyList(), Arrays.<String> asList(format, outputDirectory.getAbsolutePath()));

            if (result != 0) {
                throw new MojoExecutionException("Couldn't generate API spec; please see the logs for more details");
            }
            getLog().info("Generated API spec at " + outputDirectory.getAbsolutePath());
        }
        catch (IOException | InterruptedException e) {
            throw new MojoExecutionException("Couldn't generate API spec", e);
        }
    }

    private int exec(String className, String classPath, List<String> jvmArgs, List<String> args) throws IOException, InterruptedException {
        String javaHome = System.getProperty("java.home");
        String javaBin = javaHome + File.separator + "bin" + File.separator + "java";

        List<String> command = new ArrayList<>();
        command.add(javaBin);
        command.addAll(jvmArgs);
        command.add("-cp");
        command.add(classPath);
        command.add(className);
        command.addAll(args);

        ProcessBuilder builder = new ProcessBuilder(command);
        Process process = builder.inheritIO().start();
        process.waitFor();

        return process.exitValue();
    }

    private String getClassPath() {
        Set<Artifact> artifacts = project.getDependencyArtifacts();

        String classPath = artifacts.stream()
                .filter(a -> a.getScope().equals(Artifact.SCOPE_COMPILE) || a.getScope().equals(Artifact.SCOPE_PROVIDED))
                .map(a -> a.getFile().getAbsolutePath())
                .collect(Collectors.joining(File.pathSeparator));

        classPath += classPathEntryFor(getClass());
        classPath += classPathEntryFor(OpenAPI.class);
        classPath += classPathEntryFor(ConfigDef.class);
        classPath += classPathEntryFor(Format.class);
        classPath += classPathEntryFor(DebeziumException.class);
        classPath += classPathEntryFor(JsonProcessingException.class);
        classPath += classPathEntryFor(YAMLFactory.class);
        classPath += classPathEntryFor(JsonNode.class);
        classPath += classPathEntryFor(JsonView.class);
        classPath += classPathEntryFor(DotName.class);
        classPath += classPathEntryFor(Charsets.class);

        classPath += File.pathSeparator + project.getArtifact().getFile().getAbsolutePath();

        return classPath;
    }

    private String classPathEntryFor(Class<?> clazz) {
        return File.pathSeparator + clazz.getProtectionDomain().getCodeSource().getLocation().toString();
    }
}
