/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schemagenerator.maven;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.maven.artifact.Artifact;
import org.apache.maven.execution.MavenSession;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Component;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.plugins.annotations.ResolutionScope;
import org.apache.maven.project.MavenProject;
import org.eclipse.aether.RepositorySystem;
import org.eclipse.aether.RepositorySystemSession;
import org.eclipse.aether.artifact.DefaultArtifact;
import org.eclipse.aether.collection.CollectRequest;
import org.eclipse.aether.collection.CollectResult;
import org.eclipse.aether.collection.DependencyCollectionException;
import org.eclipse.aether.graph.DependencyNode;
import org.eclipse.aether.repository.RemoteRepository;
import org.eclipse.aether.resolution.ArtifactRequest;
import org.eclipse.aether.resolution.ArtifactResolutionException;
import org.eclipse.aether.util.graph.visitor.PreorderNodeListGenerator;

import io.debezium.schemagenerator.SchemaGenerator;

/**
 * Generates the API spec for the connector(s) in a project.
 */
@Mojo(name = "generate-api-spec", defaultPhase = LifecyclePhase.PREPARE_PACKAGE, requiresDependencyResolution = ResolutionScope.COMPILE_PLUS_RUNTIME)
public class SchemaGeneratorMojo extends AbstractMojo {

    @Parameter(defaultValue = "openapi", property = "schema.format")
    private String format;

    @Parameter(defaultValue = "${project.build.directory}${file.separator}generated-sources", required = true)
    private File outputDirectory;

    @Parameter(defaultValue = "false")
    private boolean groupDirectoryPerConnector;

    @Parameter(defaultValue = "")
    private String filenamePrefix = "";

    @Parameter(defaultValue = "")
    private String filenameSuffix = "";

    /**
     * Gives access to the Maven project information.
     */
    @Parameter(defaultValue = "${project}", required = true, readonly = true)
    private MavenProject project;

    @Parameter(defaultValue = "${session}")
    private MavenSession session;

    @Component
    private RepositorySystem repoSystem;

    @Parameter(defaultValue = "${repositorySystemSession}", readonly = true, required = true)
    private RepositorySystemSession repoSession;

    @Parameter(defaultValue = "${project.remoteProjectRepositories}", readonly = true, required = true)
    private List<RemoteRepository> remoteRepos;

    public void execute() throws MojoExecutionException, MojoFailureException {
        String classPath = getClassPath();

        try {
            int result = exec(SchemaGenerator.class.getName(), classPath, Collections.emptyList(),
                    Arrays.<String> asList(format, outputDirectory.getAbsolutePath(), String.valueOf(groupDirectoryPerConnector),
                            quoteIfNecessary(filenamePrefix), quoteIfNecessary(filenameSuffix)));

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
        getLog().debug("Executing SchemaGenerator with classpath: " + classPath);
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

    private String getClassPath() throws MojoExecutionException {
        Set<Artifact> dependencyArtifacts = project.getArtifacts();
        Set<org.eclipse.aether.artifact.Artifact> pluginDependencyArtifacts = getDependencies(getGeneratorPluginArtifact());

        // all relevant dependencies of the target project
        String classPath = dependencyArtifacts.stream()
                .filter(a -> a.getScope().equals(Artifact.SCOPE_COMPILE) || a.getScope().equals(Artifact.SCOPE_PROVIDED))
                .map(a -> a.getFile().getAbsolutePath())
                .collect(Collectors.joining(File.pathSeparator));

        // all generator plug-in dependencies
        classPath += File.pathSeparator + pluginDependencyArtifacts.stream()
                .map(a -> a.getFile().getAbsolutePath())
                .collect(Collectors.joining(File.pathSeparator));

        // this plug-in
        classPath += File.pathSeparator + classPathEntryFor(SchemaGenerator.class);

        // the target project's own artifact
        classPath += File.pathSeparator + project.getArtifact().getFile().getAbsolutePath();

        return classPath;
    }

    /**
     * Returns the schema generator plug-in artifact
     */
    private Artifact getGeneratorPluginArtifact() {
        Set<Artifact> pluginArtifacts = project.getPluginArtifacts();

        return pluginArtifacts.stream()
                .filter(a -> a.getArtifactId().equals("debezium-schema-generator"))
                .findFirst()
                .get();
    }

    private String classPathEntryFor(Class<?> clazz) {
        try {
            return new File(clazz.getProtectionDomain().getCodeSource().getLocation().toURI()).getAbsolutePath();
        }
        catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private Set<org.eclipse.aether.artifact.Artifact> getDependencies(Artifact inputArtifact) throws MojoExecutionException {
        try {
            org.eclipse.aether.artifact.Artifact artifact = new DefaultArtifact(inputArtifact.getGroupId(), inputArtifact.getArtifactId(), null,
                    inputArtifact.getVersion());
            CollectRequest collectRequest = new CollectRequest(new org.eclipse.aether.graph.Dependency(artifact, "compile"), remoteRepos);
            CollectResult collectResult = repoSystem.collectDependencies(repoSession, collectRequest);

            PreorderNodeListGenerator nlg = new PreorderNodeListGenerator();
            collectResult.getRoot().accept(nlg);

            List<DependencyNode> dependencies = nlg.getNodes();

            // remove the input artifact itself as it can't be resolved when being part of the current reactor build
            Iterator<DependencyNode> it = dependencies.iterator();
            while (it.hasNext()) {
                DependencyNode next = it.next();
                if (next.getDependency() == collectRequest.getRoot()) {
                    it.remove();
                }
            }

            return dependencies.stream()
                    .filter(d -> d.getDependency() != collectRequest.getRoot())
                    .map(d -> resolveArtifact(d.getArtifact()))
                    .collect(Collectors.toSet());
        }
        catch (DependencyCollectionException e) {
            throw new MojoExecutionException("Couldn't collect dependencies of artifact " + inputArtifact, e);
        }
    }

    private org.eclipse.aether.artifact.Artifact resolveArtifact(org.eclipse.aether.artifact.Artifact inputArtifact) {
        ArtifactRequest request = new ArtifactRequest();
        request.setArtifact(inputArtifact);
        request.setRepositories(remoteRepos);

        try {
            return repoSystem.resolveArtifact(repoSession, request).getArtifact();
        }
        catch (ArtifactResolutionException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private static boolean isWindows() {
        final String operatingSystemName = System.getProperty("os.name");
        return operatingSystemName != null && operatingSystemName.startsWith("Windows");
    }

    private static String quoteIfNecessary(String value) {
        if ((value == null || value.length() == 0) && isWindows()) {
            // On Windows, if we do not explicitly double-quote an empty/null argument, it will be omitted by
            // the operating system as an argument and the expected number of arguments in the generator will
            // be out of sync from what it expects.
            return "\"\"";
        }
        return value;
    }
}
