/*
 * Copyright (C) 2012 Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.airlift.drift.maven;

import io.airlift.drift.idl.generator.ThriftIdlGenerator;
import io.airlift.drift.idl.generator.ThriftIdlGeneratorConfig;
import io.airlift.drift.idl.generator.ThriftIdlGeneratorException;
import org.apache.maven.artifact.Artifact;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.plugins.annotations.ResolutionScope;
import org.apache.maven.project.MavenProject;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.createDirectories;

@Mojo(name = "generate-thrift-idl",
        defaultPhase = LifecyclePhase.PROCESS_CLASSES,
        requiresDependencyResolution = ResolutionScope.COMPILE_PLUS_RUNTIME)
public class IdlGeneratorMojo
        extends AbstractMojo
{
    @Parameter(defaultValue = "${project.build.outputDirectory}")
    private File classesDirectory;

    @Parameter(defaultValue = "${project}")
    private MavenProject project;

    /**
     * Drift classes to process.
     */
    @Parameter(required = true)
    private List<String> classes;

    /**
     * Package name to use for unqualified class names.
     */
    @Parameter
    private String defaultPackage;

    /**
     * Output file for the generated Thrift IDL.
     */
    @Parameter(required = true)
    private File outputFile;

    /**
     * Mapping of language scopes to language namespaces.
     * Example: java :: com.example.thrift
     */
    @Parameter
    private Map<String, String> namespaces;

    /**
     * Mapping of type names to include file paths. Any matching type will be
     * converted into an include file rather than being generated directly
     * into the IDL.
     */
    @Parameter
    private Map<String, String> includes;

    /**
     * Generate all types transitively reachable from the specified classes.
     * If this option is false, any dependent types not specified as class
     * names will need to be specified in the includes mapping.
     */
    @Parameter(required = true)
    private boolean recursive;

    /**
     * Do not log additional information while executing.
     */
    @Parameter
    private boolean quiet;

    @Override
    public void execute()
            throws MojoExecutionException
    {
        ClassLoader classLoader = createClassLoaderFromCompileTimeDependencies();

        ThriftIdlGeneratorConfig config = ThriftIdlGeneratorConfig.builder()
                .defaultPackage(defaultPackage)
                .namespaces(namespaces)
                .includes(includes)
                .recursive(recursive)
                .errorLogger(message -> getLog().error(message))
                .warningLogger(message -> getLog().warn(message))
                .verboseLogger(this::verbose)
                .build();

        String idl;
        try {
            idl = new ThriftIdlGenerator(config, classLoader).generate(classes);
        }
        catch (ThriftIdlGeneratorException e) {
            throw new MojoExecutionException("Failed to generate Thrift IDL: " + e.getMessage(), e);
        }

        if (outputFile.getParentFile() != null) {
            mkdirs(outputFile.getParentFile());
        }

        writeFile(outputFile, idl.getBytes(UTF_8));

        verbose("Wrote Thrift IDL to " + outputFile);
    }

    private void verbose(String message)
    {
        if (quiet) {
            getLog().debug(message);
        }
        else {
            getLog().info(message);
        }
    }

    private ClassLoader createClassLoaderFromCompileTimeDependencies()
            throws MojoExecutionException
    {
        List<URL> urls = new ArrayList<>();
        urls.add(fileToUrl(classesDirectory));
        for (Artifact artifact : project.getArtifacts()) {
            if (artifact.getFile() != null) {
                urls.add(fileToUrl(artifact.getFile()));
            }
        }
        return new URLClassLoader(urls.toArray(new URL[0]), Thread.currentThread().getContextClassLoader());
    }

    private static URL fileToUrl(File file)
            throws MojoExecutionException
    {
        try {
            return file.toURI().toURL();
        }
        catch (MalformedURLException e) {
            throw new MojoExecutionException("Failed to create URL for file: " + file, e);
        }
    }

    private static void writeFile(File file, byte[] bytes)
            throws MojoExecutionException
    {
        try {
            Files.write(file.toPath(), bytes);
        }
        catch (IOException e) {
            throw new MojoExecutionException("Failed to write file: " + file, e);
        }
    }

    private static void mkdirs(File file)
            throws MojoExecutionException
    {
        try {
            createDirectories(file.toPath());
        }
        catch (IOException e) {
            throw new MojoExecutionException("Failed to create directory: " + file, e);
        }
    }
}
