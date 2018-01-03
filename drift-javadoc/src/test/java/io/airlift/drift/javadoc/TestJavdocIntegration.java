/*
 * Copyright (C) 2017 Facebook, Inc.
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
package io.airlift.drift.javadoc;

import io.takari.maven.testing.TestResources;
import io.takari.maven.testing.executor.MavenRuntime;
import io.takari.maven.testing.executor.MavenRuntime.MavenRuntimeBuilder;
import io.takari.maven.testing.executor.MavenVersions;
import io.takari.maven.testing.executor.junit.MavenJUnitTestRunner;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import static com.google.common.io.Resources.asCharSource;
import static com.google.common.io.Resources.getResource;
import static io.airlift.drift.javadoc.ThriftAnnotations.META_SUFFIX;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MavenJUnitTestRunner.class)
@MavenVersions({"3.2.3", "3.3.9", "3.5.0"})
@SuppressWarnings("JUnitTestNG")
public class TestJavdocIntegration
{
    @Rule
    public final TestResources resources = new TestResources();

    private final MavenRuntime maven;

    public TestJavdocIntegration(MavenRuntimeBuilder mavenBuilder)
            throws Exception
    {
        this.maven = mavenBuilder.withCliOptions("-B", "-U").build();
    }

    @Test
    public void testBasic()
            throws Exception
    {
        File basedir = resources.getBasedir("basic");
        maven.forProject(basedir)
                .execute("compile")
                .assertErrorFreeLog();

        assertGenerated(basedir, "Fruit");
        assertGenerated(basedir, "Point");
        assertGenerated(basedir, "Response");
        assertGenerated(basedir, "SimpleLogger");
    }

    private static void assertGenerated(File basedir, String name)
            throws IOException
    {
        URL expected = getResource(format("basic/%s.txt", name));

        name += META_SUFFIX;
        assertThat(new File(basedir, format("target/classes/its/%s.class", name))).isFile();
        assertThat(new File(basedir, format("target/generated-sources/annotations/its/%s.java", name)))
                .usingCharset(UTF_8).hasContent(asCharSource(expected, UTF_8).read());
    }
}
