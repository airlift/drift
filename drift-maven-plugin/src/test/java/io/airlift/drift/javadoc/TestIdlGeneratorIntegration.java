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

import com.google.common.io.Files;
import com.google.common.io.Resources;
import io.takari.maven.testing.TestResources;
import io.takari.maven.testing.executor.MavenRuntime;
import io.takari.maven.testing.executor.MavenRuntime.MavenRuntimeBuilder;
import io.takari.maven.testing.executor.MavenVersions;
import io.takari.maven.testing.executor.junit.MavenJUnitTestRunner;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;

import static com.google.common.io.Resources.getResource;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

@RunWith(MavenJUnitTestRunner.class)
@MavenVersions({"3.2.3", "3.3.9", "3.5.0", "3.5.2"})
@SuppressWarnings("JUnitTestNG")
public class TestIdlGeneratorIntegration
{
    @Rule
    public final TestResources resources = new TestResources();

    private final MavenRuntime maven;

    public TestIdlGeneratorIntegration(MavenRuntimeBuilder mavenBuilder)
            throws Exception
    {
        this.maven = mavenBuilder.withCliOptions("-B", "-U").build();
    }

    @Test
    public void testDirect()
            throws Exception
    {
        assertGenerated("direct");
    }

    @Test
    public void testRecursive()
            throws Exception
    {
        assertGenerated("recursive");
    }

    private void assertGenerated(String testName)
            throws Exception
    {
        File basedir = resources.getBasedir(testName);
        maven.forProject(basedir)
                .execute("verify")
                .assertErrorFreeLog();

        String expected = Resources.toString(getResource(format("expected/%s.txt", testName)), UTF_8);
        String actual = Files.toString(new File(basedir, "target/test.thrift"), UTF_8);
        assertEquals(expected, actual);
    }
}
