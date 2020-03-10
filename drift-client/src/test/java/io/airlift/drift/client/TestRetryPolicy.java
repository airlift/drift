/*
 * Copyright (C) 2018 Facebook, Inc.
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
package io.airlift.drift.client;

import io.airlift.drift.transport.client.DriftApplicationException;
import io.airlift.drift.transport.client.DriftClientConfig;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.airlift.drift.client.ExceptionClassification.HostStatus.NORMAL;
import static io.airlift.drift.client.ExceptionClassification.HostStatus.OVERLOADED;
import static io.airlift.drift.client.ExceptionClassification.NORMAL_EXCEPTION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

public class TestRetryPolicy
{
    @Test
    public void testRetryUserException()
    {
        ExceptionClassification overloaded = new ExceptionClassification(Optional.of(true), OVERLOADED);
        RetryPolicy policy = new RetryPolicy(new DriftClientConfig(), classifier -> {
            if (classifier instanceof TestingUserException) {
                return overloaded;
            }
            return NORMAL_EXCEPTION;
        });
        assertSame(policy.classifyException(new DriftApplicationException(new TestingUserException(), Optional.empty()), true), overloaded);
        assertSame(policy.classifyException(new TestingUserException(), true), overloaded);
    }

    @Test
    public void testProvidedResult()
    {
        // classifier has precedence over provided result
        assertEquals(classify(Optional.of(true), Optional.of(false)), Optional.of(true));
        assertEquals(classify(Optional.of(false), Optional.of(true)), Optional.of(false));

        // both set to same
        assertEquals(classify(Optional.of(true), Optional.of(true)), Optional.of(true));
        assertEquals(classify(Optional.of(false), Optional.of(false)), Optional.of(false));

        // only one set
        assertEquals(classify(Optional.empty(), Optional.of(true)), Optional.of(true));
        assertEquals(classify(Optional.empty(), Optional.of(false)), Optional.of(false));
        assertEquals(classify(Optional.of(true), Optional.empty()), Optional.of(true));
        assertEquals(classify(Optional.of(false), Optional.empty()), Optional.of(false));

        // neither set
        assertEquals(classify(Optional.empty(), Optional.empty()), Optional.empty());
    }

    private static Optional<Boolean> classify(Optional<Boolean> classifierResult, Optional<Boolean> providedResult)
    {
        return new RetryPolicy(new DriftClientConfig(), classifier -> new ExceptionClassification(classifierResult, NORMAL))
                .classifyException(new DriftApplicationException(new TestingUserException(), providedResult), true).isRetry();
    }

    private static class TestingUserException
            extends Exception
    {}
}
