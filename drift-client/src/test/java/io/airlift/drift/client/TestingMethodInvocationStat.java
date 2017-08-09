/*
 * Copyright (C) 2012 Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.airlift.drift.client;

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.drift.client.stats.MethodInvocationStat;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.testing.Assertions.assertGreaterThan;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

public class TestingMethodInvocationStat
        implements MethodInvocationStat
{
    private final AtomicInteger invocations = new AtomicInteger();
    private final AtomicInteger successes = new AtomicInteger();
    private final AtomicInteger failures = new AtomicInteger();
    private final AtomicLong lastStartTime = new AtomicLong();

    public void clear()
    {
        invocations.set(0);
        successes.set(0);
        failures.set(0);
        lastStartTime.set(0);
    }

    public void assertSuccess()
    {
        assertGreaterThan(invocations.get(), 0);
        assertGreaterThan(successes.get(), 0);
        assertEquals(failures.get(), 0);
        assertNotEquals(lastStartTime.get(), 0);
    }

    public void assertFailure()
    {
        assertGreaterThan(invocations.get(), 0);
        assertEquals(successes.get(), 0);
        assertGreaterThan(failures.get(), 0);
        assertNotEquals(lastStartTime.get(), 0);
    }

    @Override
    public void recordResult(long startTime, ListenableFuture<Object> result)
    {
        invocations.incrementAndGet();
        result.addListener(
                () -> {
                    lastStartTime.set(startTime);
                    try {
                        result.get();
                        successes.incrementAndGet();
                    }
                    catch (Throwable throwable) {
                        failures.incrementAndGet();
                    }
                },
                directExecutor());
    }
}
