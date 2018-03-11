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
package io.airlift.drift.integration;

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.drift.client.MethodInvocationFilter;
import io.airlift.drift.transport.client.InvokeRequest;
import io.airlift.drift.transport.client.MethodInvoker;

import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static org.testng.Assert.assertEquals;

class TestFilter
        implements MethodInvocationFilter
{
    private final AtomicInteger requestCount = new AtomicInteger();
    private final AtomicInteger replyCount = new AtomicInteger();

    @Override
    public ListenableFuture<Object> invoke(InvokeRequest request, MethodInvoker next)
    {
        requestCount.getAndIncrement();
        ListenableFuture<Object> result = next.invoke(request);
        result.addListener(replyCount::getAndIncrement, directExecutor());
        return result;
    }

    public void assertCounts(int invocationCount)
    {
        assertEquals(requestCount.get(), invocationCount, "requestCount");
        assertEquals(replyCount.get(), invocationCount, "replyCount");
    }
}
