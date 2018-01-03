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
package io.airlift.drift.server;

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.drift.transport.server.ServerInvokeRequest;

import static org.testng.Assert.assertEquals;

public class ShortCircuitFilter
        implements MethodInvocationFilter, TestingInvocationTarget
{
    private final ResultsSupplier resultsSupplier;

    private ServerInvokeRequest request;

    public ShortCircuitFilter(ResultsSupplier resultsSupplier)
    {
        this.resultsSupplier = resultsSupplier;
    }

    @Override
    public ListenableFuture<Object> invoke(ServerInvokeRequest request, ServerMethodInvoker next)
    {
        this.request = request;
        return resultsSupplier.get();
    }

    @Override
    public void assertInvocation(String expectedMethodName, int expectedId, String expectedName)
    {
        assertEquals(request.getMethod().getName(), expectedMethodName);
        assertEquals(request.getParameters().get((short) 1), expectedId);
        assertEquals(request.getParameters().get((short) 2), expectedName);
    }
}
