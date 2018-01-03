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
package io.airlift.drift.client;

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.drift.transport.client.MethodInvokerFactory;

import java.util.function.Supplier;

public class MockMethodInvokerFactory<I>
        implements MethodInvokerFactory<I>
{
    private final MockMethodInvoker methodInvoker;
    private I clientIdentity;

    public MockMethodInvokerFactory(Supplier<ListenableFuture<Object>> resultsSupplier)
    {
        this.methodInvoker = new MockMethodInvoker(resultsSupplier);
    }

    public I getClientIdentity()
    {
        return clientIdentity;
    }

    public MockMethodInvoker getMethodInvoker()
    {
        return methodInvoker;
    }

    @Override
    public MockMethodInvoker createMethodInvoker(I clientIdentity)
    {
        this.clientIdentity = clientIdentity;
        return methodInvoker;
    }
}
