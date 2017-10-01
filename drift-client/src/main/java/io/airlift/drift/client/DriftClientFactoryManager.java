/*
 * Copyright (C) 2013 Facebook, Inc.
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

import io.airlift.drift.client.address.AddressSelector;
import io.airlift.drift.client.stats.MethodInvocationStatsFactory;
import io.airlift.drift.client.stats.NullMethodInvocationStatsFactory;
import io.airlift.drift.codec.ThriftCodecManager;
import io.airlift.drift.transport.MethodInvokerFactory;

import static java.util.Objects.requireNonNull;

public class DriftClientFactoryManager<I>
{
    private final ThriftCodecManager codecManager;
    private final MethodInvokerFactory<I> methodInvokerFactory;
    private final MethodInvocationStatsFactory methodInvocationStatsFactory;

    public DriftClientFactoryManager(ThriftCodecManager codecManager, MethodInvokerFactory<I> methodInvokerFactory)
    {
        this(codecManager, methodInvokerFactory, new NullMethodInvocationStatsFactory());
    }

    public DriftClientFactoryManager(ThriftCodecManager codecManager,
            MethodInvokerFactory<I> methodInvokerFactory,
            MethodInvocationStatsFactory methodInvocationStatsFactory)
    {
        this.codecManager = requireNonNull(codecManager, "codecManager is null");
        this.methodInvokerFactory = requireNonNull(methodInvokerFactory, "methodInvokerFactory is null");
        this.methodInvocationStatsFactory = methodInvocationStatsFactory;
    }

    public DriftClientFactory createDriftClientFactory(I clientIdentity, AddressSelector addressSelector)
    {
        return new DriftClientFactory(codecManager, () -> methodInvokerFactory.createMethodInvoker(clientIdentity), addressSelector, methodInvocationStatsFactory);
    }
}
