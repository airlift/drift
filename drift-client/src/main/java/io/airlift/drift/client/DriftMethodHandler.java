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

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.drift.client.stats.MethodInvocationStat;
import io.airlift.drift.transport.InvokeRequest;
import io.airlift.drift.transport.MethodInvoker;
import io.airlift.drift.transport.MethodMetadata;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

class DriftMethodHandler
{
    private final MethodMetadata metadata;
    private final MethodInvoker invoker;
    private final boolean async;
    private final MethodInvocationStat stat;

    public DriftMethodHandler(MethodMetadata metadata, MethodInvoker invoker, boolean async, MethodInvocationStat stat)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.invoker = requireNonNull(invoker, "invoker is null");
        this.async = async;
        this.stat = requireNonNull(stat, "stat is null");
    }

    public boolean isAsync()
    {
        return async;
    }

    public ListenableFuture<Object> invoke(Optional<String> addressSelectionContext, Map<String, String> headers, List<Object> parameters)
    {
        long startTime = System.nanoTime();
        ListenableFuture<Object> result = invoker.invoke(new InvokeRequest(metadata, addressSelectionContext, headers, parameters));
        stat.recordResult(startTime, result);
        return result;
    }
}
