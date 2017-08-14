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

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.drift.TApplicationException;
import io.airlift.drift.TApplicationException.Type;
import io.airlift.drift.codec.ThriftCodecManager;
import io.airlift.drift.codec.metadata.ThriftMethodMetadata;
import io.airlift.drift.codec.metadata.ThriftServiceMetadata;
import io.airlift.drift.server.stats.MethodInvocationStat;
import io.airlift.drift.server.stats.MethodInvocationStatsFactory;
import io.airlift.drift.transport.MethodMetadata;
import io.airlift.drift.transport.server.ServerInvokeRequest;
import io.airlift.drift.transport.server.ServerMethodInvoker;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.lang.String.format;

class DriftServerMethodInvoker
        implements ServerMethodInvoker
{
    private final Map<String, ServiceMethod> methods;
    private final Map<String, MethodInvocationStat> stats;

    public DriftServerMethodInvoker(
            ThriftCodecManager codecManager,
            Collection<DriftService> services,
            List<MethodInvocationFilter> filters,
            MethodInvocationStatsFactory methodInvocationStatsFactory)
    {
        Map<String, ServiceMethod> processorMap = new HashMap<>();
        ImmutableMap.Builder<String, MethodInvocationStat> stats = ImmutableMap.builder();
        for (DriftService service : services) {
            ThriftServiceMetadata serviceMetadata = new ThriftServiceMetadata(service.getService().getClass(), codecManager.getCatalog());
            for (ThriftMethodMetadata thriftMethodMetadata : serviceMetadata.getMethods().values()) {
                if (processorMap.containsKey(thriftMethodMetadata.getName())) {
                    throw new IllegalArgumentException(format("Multiple methods named '%s' are annotated with @ThriftMethod in the given services", thriftMethodMetadata.getName()));
                }
                ServiceMethod serviceMethod = new ServiceMethod(codecManager, service.getService(), thriftMethodMetadata, filters);
                processorMap.put(thriftMethodMetadata.getName(), serviceMethod);
                if (service.isStatsEnabled()) {
                    stats.put(thriftMethodMetadata.getName(), methodInvocationStatsFactory.getStat(serviceMetadata, service.getQualifier(), serviceMethod.getMethodMetadata()));
                }
            }
        }
        methods = ImmutableMap.copyOf(processorMap);
        this.stats = stats.build();
    }

    @Override
    public Optional<MethodMetadata> getMethodMetadata(String name)
    {
        ServiceMethod method = methods.get(name);
        if (method == null) {
            return Optional.empty();
        }
        return Optional.of(method.getMethodMetadata());
    }

    @Override
    public ListenableFuture<Object> invoke(ServerInvokeRequest request)
    {
        ServiceMethod method = methods.get(request.getMethod().getName());
        if (method == null) {
            return Futures.immediateFailedFuture(new TApplicationException(Type.UNKNOWN_METHOD, "Invalid method name: '" + request.getMethod().getName() + "'"));
        }

        return method.invokeMethod(request);
    }

    @Override
    public void recordResult(String methodName, long startTime, ListenableFuture<Object> result)
    {
        MethodInvocationStat stat = stats.get(methodName);
        if (stat == null) {
            return;
        }
        stat.recordResult(startTime, result);
    }
}
