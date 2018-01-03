/*
 * Copyright (C) 2013 Facebook, Inc.
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

import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.drift.client.address.AddressSelector;
import io.airlift.drift.client.stats.MethodInvocationStat;
import io.airlift.drift.codec.metadata.ThriftHeaderParameter;
import io.airlift.drift.transport.MethodMetadata;
import io.airlift.drift.transport.client.Address;
import io.airlift.drift.transport.client.MethodInvoker;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.drift.client.DriftMethodInvocation.createDriftMethodInvocation;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

class DriftMethodHandler
{
    private final MethodMetadata metadata;
    private final Map<Integer, ThriftHeaderParameter> headerParameters;
    private final MethodInvoker invoker;
    private final boolean async;
    private final AddressSelector<? extends Address> addressSelector;
    private final RetryPolicy retryPolicy;
    private final MethodInvocationStat stat;

    public DriftMethodHandler(
            MethodMetadata metadata,
            Set<ThriftHeaderParameter> headersParameters,
            MethodInvoker invoker,
            boolean async,
            AddressSelector<? extends Address> addressSelector,
            RetryPolicy retryPolicy,
            MethodInvocationStat stat)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.headerParameters = requireNonNull(headersParameters, "headersParameters is null").stream()
                .collect(toImmutableMap(ThriftHeaderParameter::getIndex, identity()));
        this.invoker = requireNonNull(invoker, "invoker is null");
        this.async = async;
        this.addressSelector = requireNonNull(addressSelector, "addressSelector is null");
        this.retryPolicy = retryPolicy;
        this.stat = requireNonNull(stat, "stat is null");
    }

    public boolean isAsync()
    {
        return async;
    }

    public ListenableFuture<Object> invoke(Optional<String> addressSelectionContext, Map<String, String> headers, List<Object> parameters)
    {
        if (!headerParameters.isEmpty()) {
            headers = new LinkedHashMap<>(headers);
            for (Entry<Integer, ThriftHeaderParameter> entry : headerParameters.entrySet()) {
                String headerValue = (String) parameters.get(entry.getKey());
                if (headerValue != null) {
                    headers.put(entry.getValue().getName(), headerValue);
                }
            }

            ImmutableList.Builder<Object> newParameters = ImmutableList.builder();
            for (int index = 0; index < parameters.size(); index++) {
                if (!headerParameters.containsKey(index)) {
                    newParameters.add(parameters.get(index));
                }
            }
            parameters = newParameters.build();
        }
        return createDriftMethodInvocation(invoker, metadata, headers, parameters, retryPolicy, addressSelector, addressSelectionContext, stat, Ticker.systemTicker());
    }
}
