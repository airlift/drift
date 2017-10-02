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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.drift.client.address.AddressSelector;
import io.airlift.drift.client.stats.MethodInvocationStat;
import io.airlift.drift.client.stats.MethodInvocationStatsFactory;
import io.airlift.drift.client.stats.NullMethodInvocationStat;
import io.airlift.drift.client.stats.NullMethodInvocationStatsFactory;
import io.airlift.drift.codec.ThriftCodec;
import io.airlift.drift.codec.ThriftCodecManager;
import io.airlift.drift.codec.metadata.ThriftMethodMetadata;
import io.airlift.drift.codec.metadata.ThriftServiceMetadata;
import io.airlift.drift.codec.metadata.ThriftType;
import io.airlift.drift.transport.DriftClientConfig;
import io.airlift.drift.transport.MethodInvoker;
import io.airlift.drift.transport.MethodInvokerFactory;
import io.airlift.drift.transport.MethodMetadata;
import io.airlift.drift.transport.ParameterMetadata;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

import static com.google.common.collect.Maps.transformEntries;
import static com.google.common.reflect.Reflection.newProxy;
import static io.airlift.drift.client.FilteredMethodInvoker.createFilteredMethodInvoker;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class DriftClientFactory
{
    private final ThriftCodecManager codecManager;
    private final Supplier<MethodInvoker> methodInvokerSupplier;
    private final AddressSelector addressSelector;
    private final ExceptionClassifier exceptionClassifier;
    private final ConcurrentMap<Class<?>, ThriftServiceMetadata> serviceMetadataCache = new ConcurrentHashMap<>();
    private final MethodInvocationStatsFactory methodInvocationStatsFactory;

    public DriftClientFactory(
            ThriftCodecManager codecManager,
            Supplier<MethodInvoker> methodInvokerSupplier,
            AddressSelector addressSelector,
            ExceptionClassifier exceptionClassifier,
            MethodInvocationStatsFactory methodInvocationStatsFactory)
    {
        this.codecManager = requireNonNull(codecManager, "codecManager is null");
        this.methodInvokerSupplier = requireNonNull(methodInvokerSupplier, "methodInvokerSupplier is null");
        this.addressSelector = requireNonNull(addressSelector, "addressSelector is null");
        this.exceptionClassifier = exceptionClassifier;
        this.methodInvocationStatsFactory = requireNonNull(methodInvocationStatsFactory, "methodInvocationStatsFactory is null");
    }

    public DriftClientFactory(ThriftCodecManager codecManager, MethodInvokerFactory<?> invokerFactory, AddressSelector addressSelector, ExceptionClassifier exceptionClassifier)
    {
        this(
                codecManager,
                () -> invokerFactory.createMethodInvoker(null),
                addressSelector,
                exceptionClassifier,
                new NullMethodInvocationStatsFactory());
    }

    public <T> DriftClient<T> createDriftClient(Class<T> clientInterface)
    {
        return createDriftClient(clientInterface, Optional.empty(), ImmutableList.of(), new DriftClientConfig());
    }

    public <T> DriftClient<T> createDriftClient(
            Class<T> clientInterface,
            Optional<Class<? extends Annotation>> qualifierAnnotation,
            List<MethodInvocationFilter> filters,
            DriftClientConfig config)
    {
        ThriftServiceMetadata serviceMetadata = serviceMetadataCache.computeIfAbsent(
                clientInterface,
                clazz -> new ThriftServiceMetadata(clazz, codecManager.getCatalog()));

        MethodInvoker invoker = createFilteredMethodInvoker(filters, methodInvokerSupplier.get());

        Optional<String> qualifier = qualifierAnnotation.map(Class::getSimpleName);

        ImmutableMap.Builder<Method, DriftMethodHandler> builder = ImmutableMap.builder();
        for (ThriftMethodMetadata method : serviceMetadata.getMethods().values()) {
            MethodMetadata metadata = getMethodMetadata(method);

            RetryPolicy retryPolicy = new RetryPolicy(config, exceptionClassifier);

            MethodInvocationStat statHandler;
            if (config.isStatsEnabled()) {
                statHandler = methodInvocationStatsFactory.getStat(serviceMetadata, qualifier, metadata);
            }
            else {
                statHandler = new NullMethodInvocationStat();
            }

            DriftMethodHandler handler = new DriftMethodHandler(metadata, invoker, method.isAsync(), addressSelector, retryPolicy, statHandler);
            builder.put(method.getMethod(), handler);
        }
        Map<Method, DriftMethodHandler> methods = builder.build();

        return (context, headers) -> newProxy(clientInterface, new DriftInvocationHandler(serviceMetadata.getName(), methods, context, headers));
    }

    private MethodMetadata getMethodMetadata(ThriftMethodMetadata metadata)
    {
        List<ParameterMetadata> parameters = metadata.getParameters().stream()
                .map(parameter -> new ParameterMetadata(
                        parameter.getId(),
                        parameter.getName(),
                        getCodec(parameter.getThriftType())))
                .collect(toList());

        ThriftCodec<Object> resultCodec = getCodec(metadata.getReturnType());

        Map<Short, ThriftCodec<Object>> exceptionCodecs = ImmutableMap.copyOf(
                transformEntries(metadata.getExceptions(), (key, value) -> getCodec(value)));

        return new MethodMetadata(
                metadata.getName(),
                parameters,
                resultCodec,
                exceptionCodecs,
                metadata.getOneway());
    }

    @SuppressWarnings("unchecked")
    private ThriftCodec<Object> getCodec(ThriftType thriftType)
    {
        return (ThriftCodec<Object>) codecManager.getCodec(thriftType);
    }
}
