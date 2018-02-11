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
package io.airlift.drift.transport;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import io.airlift.drift.codec.ThriftCodec;
import io.airlift.drift.codec.ThriftCodecManager;
import io.airlift.drift.codec.metadata.ThriftMethodMetadata;
import io.airlift.drift.codec.metadata.ThriftType;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Maps.transformEntries;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public final class MethodMetadata
{
    private final String name;
    private final List<ParameterMetadata> parameters;
    private final Map<Short, ParameterMetadata> parametersById;
    private final ThriftCodec<Object> resultCodec;
    private final Map<Short, ThriftCodec<Object>> exceptionCodecs;
    private final Map<Class<?>, Short> exceptionIdsByType;
    private final boolean oneway;

    public static MethodMetadata toMethodMetadata(ThriftCodecManager codecManager, ThriftMethodMetadata metadata)
    {
        List<ParameterMetadata> parameters = metadata.getParameters().stream()
                .map(parameter -> new ParameterMetadata(
                        parameter.getId(),
                        parameter.getName(),
                        getCodec(codecManager, parameter.getThriftType())))
                .collect(Collectors.toList());

        ThriftCodec<Object> resultCodec = getCodec(codecManager, metadata.getReturnType());

        Map<Short, ThriftCodec<Object>> exceptionCodecs = ImmutableMap.copyOf(
                transformEntries(metadata.getExceptions(), (key, value) -> getCodec(codecManager, value)));

        return new MethodMetadata(
                metadata.getName(),
                parameters,
                resultCodec,
                exceptionCodecs,
                metadata.getOneway());
    }

    @SuppressWarnings("unchecked")
    private static ThriftCodec<Object> getCodec(ThriftCodecManager codecManager, ThriftType thriftType)
    {
        return (ThriftCodec<Object>) codecManager.getCodec(thriftType);
    }

    public MethodMetadata(
            String name,
            List<ParameterMetadata> parameters,
            ThriftCodec<Object> resultCodec,
            Map<Short, ThriftCodec<Object>> exceptionCodecs,
            boolean oneway)
    {
        this.name = requireNonNull(name, "name is null");
        this.parameters = ImmutableList.copyOf(requireNonNull(parameters, "parameters is null"));
        this.parametersById = parameters.stream().collect(toImmutableMap(ParameterMetadata::getFieldId, identity()));
        this.resultCodec = requireNonNull(resultCodec, "resultCodec is null");
        this.exceptionCodecs = ImmutableMap.copyOf(requireNonNull(exceptionCodecs, "exceptionCodecs is null"));

        ImmutableMap.Builder<Class<?>, Short> exceptions = ImmutableMap.builder();
        for (Map.Entry<Short, ThriftCodec<Object>> entry : exceptionCodecs.entrySet()) {
            exceptions.put(TypeToken.of(entry.getValue().getType().getJavaType()).getRawType(), entry.getKey());
        }
        this.exceptionIdsByType = exceptions.build();

        this.oneway = oneway;
    }

    public String getName()
    {
        return name;
    }

    public List<ParameterMetadata> getParameters()
    {
        return parameters;
    }

    public ParameterMetadata getParameterByFieldId(short fieldId)
    {
        return parametersById.get(fieldId);
    }

    public ThriftCodec<Object> getResultCodec()
    {
        return resultCodec;
    }

    public Map<Short, ThriftCodec<Object>> getExceptionCodecs()
    {
        return exceptionCodecs;
    }

    public Optional<Short> getExceptionId(Class<? extends Throwable> exceptionType)
    {
        Short exceptionId = exceptionIdsByType.get(exceptionType);
        if (exceptionId != null) {
            return Optional.of(exceptionId);
        }

        for (Entry<Class<?>, Short> entry : exceptionIdsByType.entrySet()) {
            if (entry.getKey().isAssignableFrom(exceptionType)) {
                return Optional.of(entry.getValue());
            }
        }

        return Optional.empty();
    }

    public boolean isOneway()
    {
        return oneway;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("parameters", parameters)
                .toString();
    }
}
