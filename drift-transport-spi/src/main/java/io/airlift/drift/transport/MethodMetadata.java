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
import io.airlift.drift.codec.ThriftCodec;

import java.util.List;
import java.util.Map;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class MethodMetadata
{
    private final String name;
    private final List<ParameterMetadata> parameters;
    private final ThriftCodec<Object> resultCodec;
    private final Map<Short, ThriftCodec<Object>> exceptionCodecs;
    private final boolean oneway;
    private final ResultsClassifier resultsClassifier;

    public MethodMetadata(
            String name,
            List<ParameterMetadata> parameters,
            ThriftCodec<Object> resultCodec,
            Map<Short, ThriftCodec<Object>> exceptionCodecs,
            boolean oneway,
            ResultsClassifier resultsClassifier)
    {
        this.name = requireNonNull(name, "name is null");
        this.parameters = ImmutableList.copyOf(requireNonNull(parameters, "parameters is null"));
        this.resultCodec = requireNonNull(resultCodec, "resultCodec is null");
        this.exceptionCodecs = ImmutableMap.copyOf(requireNonNull(exceptionCodecs, "exceptionCodecs is null"));
        this.oneway = oneway;
        this.resultsClassifier = requireNonNull(resultsClassifier, "resultsClassifier is null");
    }

    public String getName()
    {
        return name;
    }

    public List<ParameterMetadata> getParameters()
    {
        return parameters;
    }

    public ThriftCodec<Object> getResultCodec()
    {
        return resultCodec;
    }

    public Map<Short, ThriftCodec<Object>> getExceptionCodecs()
    {
        return exceptionCodecs;
    }

    public boolean isOneway()
    {
        return oneway;
    }

    public ResultsClassifier getResultsClassifier()
    {
        return resultsClassifier;
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
