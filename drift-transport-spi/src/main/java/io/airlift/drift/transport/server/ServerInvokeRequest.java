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
package io.airlift.drift.transport.server;

import com.google.common.collect.ImmutableMap;
import io.airlift.drift.transport.MethodMetadata;

import java.util.Map;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class ServerInvokeRequest
{
    private final MethodMetadata method;
    private final Map<String, String> headers;
    private final Map<Short, Object> parameters;

    public ServerInvokeRequest(MethodMetadata method, Map<String, String> headers, Map<Short, Object> parameters)
    {
        this.method = requireNonNull(method, "method is null");
        this.headers = ImmutableMap.copyOf(requireNonNull(headers, "headers is null"));
        this.parameters = ImmutableMap.copyOf(requireNonNull(parameters, "parameters is null"));
    }

    public MethodMetadata getMethod()
    {
        return method;
    }

    public Map<String, String> getHeaders()
    {
        return headers;
    }

    public Map<Short, Object> getParameters()
    {
        return parameters;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this).omitNullValues()
                .add("method", method)
                .add("headers", headers.isEmpty() ? null : headers)
                .toString();
    }
}
