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
package io.airlift.drift.transport;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class InvokeRequest
{
    private final MethodMetadata method;
    private final Optional<String> addressSelectionContext;
    private final Map<String, String> headers;
    private final List<Object> parameters;

    public InvokeRequest(MethodMetadata method, Optional<String> addressSelectionContext, Map<String, String> headers, List<Object> parameters)
    {
        this.method = requireNonNull(method, "method is null");
        this.addressSelectionContext = requireNonNull(addressSelectionContext, "addressSelectionContext is null");
        this.headers = requireNonNull(headers, "headers is null");
        this.parameters = requireNonNull(parameters, "parameters is null");
    }

    public MethodMetadata getMethod()
    {
        return method;
    }

    public Optional<String> getAddressSelectionContext()
    {
        return addressSelectionContext;
    }

    public Map<String, String> getHeaders()
    {
        return headers;
    }

    public List<Object> getParameters()
    {
        return parameters;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .omitNullValues()
                .add("method", method)
                .add("addressSelectionContext", addressSelectionContext.orElse(null))
                .add("headers", headers.isEmpty() ? null : headers)
                .toString();
    }
}
