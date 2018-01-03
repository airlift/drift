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

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.drift.codec.ThriftCodecManager;
import io.airlift.drift.codec.metadata.ThriftFieldMetadata;
import io.airlift.drift.codec.metadata.ThriftHeaderParameter;
import io.airlift.drift.codec.metadata.ThriftInjection;
import io.airlift.drift.codec.metadata.ThriftMethodMetadata;
import io.airlift.drift.codec.metadata.ThriftParameterInjection;
import io.airlift.drift.transport.MethodMetadata;
import io.airlift.drift.transport.server.ServerInvokeRequest;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.airlift.drift.server.FilteredMethodInvoker.createFilteredMethodInvoker;
import static io.airlift.drift.transport.MethodMetadata.toMethodMetadata;
import static java.util.Objects.requireNonNull;

class ServiceMethod
{
    private final Object service;
    private final MethodMetadata methodMetadata;
    private final ServerMethodInvoker invoker;

    public ServiceMethod(ThriftCodecManager codecManager, Object service, ThriftMethodMetadata methodMetadata, List<MethodInvocationFilter> filters)
    {
        requireNonNull(codecManager, "codecManager is null");
        requireNonNull(service, "service is null");
        requireNonNull(methodMetadata, "methodMetadata is null");

        this.service = service;
        this.methodMetadata = toMethodMetadata(codecManager, methodMetadata);
        invoker = createFilteredMethodInvoker(filters, new ServiceMethodInvoker(service, methodMetadata));
    }

    public MethodMetadata getMethodMetadata()
    {
        return methodMetadata;
    }

    public ListenableFuture<Object> invokeMethod(ServerInvokeRequest request)
    {
        return invoker.invoke(request);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("methodMetadata", methodMetadata)
                .add("service", service)
                .toString();
    }

    private static class ServiceMethodInvoker
            implements ServerMethodInvoker
    {
        private final Object service;
        private final Method method;
        private final ThriftHeaderParameter[] headerParameters;
        private final ThriftFieldMetadata[] normalParameters;

        public ServiceMethodInvoker(Object service, ThriftMethodMetadata methodMetadata)
        {
            this.service = requireNonNull(service, "service is null");
            this.method = requireNonNull(methodMetadata.getMethod(), "method is null");

            this.headerParameters = new ThriftHeaderParameter[method.getParameterCount()];
            for (ThriftHeaderParameter headerParameter : methodMetadata.getHeaderParameters()) {
                this.headerParameters[headerParameter.getIndex()] = headerParameter;
            }

            this.normalParameters = new ThriftFieldMetadata[method.getParameterCount()];
            for (ThriftFieldMetadata normalParameter : methodMetadata.getParameters()) {
                for (ThriftInjection thriftInjection : normalParameter.getInjections()) {
                    ThriftParameterInjection parameterInjection = (ThriftParameterInjection) thriftInjection;
                    this.normalParameters[parameterInjection.getParameterIndex()] = normalParameter;
                }
            }
        }

        @Override
        public ListenableFuture<Object> invoke(ServerInvokeRequest request)
        {
            Object[] parameters = new Object[method.getParameterCount()];

            for (int i = 0; i < headerParameters.length; i++) {
                ThriftHeaderParameter headerParameter = headerParameters[i];
                if (headerParameter != null) {
                    parameters[i] = request.getHeaders().get(headerParameter.getName());
                }
                ThriftFieldMetadata normalParameter = normalParameters[i];
                if (normalParameter != null) {
                    parameters[i] = request.getParameters().get(normalParameter.getId());
                }
            }

            try {
                Object response = method.invoke(service, parameters);
                if (response instanceof ListenableFuture) {
                    return (ListenableFuture<Object>) response;
                }
                return immediateFuture(response);
            }
            catch (IllegalAccessException | IllegalArgumentException e) {
                // These really should never happen, since the method metadata should have prevented it
                return immediateFailedFuture(e);
            }
            catch (InvocationTargetException e) {
                Throwable cause = e.getCause();
                if (cause != null) {
                    return immediateFailedFuture(cause);
                }

                return immediateFailedFuture(e);
            }
        }
    }
}
