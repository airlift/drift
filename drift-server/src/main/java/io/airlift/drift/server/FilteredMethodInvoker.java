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

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.drift.transport.server.ServerInvokeRequest;

import java.util.List;

class FilteredMethodInvoker
        implements ServerMethodInvoker
{
    public static ServerMethodInvoker createFilteredMethodInvoker(List<MethodInvocationFilter> filters, ServerMethodInvoker methodInvoker)
    {
        for (MethodInvocationFilter filter : Lists.reverse(filters)) {
            methodInvoker = new FilteredMethodInvoker(filter, methodInvoker);
        }
        return methodInvoker;
    }

    private final MethodInvocationFilter filter;
    private final ServerMethodInvoker next;

    private FilteredMethodInvoker(MethodInvocationFilter filter, ServerMethodInvoker next)
    {
        this.filter = filter;
        this.next = next;
    }

    @Override
    public ListenableFuture<Object> invoke(ServerInvokeRequest request)
    {
        return filter.invoke(request, next);
    }
}
