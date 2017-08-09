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
package io.airlift.drift.client;

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.drift.transport.InvokeRequest;
import io.airlift.drift.transport.MethodInvoker;

import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class MockMethodInvoker
        implements MethodInvoker, Supplier<InvokeRequest>
{
    private final Supplier<ListenableFuture<Object>> resultsSupplier;

    private InvokeRequest request;

    public MockMethodInvoker(Supplier<ListenableFuture<Object>> resultsSupplier)
    {
        this.resultsSupplier = requireNonNull(resultsSupplier, "resultsSupplier is null");
    }

    @Override
    public InvokeRequest get()
    {
        return request;
    }

    @Override
    public ListenableFuture<Object> invoke(InvokeRequest request)
    {
        this.request = request;
        return resultsSupplier.get();
    }
}
