/*
 * Copyright (C) 2012 Facebook, Inc.
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

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.drift.transport.client.InvokeRequest;
import io.airlift.drift.transport.client.MethodInvoker;
import io.airlift.testing.TestingTicker;
import io.airlift.units.Duration;

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class MockMethodInvoker
        implements MethodInvoker, Supplier<InvokeRequest>
{
    private final Supplier<ListenableFuture<Object>> resultsSupplier;
    private final TestingTicker ticker;

    @GuardedBy("this")
    private InvokeRequest request;

    @GuardedBy("this")
    private final List<Duration> delays = new ArrayList<>();

    public MockMethodInvoker(Supplier<ListenableFuture<Object>> resultsSupplier)
    {
        this(resultsSupplier, new TestingTicker());
    }

    public MockMethodInvoker(Supplier<ListenableFuture<Object>> resultsSupplier, TestingTicker ticker)
    {
        this.resultsSupplier = requireNonNull(resultsSupplier, "resultsSupplier is null");
        this.ticker = requireNonNull(ticker, "ticker is null");
    }

    @Override
    public synchronized InvokeRequest get()
    {
        return request;
    }

    public synchronized List<Duration> getDelays()
    {
        return ImmutableList.copyOf(delays);
    }

    @Override
    public synchronized ListenableFuture<Object> invoke(InvokeRequest request)
    {
        this.request = request;
        return resultsSupplier.get();
    }

    @Override
    public synchronized ListenableFuture<?> delay(Duration duration)
    {
        delays.add(duration);
        ticker.increment(duration.toMillis(), MILLISECONDS);
        return Futures.immediateFuture(null);
    }
}
