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
package io.airlift.drift.transport.netty.client;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import io.airlift.concurrent.MoreFutures;
import io.airlift.drift.transport.client.InvokeRequest;
import io.airlift.drift.transport.client.MethodInvoker;
import io.airlift.drift.transport.client.RequestTimeoutException;
import io.airlift.drift.transport.netty.client.ConnectionManager.ConnectionParameters;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

class DriftNettyMethodInvoker
        implements MethodInvoker
{
    private static final Logger log = Logger.get(DriftNettyMethodInvoker.class);
    private static final Duration DEFAULT_INVOCATION_TIMEOUT_GRACE_PERIOD = new Duration(10, SECONDS);

    private final ConnectionParameters connectionParameters;
    private final ConnectionManager connectionManager;
    private final ListeningScheduledExecutorService delayService;
    private final Duration invokeTimeout;

    public DriftNettyMethodInvoker(ConnectionParameters connectionParameters, ConnectionManager connectionManager, ScheduledExecutorService delayService)
    {
        this(connectionParameters, connectionManager, delayService, DEFAULT_INVOCATION_TIMEOUT_GRACE_PERIOD);
    }

    @VisibleForTesting
    DriftNettyMethodInvoker(
            ConnectionParameters connectionParameters,
            ConnectionManager connectionManager,
            ScheduledExecutorService delayService,
            Duration invocationTimeoutGracePeriod)
    {
        this.connectionParameters = requireNonNull(connectionParameters, "connectionConfig is null");
        this.connectionManager = requireNonNull(connectionManager, "connectionManager is null");
        this.delayService = listeningDecorator(requireNonNull(delayService, "delayService is null"));

        // an invocation should complete long before this
        this.invokeTimeout = new Duration(
                invocationTimeoutGracePeriod.toMillis() +
                        connectionParameters.getConnectTimeout().toMillis() +
                        connectionParameters.getRequestTimeout().toMillis(),
                MILLISECONDS);
    }

    @Override
    public ListenableFuture<Object> invoke(InvokeRequest request)
    {
        try {
            // be safe and make sure the future always completes
            return MoreFutures.addTimeout(
                    InvocationResponseFuture.createInvocationResponseFuture(request, connectionParameters, connectionManager),
                    () -> {
                        // log before throwing as this is likely a bug in Drift or Netty
                        String message = "Invocation response future did not complete after " + invokeTimeout;
                        log.error(message);
                        throw new RequestTimeoutException(message);
                    },
                    invokeTimeout,
                    delayService);
        }
        catch (Exception e) {
            return immediateFailedFuture(e);
        }
    }

    @Override
    public ListenableFuture<?> delay(Duration duration)
    {
        return delayService.schedule(() -> null, duration.toMillis(), MILLISECONDS);
    }
}
