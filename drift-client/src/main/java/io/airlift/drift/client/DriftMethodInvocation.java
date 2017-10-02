/*
 * Copyright (C) 2017 Facebook, Inc.
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

import com.google.common.base.Ticker;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.drift.TException;
import io.airlift.drift.client.address.AddressSelector;
import io.airlift.drift.client.stats.MethodInvocationStat;
import io.airlift.drift.protocol.TTransportException;
import io.airlift.drift.transport.DriftApplicationException;
import io.airlift.drift.transport.InvokeRequest;
import io.airlift.drift.transport.MethodInvoker;
import io.airlift.drift.transport.MethodMetadata;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.drift.client.ExceptionClassification.HostStatus.DOWN;
import static io.airlift.drift.client.ExceptionClassification.HostStatus.NORMAL;
import static io.airlift.drift.client.ExceptionClassification.HostStatus.OVERLOADED;
import static io.airlift.units.Duration.succinctNanos;
import static java.lang.Boolean.FALSE;
import static java.util.Objects.requireNonNull;

@ThreadSafe
class DriftMethodInvocation
        extends AbstractFuture<Object>
{
    private static final Logger log = Logger.get(DriftMethodInvocation.class);

    private final MethodInvoker invoker;
    private final MethodMetadata metadata;
    private final Map<String, String> headers;
    private final List<Object> parameters;
    private final RetryPolicy retryPolicy;
    private final AddressSelector addressSelector;
    private final Optional<String> addressSelectionContext;
    private final MethodInvocationStat stat;
    private final Ticker ticker;
    private final long startTime;

    @GuardedBy("this")
    private int connectionAttempts;
    @GuardedBy("this")
    private int overloadedRejects;
    @GuardedBy("this")
    private int invocationAttempts;
    @GuardedBy("this")
    private Throwable lastException;

    @GuardedBy("this")
    private ListenableFuture<?> currentTask;

    static DriftMethodInvocation createDriftMethodInvocation(
            MethodInvoker invoker,
            MethodMetadata metadata,
            Map<String, String> headers,
            List<Object> parameters,
            RetryPolicy retryPolicy,
            AddressSelector addressSelector,
            Optional<String> addressSelectionContext,
            MethodInvocationStat stat,
            Ticker ticker)
    {
        DriftMethodInvocation invocation = new DriftMethodInvocation(
                invoker,
                metadata,
                headers,
                parameters,
                retryPolicy,
                addressSelector,
                addressSelectionContext,
                stat,
                ticker);
        // invocation can not be started from constructor, because it may start threads that can call back into the unpublished object
        invocation.nextAttempt();
        return invocation;
    }

    private DriftMethodInvocation(
            MethodInvoker invoker,
            MethodMetadata metadata,
            Map<String, String> headers,
            List<Object> parameters,
            RetryPolicy retryPolicy,
            AddressSelector addressSelector,
            Optional<String> addressSelectionContext,
            MethodInvocationStat stat,
            Ticker ticker)
    {
        this.invoker = requireNonNull(invoker, "methodHandler is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.headers = requireNonNull(headers, "headers is null");
        this.parameters = requireNonNull(parameters, "parameters is null");
        this.retryPolicy = requireNonNull(retryPolicy, "retryPolicy is null");
        this.addressSelector = requireNonNull(addressSelector, "addressSelector is null");
        this.addressSelectionContext = requireNonNull(addressSelectionContext, "addressSelectionContext is null");
        this.stat = requireNonNull(stat, "stat is null");
        this.ticker = requireNonNull(ticker, "ticker is null");
        this.startTime = ticker.read();

        // if this invocation is canceled, cancel the tasks
        super.addListener(() -> {
            if (super.isCancelled()) {
                onCancel(wasInterrupted());
            }
        }, directExecutor());
    }

    private synchronized void nextAttempt()
    {
        try {
            // request was already canceled
            if (isCancelled()) {
                return;
            }

            Optional<HostAndPort> address = addressSelector.selectAddress(addressSelectionContext);
            if (!address.isPresent()) {
                fail();
                return;
            }

            if (invocationAttempts > 0) {
                stat.recordRetry();
            }

            connectionAttempts++;

            long invocationStartTime = ticker.read();
            ListenableFuture<Object> result = invoker.invoke(new InvokeRequest(metadata, address.get(), headers, parameters));
            stat.recordResult(invocationStartTime, result);
            currentTask = result;

            Futures.addCallback(result, new FutureCallback<Object>()
            {
                @Override
                public void onSuccess(Object result)
                {
                    set(result);
                }

                @Override
                public void onFailure(Throwable t)
                {
                    handleFailure(address.get(), t);
                }
            });
        }
        catch (Throwable t) {
            // this should never happen, but ensure that invocation always finishes
            unexpectedError(t);
        }
    }

    private synchronized void handleFailure(HostAndPort address, Throwable throwable)
    {
        try {
            ExceptionClassification exceptionClassification = retryPolicy.classifyException(throwable);

            // update stats based on classification
            if (exceptionClassification.getHostStatus() == NORMAL) {
                // only store exception if the server is in a normal state
                lastException = throwable;
                invocationAttempts++;
            }
            else if (exceptionClassification.getHostStatus() == DOWN) {
                addressSelector.markdown(address);
            }
            else if (exceptionClassification.getHostStatus() == OVERLOADED) {
                addressSelector.markdown(address);
                overloadedRejects++;
            }

            // should retry?
            Duration duration = succinctNanos(ticker.read() - startTime);
            if (!exceptionClassification.isRetry().orElse(FALSE) ||
                    invocationAttempts > retryPolicy.getMaxRetries() ||
                    duration.compareTo(retryPolicy.getMaxRetryTime()) >= 0) {
                fail();
                return;
            }

            // A request to down or overloaded server is not counted as an attempt, and retries are not delayed
            if (exceptionClassification.getHostStatus() != NORMAL) {
                nextAttempt();
                return;
            }

            // backoff before next invocation
            Duration backoffDelay = retryPolicy.getBackoffDelay(invocationAttempts);
            log.debug("Failed invocation of %s with attempt %s, will retry in %s (overloadedRejects: %s). Exception: %s",
                    metadata.getName(),
                    invocationAttempts,
                    backoffDelay,
                    overloadedRejects,
                    throwable.getMessage());

            ListenableFuture<?> delay = invoker.delay(backoffDelay);
            currentTask = delay;
            Futures.addCallback(delay, new FutureCallback<Object>()
            {
                @Override
                public void onSuccess(Object result)
                {
                    nextAttempt();
                }

                @Override
                public void onFailure(Throwable throwable)
                {
                    // this should never happen in a delay future
                    unexpectedError(throwable);
                }
            });
        }
        catch (Throwable t) {
            // this should never happen, but ensure that invocation always finishes
            unexpectedError(t);
        }
    }

    private synchronized void onCancel(boolean wasInterrupted)
    {
        if (currentTask != null) {
            currentTask.cancel(wasInterrupted);
        }
    }

    private synchronized void fail()
    {
        Throwable cause = lastException;
        if (cause == null) {
            // There are no hosts or all hosts are marked down
            cause = new TTransportException("No hosts available");
        }

        RetriesFailedException retriesFailedException = new RetriesFailedException(
                invocationAttempts,
                succinctNanos(ticker.read() - startTime),
                connectionAttempts,
                overloadedRejects);

        // attach message exception to the exception thrown to caller
        if (cause instanceof DriftApplicationException) {
            cause.getCause().addSuppressed(retriesFailedException);
        }
        else {
            cause.addSuppressed(retriesFailedException);
        }

        setException(cause);
    }

    private synchronized void unexpectedError(Throwable throwable)
    {
        String message = "Unexpected error processing invocation of " + metadata.getName();
        setException(new TException(message, throwable));
        log.error(throwable, message);
    }
}
