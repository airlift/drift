/*
 * Copyright (C) 2017 Facebook, Inc.
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

import com.google.common.base.Ticker;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.drift.TException;
import io.airlift.drift.client.address.AddressSelector;
import io.airlift.drift.client.stats.MethodInvocationStat;
import io.airlift.drift.protocol.TTransportException;
import io.airlift.drift.transport.MethodMetadata;
import io.airlift.drift.transport.client.Address;
import io.airlift.drift.transport.client.ConnectionFailedException;
import io.airlift.drift.transport.client.DriftApplicationException;
import io.airlift.drift.transport.client.InvokeRequest;
import io.airlift.drift.transport.client.MethodInvoker;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.drift.client.ExceptionClassification.HostStatus.DOWN;
import static io.airlift.drift.client.ExceptionClassification.HostStatus.NORMAL;
import static io.airlift.drift.client.ExceptionClassification.HostStatus.OVERLOADED;
import static io.airlift.units.Duration.succinctNanos;
import static java.lang.Boolean.FALSE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@ThreadSafe
class DriftMethodInvocation<A extends Address>
        extends AbstractFuture<Object>
{
    private static final Logger log = Logger.get(DriftMethodInvocation.class);

    private final MethodInvoker invoker;
    private final MethodMetadata metadata;
    private final Map<String, String> headers;
    private final List<Object> parameters;
    private final RetryPolicy retryPolicy;
    private final AddressSelector<A> addressSelector;
    private final Optional<String> addressSelectionContext;
    private final MethodInvocationStat stat;
    private final Ticker ticker;
    private final long startTime;

    @GuardedBy("this")
    private final Set<A> attemptedAddresses = new LinkedHashSet<>();
    @GuardedBy("this")
    private final Multiset<A> failedConnectionAttempts = HashMultiset.create();
    @GuardedBy("this")
    private int failedConnections;
    @GuardedBy("this")
    private int overloadedRejects;
    @GuardedBy("this")
    private int invocationAttempts;
    @GuardedBy("this")
    private Throwable lastException;

    @GuardedBy("this")
    private ListenableFuture<?> currentTask;

    static <A extends Address> DriftMethodInvocation<A> createDriftMethodInvocation(
            MethodInvoker invoker,
            MethodMetadata metadata,
            Map<String, String> headers,
            List<Object> parameters,
            RetryPolicy retryPolicy,
            AddressSelector<A> addressSelector,
            Optional<String> addressSelectionContext,
            MethodInvocationStat stat,
            Ticker ticker)
    {
        DriftMethodInvocation<A> invocation = new DriftMethodInvocation<>(
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
        invocation.nextAttempt(true);
        return invocation;
    }

    private DriftMethodInvocation(
            MethodInvoker invoker,
            MethodMetadata metadata,
            Map<String, String> headers,
            List<Object> parameters,
            RetryPolicy retryPolicy,
            AddressSelector<A> addressSelector,
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

    private synchronized void nextAttempt(boolean noConnectDelay)
    {
        try {
            // request was already canceled
            if (isCancelled()) {
                return;
            }

            Optional<A> address = addressSelector.selectAddress(addressSelectionContext, attemptedAddresses);
            if (!address.isPresent()) {
                fail("No hosts available");
                return;
            }

            if (invocationAttempts > 0) {
                stat.recordRetry();
            }

            if (noConnectDelay) {
                invoke(address.get());
                return;
            }

            int connectionFailuresCount = failedConnectionAttempts.count(address.get());
            if (connectionFailuresCount == 0) {
                invoke(address.get());
                return;
            }

            Duration connectDelay = retryPolicy.getBackoffDelay(connectionFailuresCount);
            log.debug("Failed connection to %s with attempt %s, will retry in %s", address.get(), connectionFailuresCount, connectDelay);
            schedule(connectDelay, () -> invoke(address.get()));
        }
        catch (Throwable t) {
            // this should never happen, but ensure that invocation always finishes
            unexpectedError(t);
        }
    }

    private synchronized void invoke(A address)
    {
        try {
            long invocationStartTime = ticker.read();
            ListenableFuture<Object> result = invoker.invoke(new InvokeRequest(metadata, address, headers, parameters));
            stat.recordResult(invocationStartTime, result);
            currentTask = result;

            Futures.addCallback(result, new FutureCallback<Object>()
                    {
                        @Override
                        public void onSuccess(Object result)
                        {
                            resetConnectionFailures(address);
                            set(result);
                        }

                        @Override
                        public void onFailure(Throwable t)
                        {
                            handleFailure(address, t);
                        }
                    },
                    directExecutor());
        }
        catch (Throwable t) {
            // this should never happen, but ensure that invocation always finishes
            unexpectedError(t);
        }
    }

    private synchronized void resetConnectionFailures(A address)
    {
        failedConnectionAttempts.setCount(address, 0);
    }

    private synchronized void handleFailure(A address, Throwable throwable)
    {
        try {
            if (throwable instanceof ConnectionFailedException) {
                failedConnections++;
            }

            ExceptionClassification exceptionClassification = retryPolicy.classifyException(throwable, metadata.isIdempotent());

            // update stats based on classification
            attemptedAddresses.add(address);
            if (exceptionClassification.getHostStatus() == NORMAL) {
                // only store exception if the server is in a normal state
                lastException = throwable;
                invocationAttempts++;
            }
            else if (exceptionClassification.getHostStatus() == DOWN || exceptionClassification.getHostStatus() == OVERLOADED) {
                addressSelector.markdown(address);
                failedConnectionAttempts.add(address);
                if (exceptionClassification.getHostStatus() == OVERLOADED) {
                    overloadedRejects++;
                }
            }

            // should retry?
            Duration duration = succinctNanos(ticker.read() - startTime);
            if (!exceptionClassification.isRetry().orElse(FALSE)) {
                // always store exception if non-retryable, so it is added to the exception chain
                lastException = throwable;
                fail("Non-retryable exception");
                return;
            }
            if (invocationAttempts > retryPolicy.getMaxRetries()) {
                fail(format("Max retry attempts (%s) exceeded", retryPolicy.getMaxRetries()));
                return;
            }
            if (duration.compareTo(retryPolicy.getMaxRetryTime()) >= 0) {
                fail(format("Max retry time (%s) exceeded", retryPolicy.getMaxRetryTime()));
                return;
            }

            // A request to down or overloaded server is not counted as an attempt
            // Retries are not delayed based on the invocationAttempts, but may be delayed
            // based on the failed connection attempts for a selected address
            if (exceptionClassification.getHostStatus() != NORMAL) {
                nextAttempt(false);
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
            schedule(backoffDelay, () -> nextAttempt(true));
        }
        catch (Throwable t) {
            // this should never happen, but ensure that invocation always finishes
            unexpectedError(t);
        }
    }

    private synchronized void schedule(Duration timeout, Runnable task)
    {
        try {
            ListenableFuture<?> delay = invoker.delay(timeout);
            currentTask = delay;
            Futures.addCallback(delay, new FutureCallback<Object>()
                    {
                        @Override
                        public void onSuccess(Object result)
                        {
                            task.run();
                        }

                        @Override
                        public void onFailure(Throwable t)
                        {
                            // this should never happen in a delay future
                            unexpectedError(t);
                        }
                    },
                    directExecutor());
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

    private synchronized void fail(String reason)
    {
        Throwable cause = lastException;
        if (cause == null) {
            // There are no hosts or all hosts are marked down
            cause = new TTransportException(reason);
        }

        RetriesFailedException retriesFailedException = new RetriesFailedException(
                reason,
                invocationAttempts,
                succinctNanos(ticker.read() - startTime),
                failedConnections,
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
