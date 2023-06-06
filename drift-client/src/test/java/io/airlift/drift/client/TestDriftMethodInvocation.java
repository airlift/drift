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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.drift.TException;
import io.airlift.drift.client.ExceptionClassification.HostStatus;
import io.airlift.drift.client.address.AddressSelector;
import io.airlift.drift.client.address.SimpleAddressSelector.SimpleAddress;
import io.airlift.drift.codec.ThriftCodec;
import io.airlift.drift.codec.internal.builtin.ShortThriftCodec;
import io.airlift.drift.protocol.TTransportException;
import io.airlift.drift.transport.MethodMetadata;
import io.airlift.drift.transport.client.Address;
import io.airlift.drift.transport.client.ConnectionFailedException;
import io.airlift.drift.transport.client.DriftApplicationException;
import io.airlift.drift.transport.client.DriftClientConfig;
import io.airlift.testing.TestingTicker;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import javax.annotation.concurrent.GuardedBy;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static com.google.common.base.Ticker.systemTicker;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.airlift.drift.client.ExceptionClassification.HostStatus.DOWN;
import static io.airlift.drift.client.ExceptionClassification.HostStatus.NORMAL;
import static io.airlift.drift.client.ExceptionClassification.HostStatus.OVERLOADED;
import static io.airlift.drift.client.TestDriftMethodInvocation.ClassifiedException.createClassifiedException;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestDriftMethodInvocation
{
    private static final Object SUCCESS = "ok";
    private static final MethodMetadata METHOD_METADATA = new MethodMetadata(
            "testMethod",
            ImmutableList.of(),
            (ThriftCodec<Object>) (Object) new ShortThriftCodec(),
            ImmutableMap.of(),
            ImmutableMap.of(),
            false,
            true);
    private static final Error UNEXPECTED_EXCEPTION = new Error("unexpected exception");

    @Test(timeOut = 60000)
    public void testFirstTrySuccess()
            throws Exception
    {
        TestingMethodInvocationStat stat = new TestingMethodInvocationStat();
        DriftMethodInvocation<?> methodInvocation = createDriftMethodInvocation(RetryPolicy.NO_RETRY_POLICY, stat, () -> immediateFuture(SUCCESS));

        assertEquals(methodInvocation.get(), SUCCESS);
        stat.assertSuccess(0);
    }

    @Test(timeOut = 60000)
    public void testBasicRetriesToSuccess()
            throws Exception
    {
        testBasicRetriesToSuccess(0, true);
        testBasicRetriesToSuccess(1, true);
        testBasicRetriesToSuccess(3, true);
        testBasicRetriesToSuccess(10, true);
        testBasicRetriesToSuccess(0, false);
        testBasicRetriesToSuccess(1, false);
        testBasicRetriesToSuccess(3, false);
        testBasicRetriesToSuccess(10, false);
    }

    private static void testBasicRetriesToSuccess(int expectedRetries, boolean wrapWithApplicationException)
            throws Exception
    {
        RetryPolicy retryPolicy = new RetryPolicy(
                new DriftClientConfig()
                        .setMaxRetries(expectedRetries + 10)
                        .setMinBackoffDelay(new Duration(1, SECONDS))
                        .setMaxBackoffDelay(new Duration(1, DAYS))
                        .setBackoffScaleFactor(2.0),
                new TestingExceptionClassifier());

        TestingMethodInvocationStat stat = new TestingMethodInvocationStat();
        AtomicInteger attempts = new AtomicInteger();
        MockMethodInvoker invoker = new MockMethodInvoker(() -> {
            int currentAttempts = attempts.getAndIncrement();
            if (currentAttempts < expectedRetries) {
                return immediateFailedFuture(createClassifiedException(true, NORMAL, wrapWithApplicationException));
            }
            return immediateFuture(SUCCESS);
        });
        DriftMethodInvocation<?> methodInvocation = createDriftMethodInvocation(retryPolicy, stat, invoker, new TestingAddressSelector(100), systemTicker());

        assertEquals(methodInvocation.get(), SUCCESS);
        assertEquals(attempts.get(), expectedRetries + 1);
        stat.assertSuccess(expectedRetries);
        assertDelays(invoker, retryPolicy, expectedRetries);
    }

    @Test(timeOut = 60000)
    public void testBasicRetriesToFailure()
            throws Exception
    {
        testBasicRetriesToFailure(0, true);
        testBasicRetriesToFailure(1, true);
        testBasicRetriesToFailure(5, true);
        testBasicRetriesToFailure(10, true);
        testBasicRetriesToFailure(0, false);
        testBasicRetriesToFailure(1, false);
        testBasicRetriesToFailure(5, false);
        testBasicRetriesToFailure(10, false);
    }

    private static void testBasicRetriesToFailure(int expectedRetries, boolean wrapWithApplicationException)
            throws Exception
    {
        RetryPolicy retryPolicy = new RetryPolicy(
                new DriftClientConfig()
                        .setMaxRetries(expectedRetries + 10)
                        .setMinBackoffDelay(new Duration(1, SECONDS))
                        .setMaxBackoffDelay(new Duration(100, SECONDS))
                        .setBackoffScaleFactor(2.0),
                new TestingExceptionClassifier());

        TestingMethodInvocationStat stat = new TestingMethodInvocationStat();
        AtomicInteger attempts = new AtomicInteger();
        MockMethodInvoker invoker = new MockMethodInvoker(() -> {
            int currentAttempts = attempts.getAndIncrement();
            if (currentAttempts < expectedRetries) {
                return immediateFailedFuture(createClassifiedException(true, NORMAL, wrapWithApplicationException));
            }
            return immediateFailedFuture(createClassifiedException(false, NORMAL, wrapWithApplicationException));
        });
        DriftMethodInvocation<?> methodInvocation = createDriftMethodInvocation(
                retryPolicy,
                stat,
                invoker,
                new TestingAddressSelector(100),
                systemTicker());

        try {
            methodInvocation.get();
            fail("Expected exception");
        }
        catch (ExecutionException e) {
            assertEquals(attempts.get(), expectedRetries + 1);
            assertClassifiedException(e.getCause(), new ExceptionClassification(Optional.of(false), NORMAL), expectedRetries);
        }
        stat.assertFailure(expectedRetries);
        assertDelays(invoker, retryPolicy, expectedRetries);
    }

    @Test(timeOut = 60000)
    public void testBasicRetriesToNoHosts()
            throws Exception
    {
        RetryPolicy retryPolicy = new RetryPolicy(new DriftClientConfig().setMaxRetries(10), new TestingExceptionClassifier());

        TestingMethodInvocationStat stat = new TestingMethodInvocationStat();
        AtomicInteger attempts = new AtomicInteger();
        int expectedRetries = 3;
        DriftMethodInvocation<?> methodInvocation = createDriftMethodInvocation(
                retryPolicy,
                stat,
                new MockMethodInvoker(() -> {
                    attempts.getAndIncrement();
                    return immediateFailedFuture(createClassifiedException(true, NORMAL));
                }),
                new TestingAddressSelector(expectedRetries + 1),
                systemTicker());

        try {
            methodInvocation.get();
            fail("Expected exception");
        }
        catch (ExecutionException e) {
            assertEquals(attempts.get(), expectedRetries + 1);
            assertClassifiedException(e.getCause(), new ExceptionClassification(Optional.of(true), NORMAL), expectedRetries);
        }
        stat.assertFailure(expectedRetries);
    }

    @Test(timeOut = 60000)
    public void testMaxRetries()
            throws Exception
    {
        int maxRetries = 5;
        RetryPolicy retryPolicy = new RetryPolicy(new DriftClientConfig().setMaxRetries(maxRetries), new TestingExceptionClassifier());

        TestingMethodInvocationStat stat = new TestingMethodInvocationStat();
        AtomicInteger attempts = new AtomicInteger();
        DriftMethodInvocation<?> methodInvocation = createDriftMethodInvocation(retryPolicy, stat, () -> {
            attempts.getAndIncrement();
            return immediateFailedFuture(createClassifiedException(true, NORMAL));
        });

        try {
            methodInvocation.get();
            fail("Expected exception");
        }
        catch (ExecutionException e) {
            assertEquals(attempts.get(), maxRetries + 1);
            assertClassifiedException(e.getCause(), new ExceptionClassification(Optional.of(true), NORMAL), maxRetries);
        }
        stat.assertFailure(maxRetries);
    }

    @Test(timeOut = 60000)
    public void testMaxRetryTime()
            throws Exception
    {
        TestingTicker ticker = new TestingTicker();
        int maxRetries = 7;
        RetryPolicy retryPolicy = new RetryPolicy(
                new DriftClientConfig()
                        .setMaxRetries(maxRetries + 10)
                        .setMinBackoffDelay(new Duration(1, SECONDS))
                        .setMaxBackoffDelay(new Duration(1, DAYS))
                        .setMaxRetryTime(new Duration(1 + 2 + 4 + 8 + 16 + 32 + 64, SECONDS))
                        .setBackoffScaleFactor(2.0),
                new TestingExceptionClassifier());

        TestingMethodInvocationStat stat = new TestingMethodInvocationStat();
        AtomicInteger attempts = new AtomicInteger();
        MockMethodInvoker invoker = new MockMethodInvoker(
                () -> {
                    attempts.getAndIncrement();
                    return immediateFailedFuture(createClassifiedException(true, NORMAL));
                },
                ticker);
        DriftMethodInvocation<?> methodInvocation = createDriftMethodInvocation(retryPolicy, stat, invoker, new TestingAddressSelector(100), ticker);

        try {
            methodInvocation.get();
            fail("Expected exception");
        }
        catch (ExecutionException e) {
            assertEquals(attempts.get(), maxRetries + 1);
            assertClassifiedException(e.getCause(), new ExceptionClassification(Optional.of(true), NORMAL), maxRetries);
        }
        stat.assertFailure(maxRetries);
        assertDelays(invoker, retryPolicy, 7);
    }

    @Test(timeOut = 60000)
    public void testExhaustHosts()
            throws Exception
    {
        testExhaustHosts(0, false);
        testExhaustHosts(1, false);
        testExhaustHosts(10, false);
        testExhaustHosts(0, true);
        testExhaustHosts(1, true);
        testExhaustHosts(10, true);
    }

    private static void testExhaustHosts(int expectedRetries, boolean overloaded)
            throws Exception
    {
        RetryPolicy retryPolicy = new RetryPolicy(new DriftClientConfig().setMaxRetries(expectedRetries + 10), new TestingExceptionClassifier());

        TestingMethodInvocationStat stat = new TestingMethodInvocationStat();
        AtomicInteger attempts = new AtomicInteger();
        TestingAddressSelector addressSelector = new TestingAddressSelector(expectedRetries);
        Set<Address> attemptedAddresses = newConcurrentHashSet();
        MockMethodInvoker invoker = new MockMethodInvoker(request -> {
            attempts.getAndIncrement();
            attemptedAddresses.add(request.getAddress());
            return immediateFailedFuture(createClassifiedException(true, overloaded ? OVERLOADED : DOWN));
        });
        DriftMethodInvocation<?> methodInvocation = createDriftMethodInvocation(
                retryPolicy,
                stat,
                invoker,
                addressSelector,
                systemTicker());

        try {
            methodInvocation.get();
            fail("Expected exception");
        }
        catch (ExecutionException e) {
            assertEquals(attempts.get(), expectedRetries);
            assertTrue(e.getCause() instanceof TTransportException);
            TTransportException transportException = (TTransportException) e.getCause();
            assertTrue(transportException.getMessage().startsWith("No hosts available"));
            assertRetriesFailedInformation(transportException, 0, 0, overloaded ? expectedRetries : 0);
        }
        stat.assertNoHostsAvailable(expectedRetries);
        addressSelector.assertAllDown();
        assertEquals(invoker.getDelays().size(), 0);
        assertEquals(attemptedAddresses, addressSelector.getLastAttemptedSet());
    }

    @Test(timeOut = 60000)
    public void testConnectionFailed()
            throws Exception
    {
        testConnectionFailed(0, 3);
        testConnectionFailed(1, 3);
        testConnectionFailed(10, 3);
    }

    private static void testConnectionFailed(int expectedInvocationAttempts, int failedConnections)
            throws Exception
    {
        AtomicInteger attempts = new AtomicInteger();
        MockMethodInvoker invoker = new MockMethodInvoker(request -> {
            int tries = attempts.getAndIncrement();
            if (tries < expectedInvocationAttempts) {
                return immediateFailedFuture(createClassifiedException(true, NORMAL));
            }
            if (tries < failedConnections + expectedInvocationAttempts) {
                return immediateFailedFuture(new ConnectionFailedException(request.getAddress(), new Exception()));
            }
            // use down for last exception because it isn't counted as an invocation
            return immediateFailedFuture(createClassifiedException(false, DOWN));
        });
        DriftMethodInvocation<?> methodInvocation = createDriftMethodInvocation(
                new RetryPolicy(new DriftClientConfig().setMaxRetries(100), new TestingExceptionClassifier()),
                new TestingMethodInvocationStat(),
                invoker,
                new TestingAddressSelector(100),
                systemTicker());

        try {
            methodInvocation.get();
            fail("Expected exception");
        }
        catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof DriftApplicationException);
            DriftApplicationException applicationException = (DriftApplicationException) e.getCause();
            assertTrue(applicationException.getCause() instanceof ClassifiedException);
            ClassifiedException classifiedException = (ClassifiedException) applicationException.getCause();
            assertRetriesFailedInformation(classifiedException, failedConnections, expectedInvocationAttempts, 0);
        }
    }

    @Test
    public void testConnectionFailedDelay()
            throws Exception
    {
        testConnectionFailedDelay(0, 0, 0);
        testConnectionFailedDelay(1, 1, 0);
        testConnectionFailedDelay(10, 1, 0);
        testConnectionFailedDelay(1, 2, 1);
        testConnectionFailedDelay(2, 2, 2);
        testConnectionFailedDelay(10, 2, 10);
        testConnectionFailedDelay(10, 5, 40);
    }

    private static void testConnectionFailedDelay(int numberOfAddresses, int numberOfRetriesPerAddress, int expectedDelays)
            throws Exception
    {
        testConnectionFailedDelay(false, numberOfAddresses, numberOfRetriesPerAddress, expectedDelays);
        testConnectionFailedDelay(true, numberOfAddresses, numberOfRetriesPerAddress, expectedDelays);
    }

    private static void testConnectionFailedDelay(boolean overloaded, int numberOfAddresses, int numberOfRetriesPerAddress, int expectedDelays)
            throws Exception
    {
        ImmutableList.Builder<Address> addresses = ImmutableList.builder();
        for (int i = 0; i < numberOfAddresses; i++) {
            Address address = createTestingAddress(20_000 + i);
            for (int j = 0; j < numberOfRetriesPerAddress; j++) {
                addresses.add(address);
            }
        }

        MockMethodInvoker invoker = new MockMethodInvoker(request -> immediateFailedFuture(createClassifiedException(true, overloaded ? OVERLOADED : DOWN)));
        DriftMethodInvocation<?> methodInvocation = createDriftMethodInvocation(
                new RetryPolicy(new DriftClientConfig(), new TestingExceptionClassifier()),
                new TestingMethodInvocationStat(),
                invoker,
                new TestingAddressSelector(addresses.build()),
                systemTicker());

        try {
            methodInvocation.get();
            fail("Expected exception");
        }
        catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof TTransportException);
            TTransportException transportException = (TTransportException) e.getCause();
            assertTrue(transportException.getMessage().startsWith("No hosts available"));
        }
        assertEquals(invoker.getDelays().size(), expectedDelays);
    }

    @Test(timeOut = 60000)
    public void testExceptionFromInvokerInvoke()
            throws Exception
    {
        testExceptionFromInvokerInvoke(0);
        testExceptionFromInvokerInvoke(1);
        testExceptionFromInvokerInvoke(10);
    }

    private static void testExceptionFromInvokerInvoke(int expectedRetries)
            throws Exception
    {
        RetryPolicy retryPolicy = new RetryPolicy(new DriftClientConfig().setMaxRetries(expectedRetries + 10), new TestingExceptionClassifier());

        TestingMethodInvocationStat stat = new TestingMethodInvocationStat();

        AtomicInteger attempts = new AtomicInteger();
        DriftMethodInvocation<?> methodInvocation = createDriftMethodInvocation(
                retryPolicy,
                stat,
                () -> {
                    attempts.getAndIncrement();
                    if (attempts.get() > expectedRetries) {
                        throw UNEXPECTED_EXCEPTION;
                    }
                    return immediateFailedFuture(createClassifiedException(true, NORMAL));
                });

        try {
            methodInvocation.get();
            fail("Expected exception");
        }
        catch (ExecutionException e) {
            assertEquals(attempts.get(), expectedRetries + 1);
            assertUnexpectedException(e.getCause());
        }
    }

    @Test(timeOut = 60000)
    public void testExceptionFromInvokerDelay()
            throws Exception
    {
        testExceptionFromInvokerDelay(0, true);
        testExceptionFromInvokerDelay(1, true);
        testExceptionFromInvokerDelay(10, true);
        testExceptionFromInvokerDelay(0, false);
        testExceptionFromInvokerDelay(1, false);
        testExceptionFromInvokerDelay(10, false);
    }

    private static void testExceptionFromInvokerDelay(int expectedRetries, final boolean throwUnexpected)
            throws Exception
    {
        RetryPolicy retryPolicy = new RetryPolicy(new DriftClientConfig().setMaxRetries(expectedRetries + 10), new TestingExceptionClassifier());

        TestingMethodInvocationStat stat = new TestingMethodInvocationStat();

        AtomicInteger attempts = new AtomicInteger();
        DriftMethodInvocation<?> methodInvocation = createDriftMethodInvocation(
                retryPolicy,
                stat,
                new MockMethodInvoker(() -> {
                    attempts.getAndIncrement();
                    return immediateFailedFuture(createClassifiedException(true, NORMAL));
                })
                {
                    @Override
                    public synchronized ListenableFuture<?> delay(Duration duration)
                    {
                        if (attempts.get() > expectedRetries) {
                            if (throwUnexpected) {
                                throw UNEXPECTED_EXCEPTION;
                            }
                            else {
                                return immediateFailedFuture(UNEXPECTED_EXCEPTION);
                            }
                        }
                        return super.delay(duration);
                    }
                },
                new TestingAddressSelector(100),
                systemTicker());

        try {
            methodInvocation.get();
            fail("Expected exception");
        }
        catch (ExecutionException e) {
            assertEquals(attempts.get(), expectedRetries + 1);
            assertUnexpectedException(e.getCause());
        }
    }

    @Test(timeOut = 60000)
    public void testExceptionFromExceptionClassifier()
            throws Exception
    {
        testExceptionFromExceptionClassifier(0);
        testExceptionFromExceptionClassifier(1);
        testExceptionFromExceptionClassifier(10);
    }

    private static void testExceptionFromExceptionClassifier(int expectedRetries)
            throws Exception
    {
        AtomicInteger attempts = new AtomicInteger();
        RetryPolicy retryPolicy = new RetryPolicy(
                new DriftClientConfig().setMaxRetries(expectedRetries + 10),
                new TestingExceptionClassifier()
                {
                    @Override
                    public ExceptionClassification classifyException(Throwable throwable)
                    {
                        if (attempts.get() > expectedRetries) {
                            throw UNEXPECTED_EXCEPTION;
                        }
                        return super.classifyException(throwable);
                    }
                });

        TestingMethodInvocationStat stat = new TestingMethodInvocationStat();

        DriftMethodInvocation<?> methodInvocation = createDriftMethodInvocation(
                retryPolicy,
                stat,
                () -> {
                    attempts.getAndIncrement();
                    return immediateFailedFuture(createClassifiedException(true, NORMAL));
                });

        try {
            methodInvocation.get();
            fail("Expected exception");
        }
        catch (ExecutionException e) {
            assertEquals(attempts.get(), expectedRetries + 1);
            assertUnexpectedException(e.getCause());
        }
    }

    @Test(timeOut = 60000)
    public void testExceptionFromAddressSelectorSelectAddress()
            throws Exception
    {
        testExceptionFromAddressSelectorSelectAddress(0);
        testExceptionFromAddressSelectorSelectAddress(1);
        testExceptionFromAddressSelectorSelectAddress(10);
    }

    private static void testExceptionFromAddressSelectorSelectAddress(int expectedRetries)
            throws Exception
    {
        RetryPolicy retryPolicy = new RetryPolicy(new DriftClientConfig().setMaxRetries(expectedRetries + 10), new TestingExceptionClassifier());

        TestingMethodInvocationStat stat = new TestingMethodInvocationStat();
        AtomicInteger attempts = new AtomicInteger();

        DriftMethodInvocation<?> methodInvocation = createDriftMethodInvocation(
                retryPolicy,
                stat,
                new MockMethodInvoker(() -> {
                    attempts.getAndIncrement();
                    return immediateFailedFuture(createClassifiedException(true, NORMAL));
                }),
                new TestingAddressSelector(100)
                {
                    @Override
                    public synchronized Optional<Address> selectAddress(Optional<String> addressSelectionContext, Set<Address> attempted)
                    {
                        if (attempts.get() < expectedRetries) {
                            return super.selectAddress(addressSelectionContext, attempted);
                        }
                        throw UNEXPECTED_EXCEPTION;
                    }
                },
                systemTicker());

        try {
            methodInvocation.get();
            fail("Expected exception");
        }
        catch (ExecutionException e) {
            assertEquals(attempts.get(), expectedRetries);
            assertUnexpectedException(e.getCause());
        }
    }

    @Test(timeOut = 60000)
    public void testExceptionFromAddressSelectorMarkDown()
            throws Exception
    {
        testExceptionFromAddressSelectorMarkDown(0);
        testExceptionFromAddressSelectorMarkDown(1);
        testExceptionFromAddressSelectorMarkDown(10);
    }

    private static void testExceptionFromAddressSelectorMarkDown(int expectedRetries)
            throws Exception
    {
        RetryPolicy retryPolicy = new RetryPolicy(new DriftClientConfig().setMaxRetries(expectedRetries + 10), new TestingExceptionClassifier());

        TestingMethodInvocationStat stat = new TestingMethodInvocationStat();
        AtomicInteger attempts = new AtomicInteger();
        DriftMethodInvocation<?> methodInvocation = createDriftMethodInvocation(
                retryPolicy,
                stat,
                new MockMethodInvoker(() -> {
                    attempts.getAndIncrement();
                    return immediateFailedFuture(createClassifiedException(true, DOWN));
                }),
                new TestingAddressSelector(100)
                {
                    @Override
                    public synchronized void markdown(Address address)
                    {
                        if (attempts.get() > expectedRetries) {
                            throw UNEXPECTED_EXCEPTION;
                        }
                    }
                },
                systemTicker());

        try {
            methodInvocation.get();
            fail("Expected exception");
        }
        catch (ExecutionException e) {
            assertEquals(attempts.get(), expectedRetries + 1);
            assertUnexpectedException(e.getCause());
        }
    }

    @Test(timeOut = 60000)
    public void testPropagateCancel()
            throws Exception
    {
        testPropagateCancel(0, false);
        testPropagateCancel(1, false);
        testPropagateCancel(10, false);
        testPropagateCancel(0, true);
        testPropagateCancel(1, true);
        testPropagateCancel(10, true);
    }

    private static void testPropagateCancel(int expectedRetries, boolean interrupt)
            throws Exception
    {
        RetryPolicy retryPolicy = new RetryPolicy(new DriftClientConfig().setMaxRetries(expectedRetries + 10), new TestingExceptionClassifier());

        TestingMethodInvocationStat stat = new TestingMethodInvocationStat();
        AtomicInteger attempts = new AtomicInteger();
        TestFuture future = new TestFuture();
        CountDownLatch settableFutureFetched = new CountDownLatch(1);
        DriftMethodInvocation<?> methodInvocation = createDriftMethodInvocation(
                retryPolicy,
                stat,
                () -> {
                    attempts.getAndIncrement();
                    if (attempts.get() > expectedRetries) {
                        settableFutureFetched.countDown();
                        return future;
                    }
                    return immediateFailedFuture(createClassifiedException(true, NORMAL));
                });

        settableFutureFetched.await();
        methodInvocation.cancel(interrupt);
        assertTrue(future.isCancelled());
        assertEquals(future.checkWasInterrupted(), interrupt);
        assertEquals(attempts.get(), expectedRetries + 1);
    }

    private static DriftMethodInvocation<?> createDriftMethodInvocation(RetryPolicy retryPolicy, TestingMethodInvocationStat stat, Supplier<ListenableFuture<Object>> resultsSupplier)
    {
        return createDriftMethodInvocation(retryPolicy, stat, new MockMethodInvoker(resultsSupplier), new TestingAddressSelector(100), systemTicker());
    }

    private static DriftMethodInvocation<?> createDriftMethodInvocation(
            RetryPolicy retryPolicy,
            TestingMethodInvocationStat stat,
            MockMethodInvoker invoker,
            AddressSelector<?> addressSelector,
            Ticker ticker)
    {
        return DriftMethodInvocation.createDriftMethodInvocation(
                invoker,
                METHOD_METADATA,
                ImmutableMap.of(),
                ImmutableList.of(),
                retryPolicy,
                addressSelector,
                Optional.empty(),
                stat,
                ticker);
    }

    private static void assertClassifiedException(Throwable cause, ExceptionClassification exceptionClassification, int expectedRetries)
    {
        if (cause instanceof DriftApplicationException) {
            cause = cause.getCause();
        }
        assertTrue(cause instanceof ClassifiedException);
        ClassifiedException classifiedException = (ClassifiedException) cause;
        assertEquals(classifiedException.getClassification(), exceptionClassification);
        assertRetriesFailedInformation(classifiedException, 0, expectedRetries + 1, 0);
    }

    private static void assertRetriesFailedInformation(Throwable exception, int expectedFailedConnections, int expectedInvocationAttempts, int expectedOverloaded)
    {
        RetriesFailedException retriesFailedException = getRetriesFailedException(exception);
        assertEquals(retriesFailedException.getFailedConnections(), expectedFailedConnections);
        assertEquals(retriesFailedException.getInvocationAttempts(), expectedInvocationAttempts);
        assertEquals(retriesFailedException.getOverloadedRejects(), expectedOverloaded);
    }

    private static RetriesFailedException getRetriesFailedException(Throwable exception)
    {
        // method invocation attaches retry information using a suppressed exception
        Throwable[] suppressed = exception.getSuppressed();
        assertEquals(suppressed.length, 1);
        assertTrue(suppressed[0] instanceof RetriesFailedException);
        return (RetriesFailedException) suppressed[0];
    }

    private static void assertUnexpectedException(Throwable cause)
    {
        assertEquals(cause.getClass(), TException.class);
        TException exception = (TException) cause;
        assertTrue(exception.getMessage().matches("Unexpected error processing.*" + METHOD_METADATA.getName() + ".*"));
        assertSame(exception.getCause(), UNEXPECTED_EXCEPTION);
        // No retry information is attached to an unexpected exception
        assertEquals(exception.getSuppressed().length, 0);
    }

    private static void assertDelays(MockMethodInvoker invoker, RetryPolicy retryPolicy, int expectedRetries)
    {
        assertEquals(invoker.getDelays(), IntStream.range(0, expectedRetries)
                .mapToObj(i -> retryPolicy.getBackoffDelay(i + 1))
                .collect(toImmutableList()));
    }

    private static Address createTestingAddress(int port)
    {
        return new SimpleAddress(HostAndPort.fromParts("localhost", port));
    }

    private static class TestingExceptionClassifier
            implements ExceptionClassifier
    {
        @Override
        public ExceptionClassification classifyException(Throwable throwable)
        {
            if (throwable instanceof DriftApplicationException) {
                throwable = throwable.getCause();
            }
            return ((ClassifiedException) throwable).getClassification();
        }
    }

    public static class ClassifiedException
            extends Exception
    {
        private final ExceptionClassification classification;

        public static Exception createClassifiedException(boolean retry, HostStatus hostStatus)
        {
            return createClassifiedException(retry, hostStatus, true);
        }

        public static Exception createClassifiedException(boolean retry, HostStatus hostStatus, boolean wrapWithApplicationException)
        {
            Exception exception = new ClassifiedException(new ExceptionClassification(Optional.of(retry), hostStatus));
            if (wrapWithApplicationException) {
                exception = new DriftApplicationException(exception, Optional.empty());
            }
            return exception;
        }

        public ClassifiedException(ExceptionClassification classification)
        {
            super(classification.toString());
            this.classification = requireNonNull(classification, "classification is null");
        }

        public ExceptionClassification getClassification()
        {
            return classification;
        }
    }

    public static class TestingAddressSelector
            implements AddressSelector<Address>
    {
        private List<Address> addresses;

        @GuardedBy("this")
        private final Set<Address> markdownHosts = new HashSet<>();

        @GuardedBy("this")
        private int addressCount;

        @GuardedBy("this")
        private Set<Address> lastAttemptedSet = ImmutableSet.of();

        public TestingAddressSelector(int maxAddresses)
        {
            this(createAddresses(maxAddresses));
        }

        private static List<Address> createAddresses(int count)
        {
            return IntStream.range(0, count)
                    .mapToObj(i -> createTestingAddress(20_000 + i))
                    .collect(toImmutableList());
        }

        public TestingAddressSelector(List<Address> addresses)
        {
            this.addresses = ImmutableList.copyOf(requireNonNull(addresses, "addresses is null"));
        }

        @Override
        public synchronized Optional<Address> selectAddress(Optional<String> addressSelectionContext, Set<Address> attempted)
        {
            lastAttemptedSet = ImmutableSet.copyOf(attempted);
            if (addressCount >= addresses.size()) {
                return Optional.empty();
            }
            return Optional.of(addresses.get(addressCount++));
        }

        @Override
        public synchronized void markdown(Address address)
        {
            markdownHosts.add(address);
        }

        public synchronized void assertAllDown()
        {
            assertEquals(markdownHosts, ImmutableSet.copyOf(addresses));
        }

        public synchronized Set<Address> getLastAttemptedSet()
        {
            return lastAttemptedSet;
        }
    }

    public static class TestFuture
            extends AbstractFuture<Object>
    {
        public boolean checkWasInterrupted()
        {
            return wasInterrupted();
        }
    }
}
