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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.drift.TApplicationException;
import io.airlift.drift.TException;
import io.airlift.drift.annotations.ThriftException;
import io.airlift.drift.annotations.ThriftMethod;
import io.airlift.drift.annotations.ThriftService;
import io.airlift.drift.annotations.ThriftStruct;
import io.airlift.drift.client.address.MockAddressSelector;
import io.airlift.drift.client.stats.MethodInvocationStatsFactory;
import io.airlift.drift.codec.ThriftCodecManager;
import io.airlift.drift.codec.guice.ThriftCodecModule;
import io.airlift.drift.protocol.TProtocolException;
import io.airlift.drift.protocol.TTransportException;
import io.airlift.drift.transport.DriftClientConfig;
import io.airlift.drift.transport.InvokeRequest;
import io.airlift.drift.transport.MethodInvokerFactory;
import org.testng.annotations.Test;

import javax.inject.Qualifier;

import java.lang.annotation.Annotation;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.google.common.base.Throwables.getCausalChain;
import static com.google.common.base.Throwables.getRootCause;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.drift.client.guice.DriftClientBinder.driftClientBinder;
import static io.airlift.drift.client.guice.MethodInvocationFilterBinder.staticFilterBinder;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;

public class TestDriftClient
{
    private static final ThriftCodecManager codecManager = new ThriftCodecManager();
    private static final Optional<String> ADDRESS_SELECTION_CONTEXT = Optional.of("addressSelectionContext");
    private static final ImmutableMap<String, String> HEADERS = ImmutableMap.of("key", "value");
    private static final Key<DriftClient<Client>> DEFAULT_CLIENT_KEY = Key.get(new TypeLiteral<DriftClient<Client>>() {});
    private static final Key<DriftClient<Client>> CUSTOM_CLIENT_KEY = Key.get(new TypeLiteral<DriftClient<Client>>() {}, CustomClient.class);

    private int invocationId;

    @Test
    public void testInvoker()
            throws Exception
    {
        ResultsSupplier resultsSupplier = new ResultsSupplier();
        MockMethodInvokerFactory<String> methodInvokerFactory = new MockMethodInvokerFactory<>(resultsSupplier);
        TestingMethodInvocationStatsFactory statsFactory = new TestingMethodInvocationStatsFactory();

        DriftClientFactoryManager<String> clientFactoryManager = new DriftClientFactoryManager<>(codecManager, methodInvokerFactory, statsFactory);
        DriftClientFactory driftClientFactory = clientFactoryManager.createDriftClientFactory("clientIdentity", new MockAddressSelector());

        DriftClient<Client> driftClient = driftClientFactory.createDriftClient(Client.class);
        Client client = driftClient.get(ADDRESS_SELECTION_CONTEXT, HEADERS);
        assertEquals(methodInvokerFactory.getClientIdentity(), "clientIdentity");

        testClient(resultsSupplier, ImmutableList.of(methodInvokerFactory.getMethodInvoker()), statsFactory, client, Optional.empty());
    }

    @Test
    public void testFilter()
            throws Exception
    {
        ResultsSupplier resultsSupplier = new ResultsSupplier();
        PassThroughFilter passThroughFilter = new PassThroughFilter();
        ShortCircuitFilter shortCircuitFilter = new ShortCircuitFilter(resultsSupplier);

        MockMethodInvokerFactory<String> invokerFactory = new MockMethodInvokerFactory<>(resultsSupplier);
        TestingMethodInvocationStatsFactory statsFactory = new TestingMethodInvocationStatsFactory();
        DriftClientFactoryManager<String> clientFactoryManager = new DriftClientFactoryManager<>(codecManager, invokerFactory, statsFactory);
        DriftClientFactory driftClientFactory = clientFactoryManager.createDriftClientFactory("clientIdentity", new MockAddressSelector());

        DriftClient<Client> driftClient = driftClientFactory.createDriftClient(
                Client.class,
                Optional.empty(),
                ImmutableList.of(passThroughFilter, shortCircuitFilter),
                new DriftClientConfig());
        Client client = driftClient.get(ADDRESS_SELECTION_CONTEXT, HEADERS);
        assertEquals(invokerFactory.getClientIdentity(), "clientIdentity");

        testClient(resultsSupplier, ImmutableList.of(passThroughFilter, shortCircuitFilter), statsFactory, client, Optional.empty());
    }

    @Test
    public void testGuiceClient()
            throws Exception
    {
        TestingMethodInvocationStatsFactory statsFactory = new TestingMethodInvocationStatsFactory();
        ResultsSupplier resultsSupplier = new ResultsSupplier();
        MockMethodInvokerFactory<Annotation> invokerFactory = new MockMethodInvokerFactory<>(resultsSupplier);

        Bootstrap app = new Bootstrap(
                new ThriftCodecModule(),
                binder -> binder.bind(new TypeLiteral<MethodInvokerFactory<Annotation>>() {})
                        .toInstance(invokerFactory),
                binder -> newOptionalBinder(binder, MethodInvocationStatsFactory.class)
                        .setBinding()
                        .toInstance(statsFactory),
                binder -> driftClientBinder(binder)
                        .bindDriftClient(Client.class)
                        .withAddressSelector(new MockAddressSelector()),
                binder -> driftClientBinder(binder)
                        .bindDriftClient(Client.class, CustomClient.class)
                        .withAddressSelector(new MockAddressSelector()));

        LifeCycleManager lifeCycleManager = null;
        try {
            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .initialize();
            lifeCycleManager = injector.getInstance(LifeCycleManager.class);

            DriftClient<Client> driftClient = injector.getInstance(DEFAULT_CLIENT_KEY);
            assertSame(injector.getInstance(DEFAULT_CLIENT_KEY), driftClient);
            Client client = driftClient.get(ADDRESS_SELECTION_CONTEXT, HEADERS);
            testClient(resultsSupplier, ImmutableList.of(invokerFactory.getMethodInvoker()), statsFactory, client, Optional.empty());

            DriftClient<Client> customDriftClient = injector.getInstance(CUSTOM_CLIENT_KEY);
            assertSame(injector.getInstance(CUSTOM_CLIENT_KEY), customDriftClient);
            assertNotSame(driftClient, customDriftClient);
            Client customClient = customDriftClient.get(ADDRESS_SELECTION_CONTEXT, HEADERS);
            testClient(resultsSupplier, ImmutableList.of(invokerFactory.getMethodInvoker()), statsFactory, customClient, Optional.of(CustomClient.class.getSimpleName()));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        finally {
            if (lifeCycleManager != null) {
                try {
                    lifeCycleManager.stop();
                }
                catch (Exception ignored) {
                }
            }
        }
    }

    @Test
    public void testGuiceClientFilter()
            throws Exception
    {
        TestingMethodInvocationStatsFactory statsFactory = new TestingMethodInvocationStatsFactory();
        ResultsSupplier resultsSupplier = new ResultsSupplier();
        PassThroughFilter passThroughFilter = new PassThroughFilter();
        ShortCircuitFilter shortCircuitFilter = new ShortCircuitFilter(resultsSupplier);
        MockMethodInvokerFactory<Annotation> invokerFactory = new MockMethodInvokerFactory<>(resultsSupplier);

        Bootstrap app = new Bootstrap(
                new ThriftCodecModule(),
                binder -> binder.bind(new TypeLiteral<MethodInvokerFactory<Annotation>>() {})
                        .toInstance(invokerFactory),
                binder -> newOptionalBinder(binder, MethodInvocationStatsFactory.class)
                        .setBinding()
                        .toInstance(statsFactory),
                binder -> driftClientBinder(binder)
                        .bindDriftClient(Client.class)
                        .withAddressSelector(new MockAddressSelector())
                        .withMethodInvocationFilter(staticFilterBinder(passThroughFilter, shortCircuitFilter)),
                binder -> driftClientBinder(binder)
                        .bindDriftClient(Client.class, CustomClient.class)
                        .withAddressSelector(new MockAddressSelector())
                        .withMethodInvocationFilter(staticFilterBinder(passThroughFilter, shortCircuitFilter)));

        LifeCycleManager lifeCycleManager = null;
        try {
            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .initialize();
            lifeCycleManager = injector.getInstance(LifeCycleManager.class);

            DriftClient<Client> driftClient = injector.getInstance(DEFAULT_CLIENT_KEY);
            assertSame(injector.getInstance(DEFAULT_CLIENT_KEY), driftClient);
            Client client = driftClient.get(ADDRESS_SELECTION_CONTEXT, HEADERS);
            testClient(resultsSupplier, ImmutableList.of(passThroughFilter, shortCircuitFilter), statsFactory, client, Optional.empty());

            DriftClient<Client> customDriftClient = injector.getInstance(CUSTOM_CLIENT_KEY);
            assertSame(injector.getInstance(CUSTOM_CLIENT_KEY), customDriftClient);
            assertNotSame(driftClient, customDriftClient);
            Client customClient = customDriftClient.get(ADDRESS_SELECTION_CONTEXT, HEADERS);
            testClient(resultsSupplier, ImmutableList.of(passThroughFilter, shortCircuitFilter), statsFactory, customClient, Optional.of(CustomClient.class.getSimpleName()));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        finally {
            if (lifeCycleManager != null) {
                try {
                    lifeCycleManager.stop();
                }
                catch (Exception ignored) {
                }
            }
        }
    }

    private void testClient(
            ResultsSupplier resultsSupplier,
            List<Supplier<InvokeRequest>> targets,
            TestingMethodInvocationStatsFactory statsFactory,
            Client client, Optional<String> empty)
            throws Exception
    {
        // test built-in methods
        resultsSupplier.setFailedResult(new Throwable());
        assertEquals(client, client);
        assertEquals(client.hashCode(), client.hashCode());
        assertEquals(client.toString(), "clientService");

        // test normal invocation
        assertNormalInvocation(resultsSupplier, targets, statsFactory, client, empty);

        // test method throws TException
        assertExceptionInvocation(resultsSupplier, targets, statsFactory, client, empty, new ClientException());
        assertExceptionInvocation(resultsSupplier, targets, statsFactory, client, empty, new TException());
        assertExceptionInvocation(resultsSupplier, targets, statsFactory, client, empty, new TApplicationException());
        assertExceptionInvocation(resultsSupplier, targets, statsFactory, client, empty, new TTransportException());
        assertExceptionInvocation(resultsSupplier, targets, statsFactory, client, empty, new TProtocolException());
        assertExceptionInvocation(resultsSupplier, targets, statsFactory, client, empty, new Error());
        assertExceptionInvocation(resultsSupplier, targets, statsFactory, client, empty, new UnknownException(), TException.class);
        assertExceptionInvocation(resultsSupplier, targets, statsFactory, client, empty, new RuntimeException(), TException.class);
        assertExceptionInvocation(resultsSupplier, targets, statsFactory, client, empty, new InterruptedException(), TException.class);

        // custom exception subclasses
        assertExceptionInvocation(resultsSupplier, targets, statsFactory, client, empty, new ClientException() {});
        assertExceptionInvocation(resultsSupplier, targets, statsFactory, client, empty, new TException() {});
        assertExceptionInvocation(resultsSupplier, targets, statsFactory, client, empty, new TApplicationException() {});
        assertExceptionInvocation(resultsSupplier, targets, statsFactory, client, empty, new TTransportException() {});
        assertExceptionInvocation(resultsSupplier, targets, statsFactory, client, empty, new TProtocolException() {});

        // test method does not throw TException
        assertNoTExceptionInvocation(resultsSupplier, targets, statsFactory, client, empty, new ClientException());
        assertNoTExceptionInvocation(resultsSupplier, targets, statsFactory, client, empty, new TException(), RuntimeTException.class);
        assertNoTExceptionInvocation(resultsSupplier, targets, statsFactory, client, empty, new TApplicationException(), RuntimeTApplicationException.class);
        assertNoTExceptionInvocation(resultsSupplier, targets, statsFactory, client, empty, new TTransportException(), RuntimeTTransportException.class);
        assertNoTExceptionInvocation(resultsSupplier, targets, statsFactory, client, empty, new TProtocolException(), RuntimeTProtocolException.class);
        assertNoTExceptionInvocation(resultsSupplier, targets, statsFactory, client, empty, new Error());
        assertNoTExceptionInvocation(resultsSupplier, targets, statsFactory, client, empty, new UnknownException(), RuntimeTException.class, TException.class);
        assertNoTExceptionInvocation(resultsSupplier, targets, statsFactory, client, empty, new RuntimeException(), RuntimeTException.class, TException.class);
        assertNoTExceptionInvocation(resultsSupplier, targets, statsFactory, client, empty, new InterruptedException(), RuntimeTException.class, TException.class);

        // custom exception subclasses
        assertNoTExceptionInvocation(resultsSupplier, targets, statsFactory, client, empty, new ClientException() {});
        assertNoTExceptionInvocation(resultsSupplier, targets, statsFactory, client, empty, new TException() {}, RuntimeTException.class);
        assertNoTExceptionInvocation(resultsSupplier, targets, statsFactory, client, empty, new TApplicationException() {}, RuntimeTApplicationException.class);
        assertNoTExceptionInvocation(resultsSupplier, targets, statsFactory, client, empty, new TTransportException() {}, RuntimeTTransportException.class);
        assertNoTExceptionInvocation(resultsSupplier, targets, statsFactory, client, empty, new TProtocolException() {}, RuntimeTProtocolException.class);
    }

    private static void assertNormalInvocation(
            ResultsSupplier resultsSupplier,
            Collection<Supplier<InvokeRequest>> targets,
            TestingMethodInvocationStatsFactory statsFactory,
            Client client,
            Optional<String> qualifier)
            throws Exception
    {
        resultsSupplier.setSuccessResult("result");

        TestingMethodInvocationStat stat = statsFactory.getStat("clientService", qualifier, "test");
        stat.clear();
        int invocationId = ThreadLocalRandom.current().nextInt();
        assertEquals(client.test(invocationId, "normal"), "result");
        verifyMethodInvocation(targets, "test", invocationId, "normal");
        stat.assertSuccess(0);

        stat = statsFactory.getStat("clientService", qualifier, "testAsync");
        stat.clear();
        invocationId = ThreadLocalRandom.current().nextInt();
        assertEquals(client.testAsync(invocationId, "normal").get(), "result");
        verifyMethodInvocation(targets, "testAsync", invocationId, "normal");
        stat.assertSuccess(0);
    }

    @SafeVarargs
    private static void assertExceptionInvocation(
            ResultsSupplier resultsSupplier,
            Collection<Supplier<InvokeRequest>> targets,
            TestingMethodInvocationStatsFactory statsFactory,
            Client client,
            Optional<String> qualifier,
            Throwable testException,
            Class<? extends Throwable>... expectedWrapperTypes)
            throws InterruptedException
    {
        String name = "exception-" + testException.getClass().getName();

        TestingMethodInvocationStat stat = statsFactory.getStat("clientService", qualifier, "test");
        stat.clear();
        int invocationId = ThreadLocalRandom.current().nextInt();
        resultsSupplier.setFailedResult(testException);
        try {
            client.test(invocationId, name);
        }
        catch (Throwable e) {
            assertExceptionChain(e, testException, expectedWrapperTypes);
        }
        verifyMethodInvocation(targets, "test", invocationId, name);
        stat.assertFailure(0);

        stat = statsFactory.getStat("clientService", qualifier, "testAsync");
        stat.clear();
        invocationId = ThreadLocalRandom.current().nextInt();
        resultsSupplier.setFailedResult(testException);
        try {
            client.testAsync(invocationId, name).get();
        }
        catch (ExecutionException e) {
            assertExceptionChain(e.getCause(), testException, expectedWrapperTypes);
        }
        verifyMethodInvocation(targets, "testAsync", invocationId, name);
        stat.assertFailure(0);
    }

    @SafeVarargs
    private final void assertNoTExceptionInvocation(
            ResultsSupplier resultsSupplier,
            Collection<Supplier<InvokeRequest>> targets,
            TestingMethodInvocationStatsFactory statsFactory,
            Client client,
            Optional<String> qualifier,
            Throwable testException,
            Class<? extends Throwable>... expectedWrapperTypes)
    {
        String name = "exception-" + testException.getClass().getName();

        TestingMethodInvocationStat stat = statsFactory.getStat("clientService", qualifier, "testNoTException");
        stat.clear();
        resultsSupplier.setFailedResult(testException);
        try {
            invocationId++;
            client.testNoTException(invocationId, name);
        }
        catch (Throwable e) {
            assertExceptionChain(e, testException, expectedWrapperTypes);
        }
        verifyMethodInvocation(targets, "testNoTException", invocationId, name);
        stat.assertFailure(0);
    }

    private static void assertExceptionChain(Throwable actualException, Throwable expectedException, Class<? extends Throwable>[] expectedWrapperTypes)
    {
        assertSame(getRootCause(actualException), expectedException);

        List<Class<?>> actualTypes = getCausalChain(actualException).stream()
                .map(Object::getClass)
                .collect(Collectors.toList());
        List<Class<?>> expectedTypes = ImmutableList.<Class<?>>builder()
                .add(expectedWrapperTypes)
                .add(expectedException.getClass())
                .build();
        if (!actualException.equals(expectedException)) {
            assertEquals(actualTypes.toString(), expectedTypes.toString());
        }

        // if we tested an interrupted exception, clear the thread interrupted flag
        if (expectedException instanceof InterruptedException) {
            Thread.interrupted();
        }
    }

    private static void verifyMethodInvocation(Collection<Supplier<InvokeRequest>> targets, String methodName, int id, String name)
    {
        for (Supplier<InvokeRequest> target : targets) {
            InvokeRequest invokeRequest = target.get();
            assertEquals(invokeRequest.getMethod().getName(), methodName);
            assertEquals(invokeRequest.getParameters(), ImmutableList.of(id, name));
            assertEquals(invokeRequest.getHeaders(), HEADERS);
        }
    }

    @ThriftService("clientService")
    public interface Client
    {
        @ThriftMethod
        String test(int id, String name)
                throws ClientException, TException;

        @ThriftMethod
        void testNoTException(int id, String name)
                throws ClientException;

        @ThriftMethod(exception = @ThriftException(id = 0, type = ClientException.class))
        ListenableFuture<String> testAsync(int id, String name);
    }

    @Target({FIELD, PARAMETER, METHOD})
    @Retention(RUNTIME)
    @Qualifier
    private @interface CustomClient {}

    @ThriftStruct
    public static class ClientException
            extends Exception
    {
    }

    private static class UnknownException
            extends Exception
    {
    }
}
