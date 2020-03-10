/*
 * Copyright (C) 2018 Facebook, Inc.
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
package io.airlift.drift.integration.guice;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.drift.TApplicationException;
import io.airlift.drift.TException;
import io.airlift.drift.client.ExceptionClassification;
import io.airlift.drift.client.RetriesFailedException;
import io.airlift.drift.integration.guice.EchoService.EmptyOptionalException;
import io.airlift.drift.integration.guice.EchoService.NullValueException;
import io.airlift.drift.integration.scribe.drift.DriftLogEntry;
import io.airlift.drift.transport.client.MessageTooLargeException;
import io.airlift.drift.transport.netty.buffer.TestingPooledByteBufAllocator;
import io.airlift.drift.transport.netty.client.DriftNettyClientModule;
import io.airlift.drift.transport.netty.server.DriftNettyServerModule;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;

import static com.google.common.util.concurrent.Futures.getUnchecked;
import static io.airlift.drift.client.ExceptionClassification.HostStatus.NORMAL;
import static io.airlift.drift.client.ExceptionClassification.NORMAL_EXCEPTION;
import static io.airlift.drift.client.address.SimpleAddressSelectorBinder.simpleAddressSelector;
import static io.airlift.drift.client.guice.DriftClientBinder.driftClientBinder;
import static io.airlift.drift.integration.guice.ThrowingService.MAX_FRAME_SIZE;
import static io.airlift.drift.server.guice.DriftServerBinder.driftServerBinder;
import static java.lang.Math.toIntExact;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Arrays.fill;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestGuiceIntegration
{
    @Test
    public void testWithoutPooling()
            throws Exception
    {
        test(false);
    }

    @Test
    public void testWithPooling()
            throws Exception
    {
        test(true);
    }

    private static void test(boolean pooling)
            throws Exception
    {
        int port = findUnusedPort();

        TestingPooledByteBufAllocator testingAllocator = new TestingPooledByteBufAllocator();
        Bootstrap bootstrap = new Bootstrap(
                new DriftNettyServerModule(testingAllocator),
                new DriftNettyClientModule(testingAllocator),
                binder -> {
                    binder.bind(ThrowingServiceHandler.class).in(Scopes.SINGLETON);
                    driftServerBinder(binder).bindService(EchoServiceHandler.class);
                    driftServerBinder(binder).bindService(MismatchServiceHandler.class);
                    driftServerBinder(binder).bindService(ThrowingServiceHandler.class);
                    driftClientBinder(binder).bindDriftClient(EchoService.class).withAddressSelector(simpleAddressSelector());
                    driftClientBinder(binder).bindDriftClient(MismatchService.class).withAddressSelector(simpleAddressSelector());
                    driftClientBinder(binder).bindDriftClient(ThrowingService.class).withAddressSelector(simpleAddressSelector())
                            .withExceptionClassifier(t -> {
                                if (t instanceof ExampleException) {
                                    boolean retryable = ((ExampleException) t).isRetryable();
                                    return new ExceptionClassification(Optional.of(retryable), NORMAL);
                                }
                                return NORMAL_EXCEPTION;
                            });
                });

        Injector injector = bootstrap
                .strictConfig()
                .setRequiredConfigurationProperty("thrift.server.port", String.valueOf(port))
                .setRequiredConfigurationProperty("thrift.server.max-frame-size", MAX_FRAME_SIZE.toString())
                .setRequiredConfigurationProperty("thrift.client.connection-pool.enabled", String.valueOf(pooling))
                .setRequiredConfigurationProperty("echo.thrift.client.addresses", "localhost:" + port)
                .setRequiredConfigurationProperty("mismatch.thrift.client.addresses", "localhost:" + port)
                .setRequiredConfigurationProperty("throwing.thrift.client.addresses", "localhost:" + port)
                .setRequiredConfigurationProperty("throwing.thrift.client.min-backoff-delay", "1ms")
                .setRequiredConfigurationProperty("throwing.thrift.client.backoff-scale-factor", "1.0")
                .setRequiredConfigurationProperty("throwing.thrift.client.max-frame-size", MAX_FRAME_SIZE.toString())
                .doNotInitializeLogging()
                .initialize();

        LifeCycleManager lifeCycleManager = injector.getInstance(LifeCycleManager.class);
        EchoService echoService = injector.getInstance(EchoService.class);
        MismatchService mismatchService = injector.getInstance(MismatchService.class);
        ThrowingService throwingService = injector.getInstance(ThrowingService.class);
        ThrowingServiceHandler throwingServiceHandler = injector.getInstance(ThrowingServiceHandler.class);

        try {
            assertEchoService(echoService);

            assertEquals(mismatchService.extraClientArgs(123, 456), 123);
            assertEquals(mismatchService.extraServerArgs(), 42);

            assertExceptionClassifier(throwingService);

            assertAnnotatedException(throwingService);

            assertLargeMessage(throwingService, throwingServiceHandler);
        }
        finally {
            lifeCycleManager.stop();
            testingAllocator.close();
        }
    }

    @SuppressWarnings("OptionalAssignedToNull")
    private static void assertEchoService(EchoService service)
            throws NullValueException, EmptyOptionalException
    {
        service.echoVoid();

        assertFalse(service.echoBoolean(false));
        assertTrue(service.echoBoolean(true));

        assertEquals(service.echoByte((byte) 42), 42);
        assertEquals(service.echoByte((byte) 0xAB), (byte) 0xAB);
        assertEquals(service.echoByte(Byte.MIN_VALUE), Byte.MIN_VALUE);
        assertEquals(service.echoByte(Byte.MAX_VALUE), Byte.MAX_VALUE);

        assertEquals(service.echoShort((short) 1234), 1234);
        assertEquals(service.echoShort((short) 0xDEAD), (short) 0xDEAD);
        assertEquals(service.echoShort(Short.MIN_VALUE), Short.MIN_VALUE);
        assertEquals(service.echoShort(Short.MAX_VALUE), Short.MAX_VALUE);

        assertEquals(service.echoInt(123_456), 123_456);
        assertEquals(service.echoInt(0xDEADBEEF), 0xDEADBEEF);
        assertEquals(service.echoInt(Integer.MIN_VALUE), Integer.MIN_VALUE);
        assertEquals(service.echoInt(Integer.MAX_VALUE), Integer.MAX_VALUE);

        assertEquals(service.echoLong(9_876_543_210L), 9_876_543_210L);
        assertEquals(service.echoLong(0xDEADBEEF_CAFEBABEL), 0xDEADBEEF_CAFEBABEL);
        assertEquals(service.echoLong(Long.MIN_VALUE), Long.MIN_VALUE);
        assertEquals(service.echoLong(Long.MAX_VALUE), Long.MAX_VALUE);

        assertEquals(service.echoDouble(123.456), 123.456);
        assertEquals(service.echoDouble(-456.123), -456.123);
        assertEquals(service.echoDouble(0.0), 0.0);
        assertEquals(service.echoDouble(-0.0), -0.0);
        assertEquals(service.echoDouble(Double.NEGATIVE_INFINITY), Double.NEGATIVE_INFINITY);
        assertEquals(service.echoDouble(Double.POSITIVE_INFINITY), Double.POSITIVE_INFINITY);
        assertEquals(service.echoDouble(Double.NaN), Double.NaN);

        assertEquals(service.echoString("hello"), "hello");
        assertThrows(NullValueException.class, () -> service.echoString(null));

        assertEquals(service.echoBinary("hello".getBytes(UTF_8)), "hello".getBytes(UTF_8));
        assertThrows(NullValueException.class, () -> service.echoBinary(null));

        DriftLogEntry logEntry = new DriftLogEntry("abc", "xyz");
        assertEquals(service.echoStruct(logEntry), logEntry);
        assertThrows(NullValueException.class, () -> service.echoStruct(null));

        assertEquals(service.echoInteger(123_456), (Integer) 123_456);
        assertThrows(NullValueException.class, () -> service.echoInteger(null));

        assertEquals(service.echoListInteger(asList(123_456, 42)), asList(123_456, 42));
        assertThrows(NullValueException.class, () -> service.echoListInteger(null));

        assertEquals(service.echoListString(asList("hello", "world")), asList("hello", "world"));
        assertThrows(NullValueException.class, () -> service.echoListInteger(null));

        assertEquals(service.echoOptionalInt(OptionalInt.of(123_456)), 123_456);
        assertThrows(EmptyOptionalException.class, () -> service.echoOptionalInt(OptionalInt.empty()));
        assertThrows(EmptyOptionalException.class, () -> service.echoOptionalInt(null));

        assertEquals(service.echoOptionalLong(OptionalLong.of(9_876_543_210L)), 9_876_543_210L);
        assertThrows(EmptyOptionalException.class, () -> service.echoOptionalLong(OptionalLong.empty()));
        assertThrows(EmptyOptionalException.class, () -> service.echoOptionalLong(null));

        assertEquals(service.echoOptionalDouble(OptionalDouble.of(123.456)), 123.456);
        assertThrows(EmptyOptionalException.class, () -> service.echoOptionalDouble(OptionalDouble.empty()));
        assertThrows(EmptyOptionalException.class, () -> service.echoOptionalDouble(null));

        assertEquals(service.echoOptionalString(Optional.of("hello")), "hello");
        assertThrows(EmptyOptionalException.class, () -> service.echoOptionalString(Optional.empty()));
        assertThrows(EmptyOptionalException.class, () -> service.echoOptionalString(null));

        assertEquals(service.echoOptionalStruct(Optional.of(logEntry)), logEntry);
        assertThrows(EmptyOptionalException.class, () -> service.echoOptionalStruct(Optional.empty()));
        assertThrows(EmptyOptionalException.class, () -> service.echoOptionalStruct(null));

        assertEquals(service.echoOptionalListInteger(Optional.of(asList(123_456, 42))), asList(123_456, 42));
        assertThrows(EmptyOptionalException.class, () -> service.echoOptionalListInteger(Optional.empty()));
        assertThrows(EmptyOptionalException.class, () -> service.echoOptionalListInteger(null));

        assertEquals(service.echoOptionalListString(Optional.of(asList("hello", "world"))), asList("hello", "world"));
        assertThrows(EmptyOptionalException.class, () -> service.echoOptionalListString(Optional.empty()));
        assertThrows(EmptyOptionalException.class, () -> service.echoOptionalListString(null));
    }

    private static void assertExceptionClassifier(ThrowingService service)
    {
        assertThatThrownBy(() -> service.fail("no-retry", false))
                .hasMessage("no-retry")
                .isInstanceOfSatisfying(ExampleException.class, e -> {
                    assertThat(e.isRetryable()).isFalse();
                    assertThat(e.getSuppressed()).hasOnlyOneElementSatisfying(s ->
                            assertThat(s).isInstanceOf(RetriesFailedException.class)
                                    .hasMessageContaining("Non-retryable exception")
                                    .hasMessageContaining("invocationAttempts: 1,"));
                });

        assertThatThrownBy(() -> service.fail("can-retry", true))
                .hasMessage("can-retry")
                .isInstanceOfSatisfying(ExampleException.class, e -> {
                    assertThat(e.isRetryable()).isTrue();
                    assertThat(e.getSuppressed()).hasOnlyOneElementSatisfying(s ->
                            assertThat(s).isInstanceOf(RetriesFailedException.class)
                                    .hasMessageContaining("Max retry attempts (5) exceeded")
                                    .hasMessageContaining("invocationAttempts: 6,"));
                });
    }

    private static void assertAnnotatedException(ThrowingService service)
    {
        assertThatThrownBy(() -> service.failWithException(true))
                .hasMessage("RETRY")
                .isInstanceOfSatisfying(RetryableException.class, e ->
                        assertThat(e.getSuppressed()).hasOnlyOneElementSatisfying(s ->
                                assertThat(s).isInstanceOf(RetriesFailedException.class)
                                        .hasMessageContaining("Max retry attempts (5) exceeded")
                                        .hasMessageContaining("invocationAttempts: 6,")));

        assertThatThrownBy(() -> service.failWithException(false))
                .hasMessage("NO RETRY")
                .isInstanceOfSatisfying(NonRetryableException.class, e ->
                        assertThat(e.getSuppressed()).hasOnlyOneElementSatisfying(s ->
                                assertThat(s).isInstanceOf(RetriesFailedException.class)
                                        .hasMessageContaining("Non-retryable exception")
                                        .hasMessageContaining("invocationAttempts: 1,")));
    }

    private static void assertLargeMessage(ThrowingService service, ThrowingServiceHandler handler)
    {
        // make sure requests work after sending and receiving too large frame
        receiveTooLargeMessage(service);
        sendTooLargeMessage(service);

        // test that too large frame failures doesn't cause the failure of other requests on the same channel
        ListenableFuture<String> awaitFuture = service.await();
        getUnchecked(handler.waitForAwait());
        assertFalse(awaitFuture.isDone());
        receiveTooLargeMessage(service);
        assertFalse(awaitFuture.isDone());
        assertEquals(service.release(), "OK");
        assertEquals(getUnchecked(awaitFuture), "OK");

        awaitFuture = service.await();
        getUnchecked(handler.waitForAwait());
        assertFalse(awaitFuture.isDone());
        sendTooLargeMessage(service);
        assertFalse(awaitFuture.isDone());
        assertEquals(service.release(), "OK");
        assertEquals(getUnchecked(awaitFuture), "OK");
    }

    private static void receiveTooLargeMessage(ThrowingService service)
    {
        try {
            service.generateTooLargeFrame();
            fail("expected exception");
        }
        catch (TException e) {
            assertThat(e).isInstanceOf(MessageTooLargeException.class)
                    .hasMessageMatching("Frame size .+ exceeded max size .+");
            assertEquals(e.getSuppressed().length, 1);
            Throwable t = e.getSuppressed()[0];
            assertThat(t).isInstanceOf(RetriesFailedException.class)
                    .hasMessageContaining("Non-retryable exception")
                    .hasMessageContaining("invocationAttempts: 1,");
        }
    }

    private static void sendTooLargeMessage(ThrowingService service)
    {
        byte[] data = new byte[toIntExact(MAX_FRAME_SIZE.toBytes()) + 1];
        fill(data, (byte) 0xAB);
        try {
            service.acceptBytes(data);
            fail("expected exception");
        }
        catch (TException e) {
            assertThat(e).isInstanceOf(TApplicationException.class)
                    .hasMessageMatching("Frame size .+ exceeded max size .+");
        }
    }

    private static int findUnusedPort()
            throws IOException
    {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
