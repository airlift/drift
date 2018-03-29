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

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.drift.integration.guice.EchoService.EmptyOptionalException;
import io.airlift.drift.integration.guice.EchoService.NullValueException;
import io.airlift.drift.integration.scribe.drift.DriftLogEntry;
import io.airlift.drift.transport.netty.client.DriftNettyClientModule;
import io.airlift.drift.transport.netty.server.DriftNettyServerModule;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;

import static io.airlift.drift.client.address.SimpleAddressSelectorBinder.simpleAddressSelector;
import static io.airlift.drift.client.guice.DriftClientBinder.driftClientBinder;
import static io.airlift.drift.server.guice.DriftServerBinder.driftServerBinder;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class TestGuiceIntegration
{
    @Test
    public void test()
            throws Exception
    {
        int port = findUnusedPort();

        Bootstrap bootstrap = new Bootstrap(
                new DriftNettyServerModule(),
                new DriftNettyClientModule(),
                binder -> {
                    driftServerBinder(binder).bindService(EchoServiceHandler.class);
                    driftServerBinder(binder).bindService(MismatchServiceHandler.class);
                    driftClientBinder(binder).bindDriftClient(EchoService.class).withAddressSelector(simpleAddressSelector());
                    driftClientBinder(binder).bindDriftClient(MismatchService.class).withAddressSelector(simpleAddressSelector());
                });

        Injector injector = bootstrap
                .strictConfig()
                .setRequiredConfigurationProperty("thrift.server.port", String.valueOf(port))
                .setRequiredConfigurationProperty("echo.thrift.client.addresses", "localhost:" + port)
                .setRequiredConfigurationProperty("mismatch.thrift.client.addresses", "localhost:" + port)
                .doNotInitializeLogging()
                .initialize();

        LifeCycleManager lifeCycleManager = injector.getInstance(LifeCycleManager.class);
        EchoService echoService = injector.getInstance(EchoService.class);
        MismatchService mismatchService = injector.getInstance(MismatchService.class);

        try {
            assertEchoService(echoService);

            assertEquals(mismatchService.extraClientArgs(123, 456), 123);
            assertEquals(mismatchService.extraServerArgs(), 42);
        }
        finally {
            lifeCycleManager.stop();
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

    private static int findUnusedPort()
            throws IOException
    {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
