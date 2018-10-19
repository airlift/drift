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
package io.airlift.drift.transport.netty.client;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.drift.TApplicationException;
import io.airlift.drift.codec.ThriftCodec;
import io.airlift.drift.codec.ThriftCodecManager;
import io.airlift.drift.codec.internal.builtin.VoidThriftCodec;
import io.airlift.drift.codec.metadata.ThriftType;
import io.airlift.drift.transport.MethodMetadata;
import io.airlift.drift.transport.ParameterMetadata;
import io.airlift.drift.transport.client.InvokeRequest;
import io.airlift.drift.transport.client.MethodInvoker;
import io.airlift.drift.transport.netty.buffer.TestingPooledByteBufAllocator;
import io.airlift.drift.transport.netty.client.ConnectionManager.ConnectionParameters;
import io.airlift.drift.transport.netty.codec.Protocol;
import io.airlift.drift.transport.netty.codec.Transport;
import io.airlift.drift.transport.netty.scribe.apache.LogEntry;
import io.airlift.drift.transport.netty.scribe.apache.ResultCode;
import io.airlift.drift.transport.netty.scribe.apache.ScribeService;
import io.airlift.drift.transport.netty.scribe.apache.scribe;
import io.airlift.drift.transport.netty.scribe.apache.scribe.AsyncClient.Log_call;
import io.airlift.drift.transport.netty.scribe.apache.scribe.Client;
import io.airlift.drift.transport.netty.scribe.drift.DriftLogEntry;
import io.airlift.drift.transport.netty.scribe.drift.DriftResultCode;
import io.airlift.drift.transport.netty.server.DriftNettyServerConfig;
import io.airlift.drift.transport.netty.server.DriftNettyServerTransport;
import io.airlift.drift.transport.netty.server.DriftNettyServerTransportFactory;
import io.airlift.drift.transport.server.ServerInvokeRequest;
import io.airlift.drift.transport.server.ServerMethodInvoker;
import io.airlift.drift.transport.server.ServerTransport;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import io.netty.channel.Channel;
import io.netty.util.concurrent.DefaultEventExecutor;
import io.netty.util.concurrent.Future;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportFactory;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Lists.newArrayList;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.drift.TApplicationException.Type.UNSUPPORTED_CLIENT_TYPE;
import static io.airlift.drift.codec.metadata.ThriftType.list;
import static io.airlift.drift.codec.metadata.ThriftType.optional;
import static io.airlift.drift.transport.netty.codec.Protocol.BINARY;
import static io.airlift.drift.transport.netty.codec.Transport.FRAMED;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static java.util.Collections.nCopies;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestDriftNettyMethodInvoker
{
    private static final ThriftCodecManager CODEC_MANAGER = new ThriftCodecManager();

    private static final MethodMetadata LOG_METHOD_METADATA = new MethodMetadata(
            "Log",
            ImmutableList.of(new ParameterMetadata(
                    (short) 1,
                    "messages",
                    (ThriftCodec<Object>) CODEC_MANAGER.getCodec(list(CODEC_MANAGER.getCodec(DriftLogEntry.class).getType())))),
            (ThriftCodec<Object>) (Object) CODEC_MANAGER.getCodec(DriftResultCode.class),
            ImmutableMap.of(),
            false,
            true);

    private static final List<LogEntry> MESSAGES = ImmutableList.of(
            new LogEntry("hello", "world"),
            new LogEntry("bye", "world"));
    private static final List<DriftLogEntry> DRIFT_MESSAGES = ImmutableList.copyOf(
            MESSAGES.stream()
                    .map(input -> new DriftLogEntry(input.category, input.message))
                    .collect(Collectors.toList()));
    private static final DriftResultCode DRIFT_OK = DriftResultCode.OK;

    @Test
    public void testThriftService()
            throws Exception
    {
        ScribeService scribeService = new ScribeService();
        TProcessor processor = new scribe.Processor<>(scribeService);

        List<LogEntry> expectedMessages = testProcessor(processor);
        assertEquals(scribeService.getMessages(), expectedMessages);
    }

    private static List<LogEntry> testProcessor(TProcessor processor)
            throws Exception
    {
        int invocationCount = testProcessor(processor, ImmutableList.of(
                address -> logThrift(address, MESSAGES, new TFramedTransport.Factory(), new TBinaryProtocol.Factory()),
                address -> logThriftAsync(address, MESSAGES),
                address -> logNiftyInvocationHandlerOptional(address, DRIFT_MESSAGES),
                address -> logNiftyInvocationHandler(address, DRIFT_MESSAGES, FRAMED, BINARY)));

        return newArrayList(concat(nCopies(invocationCount, MESSAGES)));
    }

    private static int testProcessor(TProcessor processor, List<ToIntFunction<HostAndPort>> clients)
            throws Exception
    {
        try (TServerSocket serverTransport = new TServerSocket(0)) {
            TProtocolFactory protocolFactory = new TBinaryProtocol.Factory();
            TTransportFactory transportFactory = new TFramedTransport.Factory();
            TServer server = new TSimpleServer(new Args(serverTransport)
                    .protocolFactory(protocolFactory)
                    .transportFactory(transportFactory)
                    .processor(processor));

            Thread serverThread = new Thread(server::serve);
            try {
                serverThread.start();

                int localPort = serverTransport.getServerSocket().getLocalPort();
                HostAndPort address = HostAndPort.fromParts("localhost", localPort);

                int sum = 0;
                for (ToIntFunction<HostAndPort> client : clients) {
                    sum += client.applyAsInt(address);
                }
                return sum;
            }
            finally {
                server.stop();
                serverThread.interrupt();
            }
        }
    }

    @Test
    public void testDriftNettyService()
    {
        TestServerMethodInvoker methodInvoker = new TestServerMethodInvoker();
        List<DriftLogEntry> expectedMessages = testMethodInvoker(methodInvoker);
        assertEquals(ImmutableList.copyOf(methodInvoker.getMessages()), expectedMessages);
    }

    private static List<DriftLogEntry> testMethodInvoker(ServerMethodInvoker methodInvoker)
    {
        int invocationCount = testMethodInvoker(methodInvoker, ImmutableList.of(
                address -> logThrift(address, MESSAGES, new TTransportFactory(), new TBinaryProtocol.Factory()),
                address -> logThrift(address, MESSAGES, new TTransportFactory(), new TCompactProtocol.Factory()),
                address -> logThrift(address, MESSAGES, new TFramedTransport.Factory(), new TBinaryProtocol.Factory()),
                address -> logThrift(address, MESSAGES, new TFramedTransport.Factory(), new TCompactProtocol.Factory()),
                address -> logThriftAsync(address, MESSAGES),
                address -> logNiftyInvocationHandler(address, DRIFT_MESSAGES, Transport.UNFRAMED, BINARY),
                address -> logNiftyInvocationHandler(address, DRIFT_MESSAGES, Transport.UNFRAMED, Protocol.COMPACT),
                address -> logNiftyInvocationHandler(address, DRIFT_MESSAGES, Transport.UNFRAMED, Protocol.FB_COMPACT),
                address -> logNiftyInvocationHandler(address, DRIFT_MESSAGES, FRAMED, BINARY),
                address -> logNiftyInvocationHandler(address, DRIFT_MESSAGES, FRAMED, Protocol.COMPACT),
                address -> logNiftyInvocationHandler(address, DRIFT_MESSAGES, FRAMED, Protocol.FB_COMPACT),
                address -> logNiftyInvocationHandler(address, DRIFT_MESSAGES, Transport.HEADER, BINARY),
                address -> logNiftyInvocationHandler(address, DRIFT_MESSAGES, Transport.HEADER, Protocol.FB_COMPACT)));

        return newArrayList(concat(nCopies(invocationCount, DRIFT_MESSAGES)));
    }

    private static int testMethodInvoker(ServerMethodInvoker methodInvoker, List<ToIntFunction<HostAndPort>> clients)
    {
        TestingPooledByteBufAllocator testingAllocator = new TestingPooledByteBufAllocator();
        ServerTransport serverTransport = new DriftNettyServerTransportFactory(new DriftNettyServerConfig(), testingAllocator).createServerTransport(methodInvoker);
        try {
            serverTransport.start();

            HostAndPort address = HostAndPort.fromParts("localhost", ((DriftNettyServerTransport) serverTransport).getPort());

            int sum = 0;
            for (ToIntFunction<HostAndPort> client : clients) {
                sum += client.applyAsInt(address);
            }
            return sum;
        }
        finally {
            serverTransport.shutdown();
            testingAllocator.close();
        }
    }

    private static int logThrift(HostAndPort address, List<LogEntry> messages, TTransportFactory framingFactory, TProtocolFactory protocolFactory)
    {
        try {
            TSocket socket = new TSocket(address.getHost(), address.getPort());
            socket.open();
            try {
                TProtocol tp = protocolFactory.getProtocol(framingFactory.getTransport(socket));
                Client client = new Client(tp);
                assertEquals(client.Log(messages), ResultCode.OK);

                try {
                    client.Log(ImmutableList.of(new LogEntry("exception", "test")));
                    fail("Expected exception");
                }
                catch (org.apache.thrift.TApplicationException e) {
                    assertEquals(e.getType(), org.apache.thrift.TApplicationException.INTERNAL_ERROR);
                }
            }
            finally {
                socket.close();
            }
        }
        catch (TException e) {
            throw new RuntimeException(e);
        }
        return 1;
    }

    private static int logThriftAsync(HostAndPort address, List<LogEntry> messages)
    {
        try {
            TAsyncClientManager asyncClientManager = new TAsyncClientManager();
            try (TNonblockingSocket socket = new TNonblockingSocket(address.getHost(), address.getPort())) {
                scribe.AsyncClient client = new scribe.AsyncClient(new TBinaryProtocol.Factory(), asyncClientManager, socket);

                SettableFuture<ResultCode> futureResult = SettableFuture.create();
                client.Log(messages, new AsyncMethodCallback<Log_call>()
                {
                    @Override
                    public void onComplete(Log_call response)
                    {
                        try {
                            futureResult.set(response.getResult());
                        }
                        catch (Throwable exception) {
                            futureResult.setException(exception);
                        }
                    }

                    @Override
                    public void onError(Exception exception)
                    {
                        futureResult.setException(exception);
                    }
                });
                assertEquals(futureResult.get(), ResultCode.OK);
            }
            finally {
                asyncClientManager.stop();
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        return 1;
    }

    private static int logNiftyInvocationHandler(HostAndPort address, List<DriftLogEntry> entries, Transport transport, Protocol protocol)
    {
        DriftNettyClientConfig config = new DriftNettyClientConfig()
                .setTransport(transport)
                .setProtocol(protocol);

        try (TestingPooledByteBufAllocator testingAllocator = new TestingPooledByteBufAllocator();
                DriftNettyMethodInvokerFactory<Void> methodInvokerFactory = new DriftNettyMethodInvokerFactory<>(
                        new DriftNettyConnectionFactoryConfig(),
                        clientIdentity -> config,
                        testingAllocator)) {
            MethodInvoker methodInvoker = methodInvokerFactory.createMethodInvoker(null);

            ListenableFuture<Object> future = methodInvoker.invoke(new InvokeRequest(LOG_METHOD_METADATA, () -> address, ImmutableMap.of(), ImmutableList.of(entries)));
            assertEquals(future.get(), DRIFT_OK);

            try {
                future = methodInvoker.invoke(new InvokeRequest(LOG_METHOD_METADATA, () -> address, ImmutableMap.of(), ImmutableList.of(ImmutableList.of(new DriftLogEntry("exception", "test")))));
                assertEquals(future.get(), DRIFT_OK);
                fail("Expected exception");
            }
            catch (ExecutionException e) {
                Throwable cause = e.getCause();
                assertInstanceOf(cause, TApplicationException.class);
                TApplicationException applicationException = (TApplicationException) cause;
                assertEquals(applicationException.getTypeValue(), TApplicationException.Type.INTERNAL_ERROR.getType());
            }
            return 1;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testTimeout()
            throws Exception
    {
        ScheduledExecutorService executor = newSingleThreadScheduledExecutor(daemonThreadsNamed("test-timeout"));

        DriftNettyMethodInvoker invoker = new DriftNettyMethodInvoker(
                new ConnectionParameters(
                        FRAMED,
                        BINARY,
                        new DataSize(16, Unit.MEGABYTE),
                        new Duration(11, MILLISECONDS),
                        new Duration(13, MILLISECONDS),
                        Optional.empty(),
                        Optional.empty()),
                new HangingConnectionManager(),
                executor,
                new Duration(17, MILLISECONDS));

        ListenableFuture<Object> response = invoker.invoke(new InvokeRequest(
                new MethodMetadata(
                        "test",
                        ImmutableList.of(),
                        (ThriftCodec<Object>) (Object) new VoidThriftCodec(),
                        ImmutableMap.of(),
                        false,
                        true),
                () -> HostAndPort.fromParts("localhost", 1234),
                ImmutableMap.of(),
                ImmutableList.of()));

        try {
            response.get();
            fail("expected exception");
        }
        catch (ExecutionException e) {
            assertInstanceOf(e.getCause(), io.airlift.drift.TException.class);
            assertEquals(e.getCause().getMessage(), "Invocation response future did not complete after 41.00ms");
        }
        finally {
            executor.shutdown();
        }
    }

    private static int logNiftyInvocationHandlerOptional(HostAndPort address, List<DriftLogEntry> entries)
    {
        DriftNettyClientConfig config = new DriftNettyClientConfig();
        try (TestingPooledByteBufAllocator testingAllocator = new TestingPooledByteBufAllocator();
                DriftNettyMethodInvokerFactory<Void> methodInvokerFactory = new DriftNettyMethodInvokerFactory<>(
                        new DriftNettyConnectionFactoryConfig(),
                        clientIdentity -> config,
                        testingAllocator)) {
            MethodInvoker methodInvoker = methodInvokerFactory.createMethodInvoker(null);

            ThriftType optionalType = optional(list(CODEC_MANAGER.getCatalog().getThriftType(DriftLogEntry.class)));
            ParameterMetadata parameter = new ParameterMetadata(
                    (short) 1,
                    "messages",
                    (ThriftCodec<Object>) CODEC_MANAGER.getCodec(optionalType));

            MethodMetadata methodMetadata = new MethodMetadata(
                    "Log",
                    ImmutableList.of(parameter),
                    (ThriftCodec<Object>) (Object) CODEC_MANAGER.getCodec(DriftResultCode.class),
                    ImmutableMap.of(),
                    false,
                    true);

            ListenableFuture<Object> future = methodInvoker.invoke(new InvokeRequest(methodMetadata, () -> address, ImmutableMap.of(), ImmutableList.of(Optional.of(entries))));
            assertEquals(future.get(), DRIFT_OK);

            future = methodInvoker.invoke(new InvokeRequest(methodMetadata, () -> address, ImmutableMap.of(), ImmutableList.of(Optional.empty())));
            assertEquals(future.get(), DRIFT_OK);

            try {
                future = methodInvoker.invoke(new InvokeRequest(
                        methodMetadata,
                        () -> address,
                        ImmutableMap.of(),
                        ImmutableList.of(Optional.of(ImmutableList.of(new DriftLogEntry("exception", "test"))))));
                assertEquals(future.get(), DRIFT_OK);
            }
            catch (ExecutionException e) {
                Throwable cause = e.getCause();
                assertInstanceOf(cause, io.airlift.drift.TApplicationException.class);
                io.airlift.drift.TApplicationException applicationException = (io.airlift.drift.TApplicationException) cause;
                assertEquals(applicationException.getTypeValue(), io.airlift.drift.TApplicationException.Type.INTERNAL_ERROR.getType());
            }
            return 1;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static class HangingConnectionManager
            implements ConnectionManager
    {
        @Override
        public Future<Channel> getConnection(ConnectionParameters connectionParameters, HostAndPort address)
        {
            return new DefaultEventExecutor().newPromise();
        }

        @Override
        public void returnConnection(Channel connection)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {}
    }

    private static class TestServerMethodInvoker
            implements ServerMethodInvoker
    {
        private final List<DriftLogEntry> messages = new CopyOnWriteArrayList<>();

        private List<DriftLogEntry> getMessages()
        {
            return messages;
        }

        @Override
        public Optional<MethodMetadata> getMethodMetadata(String name)
        {
            if (LOG_METHOD_METADATA.getName().equals(name)) {
                return Optional.of(LOG_METHOD_METADATA);
            }
            return Optional.empty();
        }

        @Override
        public ListenableFuture<Object> invoke(ServerInvokeRequest request)
        {
            MethodMetadata method = request.getMethod();
            if (!LOG_METHOD_METADATA.getName().equals(method.getName())) {
                return Futures.immediateFailedFuture(new IllegalArgumentException("unknown method " + method));
            }

            Map<Short, Object> parameters = request.getParameters();
            if (parameters.size() != 1 || !parameters.containsKey((short) 1) || !(getOnlyElement(parameters.values()) instanceof List)) {
                return Futures.immediateFailedFuture(new IllegalArgumentException("invalid parameters"));
            }
            List<DriftLogEntry> messages = (List<DriftLogEntry>) getOnlyElement(parameters.values());

            for (DriftLogEntry message : messages) {
                if (message.getCategory().equals("exception")) {
                    return Futures.immediateFailedFuture(new TApplicationException(UNSUPPORTED_CLIENT_TYPE, message.getMessage()));
                }
            }
            this.messages.addAll(messages);
            return Futures.immediateFuture(DRIFT_OK);
        }

        @Override
        public void recordResult(String methodName, long startTime, ListenableFuture<Object> result)
        {
            // todo implement
        }
    }
}
