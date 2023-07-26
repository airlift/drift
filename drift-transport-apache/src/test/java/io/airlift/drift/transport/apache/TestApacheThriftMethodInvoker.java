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
package io.airlift.drift.transport.apache;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.drift.codec.ThriftCodec;
import io.airlift.drift.codec.ThriftCodecManager;
import io.airlift.drift.codec.metadata.ThriftType;
import io.airlift.drift.transport.MethodMetadata;
import io.airlift.drift.transport.ParameterMetadata;
import io.airlift.drift.transport.apache.client.ApacheThriftClientConfig;
import io.airlift.drift.transport.apache.client.ApacheThriftConnectionFactoryConfig;
import io.airlift.drift.transport.apache.client.ApacheThriftMethodInvokerFactory;
import io.airlift.drift.transport.apache.scribe.apache.LogEntry;
import io.airlift.drift.transport.apache.scribe.apache.ResultCode;
import io.airlift.drift.transport.apache.scribe.apache.ScribeService;
import io.airlift.drift.transport.apache.scribe.apache.scribe;
import io.airlift.drift.transport.apache.scribe.drift.DriftLogEntry;
import io.airlift.drift.transport.apache.scribe.drift.DriftResultCode;
import io.airlift.drift.transport.client.InvokeRequest;
import io.airlift.drift.transport.client.MethodInvoker;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportFactory;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.function.ToIntFunction;

import static com.google.common.collect.Lists.newArrayList;
import static io.airlift.drift.codec.metadata.ThriftType.list;
import static io.airlift.drift.codec.metadata.ThriftType.optional;
import static java.util.Collections.nCopies;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;

public class TestApacheThriftMethodInvoker
{
    private static final ThriftCodecManager codecManager = new ThriftCodecManager();
    private static final List<LogEntry> MESSAGES = ImmutableList.of(
            new LogEntry("hello", "world"),
            new LogEntry("bye", "world"));
    private static final List<DriftLogEntry> DRIFT_MESSAGES = ImmutableList.copyOf(
            MESSAGES.stream()
                    .map(input -> new DriftLogEntry(input.category, input.message))
                    .collect(toList()));
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
                address -> logThrift(address, MESSAGES),
                address -> logThriftAsync(address, MESSAGES),
                address -> logApacheThriftInvocationHandler(address, DRIFT_MESSAGES),
                address -> logApacheThriftInvocationHandlerOptional(address, DRIFT_MESSAGES)));

        return newArrayList(Iterables.concat(nCopies(invocationCount, MESSAGES)));
    }

    private static int testProcessor(TProcessor processor, List<ToIntFunction<HostAndPort>> clients)
            throws Exception
    {
        try (TServerSocket serverTransport = new TServerSocket(0)) {
            TProtocolFactory protocolFactory = new Factory();
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

    private static int logThrift(HostAndPort address, List<LogEntry> messages)
    {
        try {
            TSocket socket = new TSocket(address.getHost(), address.getPort());
            socket.open();
            try {
                TBinaryProtocol tp = new TBinaryProtocol(new TFramedTransport(socket));
                assertEquals(new scribe.Client(tp).Log(messages), ResultCode.OK);
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
                scribe.AsyncClient client = new scribe.AsyncClient(
                        new Factory(),
                        asyncClientManager,
                        socket);

                SettableFuture<ResultCode> futureResult = SettableFuture.create();
                client.Log(messages, new AsyncMethodCallback<ResultCode>()
                {
                    @Override
                    public void onComplete(ResultCode resultCode)
                    {
                        try {
                            futureResult.set(resultCode);
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

    private static int logApacheThriftInvocationHandler(HostAndPort address, List<DriftLogEntry> entries)
    {
        ApacheThriftClientConfig config = new ApacheThriftClientConfig();
        ApacheThriftConnectionFactoryConfig factoryConfig = new ApacheThriftConnectionFactoryConfig();
        try (ApacheThriftMethodInvokerFactory<Void> methodInvokerFactory = new ApacheThriftMethodInvokerFactory<>(factoryConfig, clientIdentity -> config)) {
            MethodInvoker methodInvoker = methodInvokerFactory.createMethodInvoker(null);

            ParameterMetadata parameter = new ParameterMetadata(
                    (short) 1,
                    "messages",
                    (ThriftCodec<Object>) codecManager.getCodec(list(codecManager.getCodec(DriftLogEntry.class).getType())));

            MethodMetadata methodMetadata = new MethodMetadata(
                    "Log",
                    ImmutableList.of(parameter),
                    (ThriftCodec<Object>) (Object) codecManager.getCodec(DriftResultCode.class),
                    ImmutableMap.of(),
                    ImmutableMap.of(),
                    false,
                    true);

            ListenableFuture<Object> future = methodInvoker.invoke(new InvokeRequest(methodMetadata, () -> address, ImmutableMap.of(), ImmutableList.of(entries)));
            assertEquals(future.get(), DRIFT_OK);

            return 1;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static int logApacheThriftInvocationHandlerOptional(HostAndPort address, List<DriftLogEntry> entries)
    {
        ApacheThriftClientConfig config = new ApacheThriftClientConfig();
        ApacheThriftConnectionFactoryConfig factoryConfig = new ApacheThriftConnectionFactoryConfig();
        try (ApacheThriftMethodInvokerFactory<Void> methodInvokerFactory = new ApacheThriftMethodInvokerFactory<>(factoryConfig, clientIdentity -> config)) {
            MethodInvoker methodInvoker = methodInvokerFactory.createMethodInvoker(null);

            ThriftType optionalType = optional(list(codecManager.getCatalog().getThriftType(DriftLogEntry.class)));
            ParameterMetadata parameter = new ParameterMetadata(
                    (short) 1,
                    "messages",
                    (ThriftCodec<Object>) codecManager.getCodec(optionalType));

            MethodMetadata methodMetadata = new MethodMetadata(
                    "Log",
                    ImmutableList.of(parameter),
                    (ThriftCodec<Object>) (Object) codecManager.getCodec(DriftResultCode.class),
                    ImmutableMap.of(),
                    ImmutableMap.of(),
                    false,
                    true);

            ListenableFuture<Object> future = methodInvoker.invoke(new InvokeRequest(methodMetadata, () -> address, ImmutableMap.of(), ImmutableList.of(Optional.of(entries))));
            assertEquals(future.get(), DRIFT_OK);

            future = methodInvoker.invoke(new InvokeRequest(methodMetadata, () -> address, ImmutableMap.of(), ImmutableList.of(Optional.empty())));
            assertEquals(future.get(), DRIFT_OK);

            return 1;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
