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
package io.airlift.drift.transport.netty;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.drift.codec.ThriftCodec;
import io.airlift.drift.codec.ThriftCodecManager;
import io.airlift.drift.transport.InvokeRequest;
import io.airlift.drift.transport.MethodInvoker;
import io.airlift.drift.transport.MethodMetadata;
import io.airlift.drift.transport.ParameterMetadata;
import io.airlift.drift.transport.netty.scribe.apache.LogEntry;
import io.airlift.drift.transport.netty.scribe.apache.ResultCode;
import io.airlift.drift.transport.netty.scribe.apache.ScribeService;
import io.airlift.drift.transport.netty.scribe.apache.scribe;
import io.airlift.drift.transport.netty.scribe.apache.scribe.AsyncClient.Log_call;
import io.airlift.drift.transport.netty.scribe.apache.scribe.Client;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol;
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
import java.util.concurrent.ExecutionException;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Lists.newArrayList;
import static io.airlift.drift.codec.metadata.ThriftType.list;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static java.util.Collections.nCopies;
import static org.apache.thrift.TApplicationException.INTERNAL_ERROR;
import static org.testng.Assert.assertEquals;

public class TestDriftNettyMethodInvoker
{
    private static final ThriftCodecManager codecManager = new ThriftCodecManager();
    private static final List<LogEntry> MESSAGES = ImmutableList.of(
            new LogEntry("hello", "world"),
            new LogEntry("bye", "world"));
    private static final List<io.airlift.drift.transport.netty.scribe.drift.LogEntry> DRIFT_MESSAGES = ImmutableList.copyOf(
            MESSAGES.stream()
                    .map(input -> new io.airlift.drift.transport.netty.scribe.drift.LogEntry(input.category, input.message))
                    .collect(Collectors.toList()));
    private static final io.airlift.drift.transport.netty.scribe.drift.ResultCode DRIFT_OK = io.airlift.drift.transport.netty.scribe.drift.ResultCode.OK;

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
                address -> logNiftyInvocationHandler1(address, DRIFT_MESSAGES)));

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

    private static int logThrift(HostAndPort address, List<LogEntry> messages)
    {
        try {
            TSocket socket = new TSocket(address.getHost(), address.getPort());
            socket.open();
            try {
                TBinaryProtocol tp = new TBinaryProtocol(new TFramedTransport(socket));
                Client client = new Client(tp);
                assertEquals(client.Log(messages), ResultCode.OK);

                try {
                    client.Log(ImmutableList.of(new LogEntry("exception", "test")));
                }
                catch (TApplicationException e) {
                    assertEquals(e.getType(), INTERNAL_ERROR);
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

    private static int logNiftyInvocationHandler1(HostAndPort address, List<io.airlift.drift.transport.netty.scribe.drift.LogEntry> entries)
    {
        DriftNettyClientConfig config = new DriftNettyClientConfig()
                .setPoolEnabled(true);
        try (DriftNettyMethodInvokerFactory<Void> methodInvokerFactory = new DriftNettyMethodInvokerFactory<>(new DriftNettyConnectionFactoryConfig(), clientIdentity -> config)) {
            MethodInvoker methodInvoker = methodInvokerFactory.createMethodInvoker(null);

            ParameterMetadata parameter = new ParameterMetadata(
                    (short) 1,
                    "messages",
                    (ThriftCodec<Object>) codecManager.getCodec(list(codecManager.getCodec(io.airlift.drift.transport.netty.scribe.drift.LogEntry.class).getType())));

            MethodMetadata methodMetadata = new MethodMetadata(
                    "Log",
                    ImmutableList.of(parameter),
                    (ThriftCodec<Object>) (Object) codecManager.getCodec(io.airlift.drift.transport.netty.scribe.drift.ResultCode.class),
                    ImmutableMap.of(),
                    false);

            ListenableFuture<Object> future = methodInvoker.invoke(new InvokeRequest(methodMetadata, () -> address, ImmutableMap.of(), ImmutableList.of(entries)));
            assertEquals(future.get(), DRIFT_OK);

            try {
                future = methodInvoker.invoke(new InvokeRequest(methodMetadata, () -> address, ImmutableMap.of(), ImmutableList.of(ImmutableList.of(new io.airlift.drift.transport.netty.scribe.drift.LogEntry("exception", "test")))));
                assertEquals(future.get(), DRIFT_OK);
            }
            catch (ExecutionException e) {
                Throwable cause = e.getCause();
                assertInstanceOf(cause, io.airlift.drift.TApplicationException.class);
                io.airlift.drift.TApplicationException applicationException = (io.airlift.drift.TApplicationException) cause;
                assertEquals(applicationException.getType(), io.airlift.drift.TApplicationException.Type.INTERNAL_ERROR);
            }
            return 1;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
