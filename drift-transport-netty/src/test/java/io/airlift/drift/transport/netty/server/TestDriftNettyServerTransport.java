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
package io.airlift.drift.transport.netty.server;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.drift.codec.ThriftCodec;
import io.airlift.drift.codec.ThriftCodecManager;
import io.airlift.drift.transport.MethodMetadata;
import io.airlift.drift.transport.ParameterMetadata;
import io.airlift.drift.transport.netty.buffer.TestingPooledByteBufAllocator;
import io.airlift.drift.transport.netty.scribe.apache.LogEntry;
import io.airlift.drift.transport.netty.scribe.apache.ResultCode;
import io.airlift.drift.transport.netty.scribe.apache.scribe.Log_args;
import io.airlift.drift.transport.netty.scribe.apache.scribe.Log_result;
import io.airlift.drift.transport.netty.scribe.drift.DriftLogEntry;
import io.airlift.drift.transport.netty.scribe.drift.DriftResultCode;
import io.airlift.drift.transport.server.ServerInvokeRequest;
import io.airlift.drift.transport.server.ServerMethodInvoker;
import io.airlift.drift.transport.server.ServerTransport;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportFactory;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Lists.newArrayList;
import static io.airlift.drift.codec.metadata.ThriftType.list;
import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.thrift.TApplicationException.BAD_SEQUENCE_ID;
import static org.apache.thrift.TApplicationException.MISSING_RESULT;
import static org.apache.thrift.protocol.TMessageType.CALL;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;

public class TestDriftNettyServerTransport
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

    @Test
    public void testOutOfOrderNot()
    {
        TestingServerMethodInvoker methodInvoker = new TestingServerMethodInvoker();
        int invocationCount = testServerMethodInvoker(methodInvoker, true, ImmutableList.of(
                address -> testOutOfOrder(address, MESSAGES, new TTransportFactory(), new TBinaryProtocol.Factory(), methodInvoker.getFutureResults()),
                address -> testOutOfOrder(address, MESSAGES, new TTransportFactory(), new TCompactProtocol.Factory(), methodInvoker.getFutureResults()),
                address -> testOutOfOrder(address, MESSAGES, new TFramedTransport.Factory(), new TBinaryProtocol.Factory(), methodInvoker.getFutureResults()),
                address -> testOutOfOrder(address, MESSAGES, new TFramedTransport.Factory(), new TCompactProtocol.Factory(), methodInvoker.getFutureResults())));

        List<DriftLogEntry> expectedMessages = newArrayList(concat(nCopies(invocationCount, DRIFT_MESSAGES)));
        assertEquals(ImmutableList.copyOf(methodInvoker.getMessages()), expectedMessages);
    }

    private static int testOutOfOrder(
            HostAndPort address,
            List<LogEntry> messages,
            TTransportFactory framingFactory,
            TProtocolFactory protocolFactory,
            BlockingQueue<SettableFuture<Object>> results)
    {
        try {
            TSocket socket = new TSocket(address.getHost(), address.getPort());
            socket.open();
            try {
                TProtocol protocol = protocolFactory.getProtocol(framingFactory.getTransport(socket));

                // send first request, but do not finish the result
                sendLogRequest(11, messages, protocol);
                SettableFuture<Object> firstResult = results.take();
                assertFalse(firstResult.isDone());

                // send second request, but do not finish the result
                sendLogRequest(22, messages, protocol);
                SettableFuture<Object> secondResult = results.take();
                assertFalse(secondResult.isDone());

                // finish the second invocation, first invocation will not be completed
                secondResult.set(DriftResultCode.OK);
                assertEquals(readLogResponse(22, protocol), ResultCode.OK);
                assertFalse(firstResult.isDone());

                // complete first invocation
                firstResult.set(DriftResultCode.OK);
                assertEquals(readLogResponse(11, protocol), ResultCode.OK);
            }
            finally {
                socket.close();
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        catch (TException e) {
            throw new RuntimeException(e);
        }
        return 2;
    }

    @Test
    public void testOutOfOrderNotSupported()
    {
        TestingServerMethodInvoker methodInvoker = new TestingServerMethodInvoker();
        int invocationCount = testServerMethodInvoker(methodInvoker, false, ImmutableList.of(
                address -> testOutOfOrderNotSupported(address, MESSAGES, new TTransportFactory(), new TBinaryProtocol.Factory(), methodInvoker.getFutureResults()),
                address -> testOutOfOrderNotSupported(address, MESSAGES, new TTransportFactory(), new TCompactProtocol.Factory(), methodInvoker.getFutureResults()),
                address -> testOutOfOrderNotSupported(address, MESSAGES, new TFramedTransport.Factory(), new TBinaryProtocol.Factory(), methodInvoker.getFutureResults()),
                address -> testOutOfOrderNotSupported(address, MESSAGES, new TFramedTransport.Factory(), new TCompactProtocol.Factory(), methodInvoker.getFutureResults())));

        List<DriftLogEntry> expectedMessages = newArrayList(concat(nCopies(invocationCount, DRIFT_MESSAGES)));
        assertEquals(ImmutableList.copyOf(methodInvoker.getMessages()), expectedMessages);
    }

    private static int testOutOfOrderNotSupported(
            HostAndPort address,
            List<LogEntry> messages,
            TTransportFactory framingFactory,
            TProtocolFactory protocolFactory,
            BlockingQueue<SettableFuture<Object>> results)
    {
        try {
            TSocket socket = new TSocket(address.getHost(), address.getPort());
            socket.open();
            try {
                TProtocol protocol = protocolFactory.getProtocol(framingFactory.getTransport(socket));

                // send first request, but do not finish the result
                sendLogRequest(11, messages, protocol);
                SettableFuture<Object> firstResult = results.take();
                assertFalse(firstResult.isDone());

                // send second request, which will be blocked in the server because this client does not support out of order responses
                // the only way to test this is with a sleep because the request is blocked inside of the server IO stack
                sendLogRequest(22, messages, protocol);
                assertNull(results.poll(1, SECONDS), "Second request future");
                assertFalse(firstResult.isDone());

                // finish the first invocation, second invocation will not be completed
                firstResult.set(DriftResultCode.OK);
                assertEquals(readLogResponse(11, protocol), ResultCode.OK);

                SettableFuture<Object> secondResult = results.take();
                assertFalse(secondResult.isDone());

                // complete second invocation
                secondResult.set(DriftResultCode.OK);
                assertEquals(readLogResponse(22, protocol), ResultCode.OK);
            }
            finally {
                socket.close();
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        catch (TException e) {
            throw new RuntimeException(e);
        }
        return 2;
    }

    private static int testServerMethodInvoker(ServerMethodInvoker methodInvoker, boolean assumeClientsSupportOutOfOrderResponses, List<ToIntFunction<HostAndPort>> clients)
    {
        DriftNettyServerConfig config = new DriftNettyServerConfig()
                .setAssumeClientsSupportOutOfOrderResponses(assumeClientsSupportOutOfOrderResponses);
        TestingPooledByteBufAllocator testingAllocator = new TestingPooledByteBufAllocator();
        ServerTransport serverTransport = new DriftNettyServerTransportFactory(config, testingAllocator).createServerTransport(methodInvoker);
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

    private static class TestingServerMethodInvoker
            implements ServerMethodInvoker
    {
        private final BlockingQueue<SettableFuture<Object>> futureResults = new ArrayBlockingQueue<>(100);
        private final List<LogEntry> messages = new CopyOnWriteArrayList<>();

        public BlockingQueue<SettableFuture<Object>> getFutureResults()
        {
            return futureResults;
        }

        private List<LogEntry> getMessages()
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
            messages.addAll((List<LogEntry>) getOnlyElement(parameters.values()));

            SettableFuture<Object> result = SettableFuture.create();
            futureResults.add(result);
            return result;
        }

        @Override
        public void recordResult(String methodName, long startTime, ListenableFuture<Object> result)
        {
            // TODO implement
        }
    }

    private static void sendLogRequest(int sequenceId, List<LogEntry> messages, TProtocol protocol)
            throws TException
    {
        protocol.writeMessageBegin(new TMessage("Log", CALL, sequenceId));
        new Log_args().setMessages(messages).write(protocol);
        protocol.writeMessageEnd();
        protocol.getTransport().flush();
    }

    private static ResultCode readLogResponse(int expectedSequenceId, TProtocol protocol)
            throws TException
    {
        TMessage message = protocol.readMessageBegin();
        if (message.type == TMessageType.EXCEPTION) {
            throw TApplicationException.read(protocol);
        }
        if (message.type != TMessageType.REPLY) {
            throw new TApplicationException(MISSING_RESULT, "request failed");
        }
        if (message.seqid != expectedSequenceId) {
            throw new TApplicationException(BAD_SEQUENCE_ID, format("expected sequenceId %s, but received %s", expectedSequenceId, message.seqid));
        }

        Log_result result = new Log_result();
        result.read(protocol);
        protocol.readMessageEnd();
        return result.success;
    }
}
