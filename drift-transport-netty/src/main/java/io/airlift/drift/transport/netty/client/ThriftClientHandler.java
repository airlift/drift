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
package io.airlift.drift.transport.netty.client;

import com.google.common.util.concurrent.AbstractFuture;
import io.airlift.drift.TApplicationException;
import io.airlift.drift.TException;
import io.airlift.drift.codec.ThriftCodec;
import io.airlift.drift.codec.internal.ProtocolReader;
import io.airlift.drift.codec.internal.ProtocolWriter;
import io.airlift.drift.codec.metadata.ThriftType;
import io.airlift.drift.protocol.TMessage;
import io.airlift.drift.protocol.TProtocolReader;
import io.airlift.drift.protocol.TProtocolWriter;
import io.airlift.drift.protocol.TTransportException;
import io.airlift.drift.transport.MethodMetadata;
import io.airlift.drift.transport.ParameterMetadata;
import io.airlift.drift.transport.client.DriftApplicationException;
import io.airlift.drift.transport.client.RequestTimeoutException;
import io.airlift.drift.transport.netty.codec.Protocol;
import io.airlift.drift.transport.netty.codec.ThriftFrame;
import io.airlift.drift.transport.netty.codec.Transport;
import io.airlift.drift.transport.netty.ssl.TChannelBufferInputTransport;
import io.airlift.drift.transport.netty.ssl.TChannelBufferOutputTransport;
import io.airlift.units.Duration;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.ScheduledFuture;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.airlift.drift.TApplicationException.Type.BAD_SEQUENCE_ID;
import static io.airlift.drift.TApplicationException.Type.INVALID_MESSAGE_TYPE;
import static io.airlift.drift.TApplicationException.Type.MISSING_RESULT;
import static io.airlift.drift.TApplicationException.Type.WRONG_METHOD_NAME;
import static io.airlift.drift.protocol.TMessageType.CALL;
import static io.airlift.drift.protocol.TMessageType.EXCEPTION;
import static io.airlift.drift.protocol.TMessageType.ONEWAY;
import static io.airlift.drift.protocol.TMessageType.REPLY;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@ThreadSafe
public class ThriftClientHandler
        extends ChannelDuplexHandler
{
    private static final int ONEWAY_SEQUENCE_ID = 0xFFFF_FFFF;

    private final Duration requestTimeout;
    private final Transport transport;
    private final Protocol protocol;

    private final ConcurrentHashMap<Integer, RequestHandler> pendingRequests = new ConcurrentHashMap<>();
    private final AtomicReference<TException> channelError = new AtomicReference<>();
    private final AtomicInteger sequenceId = new AtomicInteger(42);

    ThriftClientHandler(Duration requestTimeout, Transport transport, Protocol protocol)
    {
        this.requestTimeout = requireNonNull(requestTimeout, "requestTimeout is null");
        this.transport = requireNonNull(transport, "transport is null");
        this.protocol = requireNonNull(protocol, "protocol is null");
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object message, ChannelPromise promise)
            throws Exception
    {
        if (message instanceof ThriftRequest) {
            ThriftRequest thriftRequest = (ThriftRequest) message;
            sendMessage(ctx, thriftRequest, promise);
        }
        else {
            ctx.write(message, promise);
        }
    }

    private void sendMessage(ChannelHandlerContext context, ThriftRequest thriftRequest, ChannelPromise promise)
            throws Exception
    {
        // todo ONEWAY_SEQUENCE_ID is a header protocol thing... make sure this works with framed and unframed
        int sequenceId = thriftRequest.isOneway() ? ONEWAY_SEQUENCE_ID : this.sequenceId.incrementAndGet();
        RequestHandler requestHandler = new RequestHandler(thriftRequest, sequenceId);

        // register timeout
        requestHandler.registerRequestTimeout(context.executor());

        // write request
        ByteBuf requestBuffer = requestHandler.encodeRequest(context.alloc());

        // register request if we are expecting a response
        if (!thriftRequest.isOneway()) {
            if (pendingRequests.putIfAbsent(sequenceId, requestHandler) != null) {
                requestHandler.onChannelError(new TTransportException("Another request with the same sequenceId is already in progress"));
            }
        }

        // if this connection is failed, immediately fail the request
        TException channelError = this.channelError.get();
        if (channelError != null) {
            thriftRequest.failed(channelError);
            requestBuffer.release();
            return;
        }

        try {
            ThriftFrame thriftFrame = new ThriftFrame(
                    sequenceId,
                    requestBuffer,
                    thriftRequest.getHeaders(),
                    transport,
                    protocol,
                    true);

            ChannelFuture sendFuture = context.write(thriftFrame, promise);
            sendFuture.addListener(future -> messageSent(context, sendFuture, requestHandler));
        }
        catch (Throwable t) {
            onError(context, t, Optional.of(requestHandler));
        }
    }

    private void messageSent(ChannelHandlerContext context, ChannelFuture future, RequestHandler requestHandler)
    {
        try {
            if (!future.isSuccess()) {
                onError(context, new TTransportException("Sending request failed", future.cause()), Optional.of(requestHandler));
                return;
            }

            requestHandler.onRequestSent();
        }
        catch (Throwable t) {
            onError(context, t, Optional.of(requestHandler));
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext context, Object message)
            throws Exception
    {
        if (message instanceof ThriftFrame) {
            messageReceived(context, (ThriftFrame) message);
            return;
        }
        context.fireChannelRead(message);
    }

    private void messageReceived(ChannelHandlerContext context, ThriftFrame thriftFrame)
    {
        RequestHandler requestHandler = null;
        try {
            requestHandler = pendingRequests.remove(thriftFrame.getSequenceId());
            if (requestHandler == null) {
                throw new TTransportException("Unknown sequence id in response: " + thriftFrame.getSequenceId());
            }

            requestHandler.onResponseReceived(thriftFrame.retain());
        }
        catch (Throwable t) {
            onError(context, t, Optional.ofNullable(requestHandler));
        }
        finally {
            thriftFrame.release();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext context, Throwable cause)
    {
        onError(context, cause, Optional.empty());
    }

    @Override
    public void channelInactive(ChannelHandlerContext context)
            throws Exception
    {
        onError(context, new TTransportException("Client was disconnected by server"), Optional.empty());
    }

    private void onError(ChannelHandlerContext context, Throwable throwable, Optional<RequestHandler> currentRequest)
    {
        TException thriftException;
        if (throwable instanceof TException) {
            thriftException = (TException) throwable;
        }
        else {
            thriftException = new TTransportException(throwable);
        }

        // set channel error
        if (!channelError.compareAndSet(null, thriftException)) {
            // another thread is already tearing down this channel
            return;
        }

        // current request may have already been removed from pendingRequests, so notify it directly
        currentRequest.ifPresent(request -> {
            pendingRequests.remove(request.getSequenceId());
            request.onChannelError(thriftException);
        });

        // notify all pending requests of the error
        // Note while loop should not be necessary since this class should be single
        // threaded, but it is better to be safe in cleanup code
        while (!pendingRequests.isEmpty()) {
            pendingRequests.values().removeIf(request -> {
                request.onChannelError(thriftException);
                return true;
            });
        }

        context.close();
    }

    public static class ThriftRequest
            extends AbstractFuture<Object>
    {
        private final MethodMetadata method;
        private final List<Object> parameters;
        private final Map<String, String> headers;

        public ThriftRequest(MethodMetadata method, List<Object> parameters, Map<String, String> headers)
        {
            this.method = method;
            this.parameters = parameters;
            this.headers = headers;
        }

        MethodMetadata getMethod()
        {
            return method;
        }

        List<Object> getParameters()
        {
            return parameters;
        }

        public Map<String, String> getHeaders()
        {
            return headers;
        }

        boolean isOneway()
        {
            return method.isOneway();
        }

        void setResponse(Object response)
        {
            set(response);
        }

        void failed(Throwable throwable)
        {
            setException(throwable);
        }
    }

    private final class RequestHandler
    {
        private final ThriftRequest thriftRequest;
        private final int sequenceId;

        private final AtomicBoolean finished = new AtomicBoolean();
        private final AtomicReference<ScheduledFuture<?>> timeout = new AtomicReference<>();

        public RequestHandler(ThriftRequest thriftRequest, int sequenceId)
        {
            this.thriftRequest = thriftRequest;
            this.sequenceId = sequenceId;
        }

        public int getSequenceId()
        {
            return sequenceId;
        }

        void registerRequestTimeout(EventExecutor executor)
        {
            try {
                timeout.set(executor.schedule(
                        () -> onChannelError(new RequestTimeoutException("Timed out waiting " + requestTimeout + " to receive response")),
                        requestTimeout.toMillis(),
                        MILLISECONDS));
            }
            catch (Throwable throwable) {
                onChannelError(new TTransportException("Unable to schedule request timeout", throwable));
                throw throwable;
            }
        }

        ByteBuf encodeRequest(ByteBufAllocator allocator)
                throws Exception
        {
            TChannelBufferOutputTransport transport = new TChannelBufferOutputTransport(allocator);
            try {
                TProtocolWriter protocolWriter = protocol.createProtocol(transport);

                // Note that though setting message type to ONEWAY can be helpful when looking at packet
                // captures, some clients always send CALL and so servers are forced to rely on the "oneway"
                // attribute on thrift method in the interface definition, rather than checking the message
                // type.
                MethodMetadata method = thriftRequest.getMethod();
                protocolWriter.writeMessageBegin(new TMessage(method.getName(), method.isOneway() ? ONEWAY : CALL, sequenceId));

                // write the parameters
                ProtocolWriter writer = new ProtocolWriter(protocolWriter);
                writer.writeStructBegin(method.getName() + "_args");
                List<Object> parameters = thriftRequest.getParameters();
                for (int i = 0; i < parameters.size(); i++) {
                    Object value = parameters.get(i);
                    ParameterMetadata parameter = method.getParameters().get(i);
                    writer.writeField(parameter.getName(), parameter.getFieldId(), parameter.getCodec(), value);
                }
                writer.writeStructEnd();

                protocolWriter.writeMessageEnd();
                return transport.getBuffer();
            }
            catch (Throwable throwable) {
                onChannelError(throwable);
                throw throwable;
            }
            finally {
                transport.release();
            }
        }

        void onRequestSent()
        {
            if (!thriftRequest.isOneway()) {
                return;
            }

            if (!finished.compareAndSet(false, true)) {
                return;
            }

            try {
                cancelRequestTimeout();
                thriftRequest.setResponse(null);
            }
            catch (Throwable throwable) {
                onChannelError(throwable);
            }
        }

        void onResponseReceived(ThriftFrame thriftFrame)
        {
            try {
                if (!finished.compareAndSet(false, true)) {
                    return;
                }

                cancelRequestTimeout();
                Object response = decodeResponse(thriftFrame.getMessage());
                thriftRequest.setResponse(response);
            }
            catch (Throwable throwable) {
                thriftRequest.failed(throwable);
            }
            finally {
                thriftFrame.release();
            }
        }

        Object decodeResponse(ByteBuf responseMessage)
                throws Exception
        {
            TChannelBufferInputTransport transport = new TChannelBufferInputTransport(responseMessage);
            try {
                TProtocolReader protocolReader = protocol.createProtocol(transport);
                MethodMetadata method = thriftRequest.getMethod();

                // validate response header
                TMessage message = protocolReader.readMessageBegin();
                if (message.getType() == EXCEPTION) {
                    TApplicationException exception = ExceptionReader.readTApplicationException(protocolReader);
                    protocolReader.readMessageEnd();
                    throw exception;
                }
                if (message.getType() != REPLY) {
                    throw new TApplicationException(INVALID_MESSAGE_TYPE, format("Received invalid message type %s from server", message.getType()));
                }
                if (!message.getName().equals(method.getName())) {
                    throw new TApplicationException(WRONG_METHOD_NAME, format("Wrong method name in reply: expected %s but received %s", method.getName(), message.getName()));
                }
                if (message.getSequenceId() != sequenceId) {
                    throw new TApplicationException(BAD_SEQUENCE_ID, format("%s failed: out of sequence response", method.getName()));
                }

                // read response struct
                ProtocolReader reader = new ProtocolReader(protocolReader);
                reader.readStructBegin();

                Object results = null;
                Exception exception = null;
                while (reader.nextField()) {
                    if (reader.getFieldId() == 0) {
                        results = reader.readField(method.getResultCodec());
                    }
                    else {
                        ThriftCodec<Object> exceptionCodec = method.getExceptionCodecs().get(reader.getFieldId());
                        if (exceptionCodec != null) {
                            exception = (Exception) reader.readField(exceptionCodec);
                        }
                        else {
                            reader.skipFieldData();
                        }
                    }
                }
                reader.readStructEnd();
                protocolReader.readMessageEnd();

                if (exception != null) {
                    throw new DriftApplicationException(exception);
                }

                if (method.getResultCodec().getType() == ThriftType.VOID) {
                    return null;
                }

                if (results == null) {
                    throw new TApplicationException(MISSING_RESULT, format("%s failed: unknown result", method.getName()));
                }
                return results;
            }
            finally {
                transport.release();
            }
        }

        void onChannelError(Throwable requestException)
        {
            if (!finished.compareAndSet(false, true)) {
                return;
            }

            try {
                cancelRequestTimeout();
            }
            finally {
                thriftRequest.failed(requestException);
            }
        }

        private void cancelRequestTimeout()
        {
            ScheduledFuture<?> timeout = this.timeout.get();
            if (timeout != null) {
                timeout.cancel(false);
            }
        }
    }
}
