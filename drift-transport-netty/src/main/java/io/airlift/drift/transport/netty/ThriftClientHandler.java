/*
 * Copyright (C) 2013 Facebook, Inc.
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

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractFuture;
import io.airlift.drift.TException;
import io.airlift.drift.transport.MethodMetadata;
import io.airlift.drift.transport.TTransportException;
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

import java.util.Iterator;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@ThreadSafe
class ThriftClientHandler
        extends ChannelDuplexHandler
{
    private static final int ONEWAY_SEQUENCE_ID = 0xFFFF_FFFF;

    private final Duration requestTimeout;
    private final MessageEncoding messageEncoding;

    private final ConcurrentHashMap<Integer, RequestHandler> pendingRequests = new ConcurrentHashMap<>();
    private final AtomicReference<TException> channelError = new AtomicReference<>();
    private final AtomicInteger sequenceId = new AtomicInteger(42);

    ThriftClientHandler(Duration requestTimeout, MessageEncoding messageEncoding)
    {
        this.requestTimeout = requireNonNull(requestTimeout, "requestTimeout is null");
        this.messageEncoding = requireNonNull(messageEncoding, "messageEncoding is null");
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

        try {
            ChannelFuture sendFuture = context.write(requestBuffer, promise);
            sendFuture.addListener(future -> messageSent(context, sendFuture, requestHandler));
        }
        catch (Throwable t) {
            onError(context, t);
        }
    }

    private void messageSent(ChannelHandlerContext context, ChannelFuture future, RequestHandler requestHandler)
    {
        try {
            if (!future.isSuccess()) {
                onError(context, new TTransportException("Sending request failed", future.cause()));
                return;
            }

            requestHandler.onRequestSent();
        }
        catch (Throwable t) {
            onError(context, t);
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext context, Object message)
            throws Exception
    {
        if (message instanceof ByteBuf && ((ByteBuf) message).isReadable()) {
            ByteBuf response = (ByteBuf) message;
            if (response.isReadable()) {
                messageReceived(context, response);
                return;
            }
        }
        context.fireChannelRead(message);
    }

    private void messageReceived(ChannelHandlerContext context, ByteBuf response)
    {
        try {
            OptionalInt sequenceId = messageEncoding.extractResponseSequenceId(response);
            if (!sequenceId.isPresent()) {
                throw new TTransportException("Could not find sequenceId in Thrift message");
            }

            RequestHandler requestHandler = pendingRequests.remove(sequenceId.getAsInt());
            if (requestHandler == null) {
                throw new TTransportException("Unknown sequence id in response: " + sequenceId.getAsInt());
            }

            requestHandler.onResponseReceived(response);
        }
        catch (Throwable t) {
            onError(context, t);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext context, Throwable cause)
    {
        onError(context, cause);
    }

    @Override
    public void channelInactive(ChannelHandlerContext context)
            throws Exception
    {
        if (!pendingRequests.isEmpty()) {
            onError(context, new TTransportException("Client was disconnected by server"));
        }
    }

    private void onError(ChannelHandlerContext context, Throwable throwable)
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

        // notify all pending requests of the error
        while (!pendingRequests.isEmpty()) {
            for (Iterator<RequestHandler> iterator = pendingRequests.values().iterator(); iterator.hasNext(); ) {
                RequestHandler requestHandler = iterator.next();
                iterator.remove();
                requestHandler.onChannelError(thriftException);
            }
        }

        context.close();
    }

    public static class ThriftRequest
            extends AbstractFuture<Object>
    {
        private final MethodMetadata method;
        private final List<Object> parameters;

        public ThriftRequest(MethodMetadata method, List<Object> parameters)
        {
            this.method = method;
            this.parameters = parameters;
        }

        MethodMetadata getMethod()
        {
            return method;
        }

        List<Object> getParameters()
        {
            return parameters;
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

        void registerRequestTimeout(EventExecutor executor)
        {
            try {
                timeout.set(executor.schedule(
                        () -> onChannelError(new TTransportException("Timed out waiting " + requestTimeout + " to receive response")),
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
            try {
                return messageEncoding.writeRequest(allocator, sequenceId, thriftRequest.getMethod(), thriftRequest.getParameters(), ImmutableMap.of());
            }
            catch (Throwable throwable) {
                onChannelError(throwable);
                throw throwable;
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

        void onResponseReceived(ByteBuf message)
        {
            if (!finished.compareAndSet(false, true)) {
                return;
            }
            try {
                cancelRequestTimeout();
                Object response = messageEncoding.readResponse(message, sequenceId, thriftRequest.getMethod());
                thriftRequest.setResponse(response);
            }
            catch (Throwable throwable) {
                thriftRequest.failed(throwable);
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
