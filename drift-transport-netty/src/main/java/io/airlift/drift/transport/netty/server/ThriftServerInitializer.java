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

import io.airlift.drift.transport.server.ServerMethodInvoker;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslContext;

import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class ThriftServerInitializer
        extends ChannelInitializer<SocketChannel>
{
    private final ServerMethodInvoker methodInvoker;
    private final DataSize maxFrameSize;
    private final Duration requestTimeout;
    private final Optional<Supplier<SslContext>> sslContextSupplier;
    private final boolean allowPlainText;
    private final boolean assumeClientsSupportOutOfOrderResponses;
    private final ScheduledExecutorService timeoutExecutor;

    public ThriftServerInitializer(
            ServerMethodInvoker methodInvoker,
            DataSize maxFrameSize,
            Duration requestTimeout,
            Optional<Supplier<SslContext>> sslContextSupplier,
            boolean allowPlainText,
            boolean assumeClientsSupportOutOfOrderResponses,
            ScheduledExecutorService timeoutExecutor)
    {
        requireNonNull(methodInvoker, "methodInvoker is null");
        requireNonNull(maxFrameSize, "maxFrameSize is null");
        requireNonNull(requestTimeout, "requestTimeout is null");
        requireNonNull(sslContextSupplier, "sslContextSupplier is null");
        checkArgument(allowPlainText || sslContextSupplier.isPresent(), "Plain text is not allowed, but SSL is not configured");
        requireNonNull(timeoutExecutor, "timeoutExecutor is null");

        this.methodInvoker = methodInvoker;
        this.maxFrameSize = maxFrameSize;
        this.requestTimeout = requestTimeout;
        this.sslContextSupplier = sslContextSupplier;
        this.allowPlainText = allowPlainText;
        this.assumeClientsSupportOutOfOrderResponses = assumeClientsSupportOutOfOrderResponses;
        this.timeoutExecutor = timeoutExecutor;
    }

    @Override
    protected void initChannel(SocketChannel channel)
    {
        ChannelPipeline pipeline = channel.pipeline();

        if (sslContextSupplier.isPresent()) {
            if (allowPlainText) {
                pipeline.addLast(new OptionalSslHandler(sslContextSupplier.get().get()));
            }
            else {
                pipeline.addLast(sslContextSupplier.get().get().newHandler(channel.alloc()));
            }
        }

        pipeline.addLast(new ThriftProtocolDetection(
                new ThriftServerHandler(methodInvoker, requestTimeout, timeoutExecutor),
                maxFrameSize,
                assumeClientsSupportOutOfOrderResponses));
    }
}
