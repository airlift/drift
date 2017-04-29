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
package io.airlift.drift.transport.netty.server;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.drift.transport.netty.SslContextFactory;
import io.airlift.drift.transport.server.ServerMethodInvoker;
import io.airlift.drift.transport.server.ServerTransport;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.util.concurrent.Future;

import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.drift.transport.netty.SslContextFactory.createSslContextFactory;
import static io.netty.channel.ChannelOption.SO_BACKLOG;
import static io.netty.channel.ChannelOption.SO_KEEPALIVE;
import static java.util.Objects.requireNonNull;

public class DriftNettyServerTransport
        implements ServerTransport
{
    private final ServerBootstrap bootstrap;
    private final int port;

    private final EventLoopGroup ioGroup;
    private final EventLoopGroup workerGroup;

    private Channel channel;

    private final AtomicBoolean running = new AtomicBoolean();

    public DriftNettyServerTransport(ServerMethodInvoker methodInvoker, DriftNettyServerConfig config)
    {
        requireNonNull(methodInvoker, "methodInvoker is null");
        requireNonNull(config, "config is null");
        this.port = config.getPort();

        ioGroup = new NioEventLoopGroup(config.getIoThreadCount(), daemonThreadsNamed("drift-server-io-%s"));

        workerGroup = new NioEventLoopGroup(config.getWorkerThreadCount(), daemonThreadsNamed("drift-server-worker-%s"));

        Optional<Supplier<SslContext>> sslContext = Optional.empty();
        if (config.isSslEnabled()) {
            SslContextFactory sslContextFactory = createSslContextFactory(false, config.getSslContextRefreshTime(), workerGroup);
            sslContext = Optional.of(sslContextFactory.get(
                    config.getTrustCertificate(),
                    Optional.ofNullable(config.getKey()),
                    Optional.ofNullable(config.getKey()),
                    Optional.ofNullable(config.getKeyPassword()),
                    config.getSessionCacheSize(),
                    config.getSessionTimeout(),
                    config.getCiphers()));

            // validate ssl context configuration is valid
            sslContext.get().get();
        }

        ThriftServerInitializer serverInitializer = new ThriftServerInitializer(
                methodInvoker,
                config.getMaxFrameSize(),
                config.getRequestTimeout(),
                sslContext,
                config.isAllowPlaintext(),
                config.isAssumeClientsSupportOutOfOrderResponses(),
                workerGroup);

        bootstrap = new ServerBootstrap()
                .group(ioGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(serverInitializer)
                .option(SO_BACKLOG, config.getAcceptBacklog())
                .childOption(SO_KEEPALIVE, true)
                .validate();
    }

    public void start()
    {
        if (!running.compareAndSet(false, true)) {
            return;
        }

        try {
            channel = bootstrap.bind(port).sync().channel();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("interrupted while starting", e);
        }
    }

    public int getPort()
    {
        return ((InetSocketAddress) channel.localAddress()).getPort();
    }

    @Override
    public ListenableFuture<?> shutdown()
    {
        SettableFuture<?> shutdownFuture = SettableFuture.create();

        if (channel != null) {
            channel.close().addListener(ignored -> completeWhenAllFinished(shutdownFuture, ioGroup.shutdownGracefully(), workerGroup.shutdownGracefully()));
        }
        else {
            completeWhenAllFinished(shutdownFuture, ioGroup.shutdownGracefully(), workerGroup.shutdownGracefully());
        }

        return shutdownFuture;
    }

    private static void completeWhenAllFinished(SettableFuture<?> shutdownFuture, Future<?>... futures)
    {
        requireNonNull(shutdownFuture, "shutdownFuture is null");
        checkArgument(futures.length > 0, "no futures");

        AtomicLong remainingFutures = new AtomicLong(futures.length);
        for (Future<?> future : futures) {
            future.addListener(ignored -> {
                if (remainingFutures.decrementAndGet() == 0) {
                    shutdownFuture.set(null);
                }
            });
        }
    }
}
