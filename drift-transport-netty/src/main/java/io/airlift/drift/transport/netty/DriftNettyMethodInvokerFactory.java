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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.airlift.drift.protocol.TBinaryProtocol;
import io.airlift.drift.protocol.TCompactProtocol;
import io.airlift.drift.protocol.TFacebookCompactProtocol;
import io.airlift.drift.protocol.TProtocolFactory;
import io.airlift.drift.transport.MethodInvoker;
import io.airlift.drift.transport.MethodInvokerFactory;
import io.airlift.drift.transport.netty.DriftNettyClientConfig.Transport;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.ssl.SslContext;
import io.netty.util.concurrent.Future;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.Closeable;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.airlift.drift.transport.netty.SslContextFactory.createSslContextFactory;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class DriftNettyMethodInvokerFactory<I>
        implements MethodInvokerFactory<I>, Closeable
{
    private final Function<I, DriftNettyClientConfig> clientConfigurationProvider;

    private final EventLoopGroup group;
    private final SslContextFactory sslContextFactory;

    public static DriftNettyMethodInvokerFactory<?> createStaticDriftNettyMethodInvokerFactory(DriftNettyClientConfig clientConfig)
    {
        return new DriftNettyMethodInvokerFactory<>(new DriftNettyConnectionFactoryConfig(), clientIdentity -> clientConfig);
    }

    @Inject
    public DriftNettyMethodInvokerFactory(DriftNettyConnectionFactoryConfig factoryConfig, Function<I, DriftNettyClientConfig> clientConfigurationProvider)
    {
        requireNonNull(factoryConfig, "factoryConfig is null");

        group = new NioEventLoopGroup(factoryConfig.getThreadCount(), new ThreadFactoryBuilder()
                .setNameFormat("drift-client-%s")
                .setDaemon(true)
                .build());

        this.clientConfigurationProvider = requireNonNull(clientConfigurationProvider, "clientConfigurationProvider is null");
        this.sslContextFactory = createSslContextFactory(factoryConfig.getSslContextRefreshTime(), group);
    }

    @Override
    public MethodInvoker createMethodInvoker(I clientIdentity)
    {
        DriftNettyClientConfig clientConfig = clientConfigurationProvider.apply(clientIdentity);

        TProtocolFactory protocolFactory;
        switch (clientConfig.getProtocol()) {
            case BINARY:
                protocolFactory = new TBinaryProtocol.Factory(false, true, -1, clientConfig.getMaxFrameSize().toBytes());
                break;
            case COMPACT:
                // Header transport uses the FB fork of the compact protocol
                if (clientConfig.getTransport() == Transport.HEADER) {
                    protocolFactory = new TFacebookCompactProtocol.Factory(toIntExact(clientConfig.getMaxFrameSize().toBytes()));
                }
                else {
                    protocolFactory = new TCompactProtocol.Factory(-1, clientConfig.getMaxFrameSize().toBytes());
                }
                break;
            default:
                throw new IllegalArgumentException("Unknown protocol: " + clientConfig.getProtocol());
        }

        MessageFraming messageFraming;
        MessageEncoding messageEncoding;
        switch (clientConfig.getTransport()) {
            case UNFRAMED:
                messageFraming = new NoMessageFraming(protocolFactory, clientConfig.getMaxFrameSize());
                messageEncoding = new SimpleMessageEncoding(protocolFactory);
                break;
            case FRAMED:
                messageFraming = new LengthPrefixedMessageFraming(clientConfig.getMaxFrameSize());
                messageEncoding = new SimpleMessageEncoding(protocolFactory);
                break;
            case HEADER:
                messageFraming = new LengthPrefixedMessageFraming(clientConfig.getMaxFrameSize());
                messageEncoding = new HeaderMessageEncoding(protocolFactory);
                break;
            default:
                throw new IllegalArgumentException("Unknown transport: " + clientConfig.getTransport());
        }

        Optional<Supplier<SslContext>> sslContext = Optional.empty();
        if (clientConfig.isSslEnabled()) {
            sslContext = Optional.of(sslContextFactory.get(
                    clientConfig.getTrustCertificate(),
                    Optional.ofNullable(clientConfig.getKey()),
                    Optional.ofNullable(clientConfig.getKey()),
                    Optional.ofNullable(clientConfig.getKeyPassword()),
                    clientConfig.getSessionCacheSize(),
                    clientConfig.getSessionTimeout(),
                    clientConfig.getCiphers()));

            // validate ssl context configuration is valid
            sslContext.get().get();
        }

        ConnectionManager connectionManager = new ConnectionFactory(
                group,
                messageFraming,
                messageEncoding,
                sslContext,
                clientConfig);
        if (clientConfig.isPoolEnabled()) {
            connectionManager = new ConnectionPool(connectionManager, group, clientConfig);
        }
        return new DriftNettyMethodInvoker(connectionManager, group);
    }

    @PreDestroy
    public void shutdownGracefully()
    {
        shutdownGracefully(true);
    }

    @Override
    public void close()
    {
        shutdownGracefully(false);
    }

    private void shutdownGracefully(boolean awaitTermination)
    {
        Future<?> future = group.shutdownGracefully();
        if (awaitTermination) {
            try {
                future.get();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            catch (ExecutionException ignored) {
            }
        }
    }
}
