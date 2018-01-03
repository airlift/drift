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

import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.airlift.drift.transport.client.MethodInvoker;
import io.airlift.drift.transport.client.MethodInvokerFactory;
import io.airlift.drift.transport.netty.ssl.SslContextFactory;
import io.airlift.units.Duration;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.ssl.SslContext;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.Closeable;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.airlift.drift.transport.netty.codec.Protocol.COMPACT;
import static io.airlift.drift.transport.netty.codec.Transport.HEADER;
import static io.airlift.drift.transport.netty.ssl.SslContextFactory.createSslContextFactory;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class DriftNettyMethodInvokerFactory<I>
        implements MethodInvokerFactory<I>, Closeable
{
    private final Function<I, DriftNettyClientConfig> clientConfigurationProvider;

    private final EventLoopGroup group;
    private final SslContextFactory sslContextFactory;
    private final HostAndPort defaultSocksProxy;

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
        this.sslContextFactory = createSslContextFactory(true, factoryConfig.getSslContextRefreshTime(), group);
        this.defaultSocksProxy = factoryConfig.getSocksProxy();
    }

    @Override
    public MethodInvoker createMethodInvoker(I clientIdentity)
    {
        DriftNettyClientConfig clientConfig = clientConfigurationProvider.apply(clientIdentity);
        if (clientConfig.getTransport() == HEADER && clientConfig.getProtocol() == COMPACT) {
            throw new IllegalArgumentException("HEADER transport cannot be used with COMPACT protocol, use FB_COMPACT instead");
        }
        if (clientConfig.getSocksProxy() == null) {
            clientConfig.setSocksProxy(defaultSocksProxy);
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
                clientConfig.getTransport(),
                clientConfig.getProtocol(),
                clientConfig.getMaxFrameSize(),
                sslContext,
                clientConfig);
        if (clientConfig.isPoolEnabled()) {
            connectionManager = new ConnectionPool(connectionManager, group, clientConfig);
        }

        // an invocation should complete long before this
        Duration invokeTimeout = new Duration(
                SECONDS.toMillis(10) +
                        clientConfig.getConnectTimeout().toMillis() +
                        clientConfig.getRequestTimeout().toMillis(),
                MILLISECONDS);

        return new DriftNettyMethodInvoker(connectionManager, group, invokeTimeout);
    }

    @PreDestroy
    @Override
    public void close()
    {
        try {
            group.shutdownGracefully(0, 0, MILLISECONDS).await();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
