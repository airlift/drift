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
import io.airlift.drift.transport.netty.client.ConnectionManager.ConnectionParameters;
import io.airlift.drift.transport.netty.ssl.SslContextFactory;
import io.airlift.drift.transport.netty.ssl.SslContextFactory.SslContextConfig;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.Closeable;
import java.util.Optional;
import java.util.function.Function;

import static io.airlift.drift.transport.netty.codec.Protocol.COMPACT;
import static io.airlift.drift.transport.netty.codec.Transport.HEADER;
import static io.airlift.drift.transport.netty.ssl.SslContextFactory.createSslContextFactory;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class DriftNettyMethodInvokerFactory<I>
        implements MethodInvokerFactory<I>, Closeable
{
    private final Function<I, DriftNettyClientConfig> clientConfigurationProvider;

    private final EventLoopGroup group;
    private final SslContextFactory sslContextFactory;
    private final Optional<HostAndPort> defaultSocksProxy;
    private final ConnectionManager connectionManager;

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
        this.defaultSocksProxy = Optional.ofNullable(factoryConfig.getSocksProxy());

        ConnectionManager connectionManager = new ConnectionFactory(group, sslContextFactory);
        if (factoryConfig.isConnectionPoolEnabled()) {
            connectionManager = new ConnectionPool(connectionManager, group, factoryConfig.getConnectionPoolMaxSize(), factoryConfig.getConnectionPoolIdleTimeout());
        }
        this.connectionManager = connectionManager;
    }

    @Override
    public MethodInvoker createMethodInvoker(I clientIdentity)
    {
        ConnectionParameters clientConfig = toConnectionConfig(clientConfigurationProvider.apply(clientIdentity));

        // validate ssl context configuration is valid
        clientConfig.getSslContextConfig()
                .ifPresent(sslContextConfig -> sslContextFactory.get(sslContextConfig).get());

        return new DriftNettyMethodInvoker(clientConfig, connectionManager, group);
    }

    @PreDestroy
    @Override
    public void close()
    {
        try {
            connectionManager.close();
        }
        finally {
            try {
                group.shutdownGracefully(0, 0, MILLISECONDS).await();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private ConnectionParameters toConnectionConfig(DriftNettyClientConfig clientConfig)
    {
        if (clientConfig.getTransport() == HEADER && clientConfig.getProtocol() == COMPACT) {
            throw new IllegalArgumentException("HEADER transport cannot be used with COMPACT protocol, use FB_COMPACT instead");
        }

        Optional<SslContextConfig> sslContextConfig = Optional.empty();
        if (clientConfig.isSslEnabled()) {
            sslContextConfig = Optional.of(new SslContextConfig(
                    clientConfig.getTrustCertificate(),
                    Optional.ofNullable(clientConfig.getKey()),
                    Optional.ofNullable(clientConfig.getKey()),
                    Optional.ofNullable(clientConfig.getKeyPassword()),
                    clientConfig.getSessionCacheSize(),
                    clientConfig.getSessionTimeout(),
                    clientConfig.getCiphers()));
        }

        Optional<HostAndPort> socksProxy = Optional.ofNullable(clientConfig.getSocksProxy());
        if (!socksProxy.isPresent()) {
            socksProxy = defaultSocksProxy;
        }

        return new ConnectionParameters(
                clientConfig.getTransport(),
                clientConfig.getProtocol(),
                clientConfig.getMaxFrameSize(),
                clientConfig.getConnectTimeout(),
                clientConfig.getRequestTimeout(),
                socksProxy,
                sslContextConfig);
    }
}
