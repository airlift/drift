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
package io.airlift.drift.transport.apache.client;

import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.inject.Inject;
import io.airlift.drift.transport.client.MethodInvoker;
import io.airlift.drift.transport.client.MethodInvokerFactory;
import io.airlift.security.pem.PemReader;
import jakarta.annotation.PreDestroy;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TTransportFactory;
import org.apache.thrift.transport.layered.TFramedTransport;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.Optional;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class ApacheThriftMethodInvokerFactory<I>
        implements MethodInvokerFactory<I>, Closeable
{
    private final Function<I, ApacheThriftClientConfig> clientConfigurationProvider;

    private final ListeningExecutorService executorService;
    private final ListeningScheduledExecutorService delayService;
    private final HostAndPort defaultSocksProxy;

    public static ApacheThriftMethodInvokerFactory<?> createStaticApacheThriftMethodInvokerFactory(ApacheThriftClientConfig clientConfig)
    {
        return createStaticApacheThriftMethodInvokerFactory(clientConfig, new ApacheThriftConnectionFactoryConfig());
    }

    public static ApacheThriftMethodInvokerFactory<?> createStaticApacheThriftMethodInvokerFactory(
            ApacheThriftClientConfig clientConfig,
            ApacheThriftConnectionFactoryConfig factoryConfig)
    {
        requireNonNull(clientConfig, "clientConfig is null");
        return new ApacheThriftMethodInvokerFactory<>(factoryConfig, clientIdentity -> clientConfig);
    }

    @Inject
    public ApacheThriftMethodInvokerFactory(ApacheThriftConnectionFactoryConfig factoryConfig, Function<I, ApacheThriftClientConfig> clientConfigurationProvider)
    {
        requireNonNull(factoryConfig, "factoryConfig is null");

        ThreadFactory threadFactory = daemonThreadsNamed("drift-client-%s");
        if (factoryConfig.getThreadCount() == null) {
            executorService = listeningDecorator(newCachedThreadPool(threadFactory));
            delayService = listeningDecorator(newSingleThreadScheduledExecutor(daemonThreadsNamed("drift-client-delay-%s")));
        }
        else {
            delayService = listeningDecorator(newScheduledThreadPool(factoryConfig.getThreadCount(), threadFactory));
            executorService = delayService;
        }
        this.clientConfigurationProvider = requireNonNull(clientConfigurationProvider, "clientConfigurationProvider is null");
        this.defaultSocksProxy = factoryConfig.getSocksProxy();
    }

    @Override
    public MethodInvoker createMethodInvoker(I clientIdentity)
    {
        ApacheThriftClientConfig config = clientConfigurationProvider.apply(clientIdentity);
        if (config.getSocksProxy() == null) {
            config.setSocksProxy(defaultSocksProxy);
        }

        TTransportFactory transportFactory;
        switch (config.getTransport()) {
            case UNFRAMED:
                transportFactory = new TTransportFactory();
                break;
            case FRAMED:
                transportFactory = new TFramedTransport.Factory(toIntExact(config.getMaxFrameSize().toBytes()));
                break;
            default:
                throw new IllegalArgumentException("Unknown transport: " + config.getTransport());
        }

        TProtocolFactory protocolFactory;
        switch (config.getProtocol()) {
            case BINARY:
                protocolFactory = new TBinaryProtocol.Factory();
                break;
            case COMPACT:
                protocolFactory = new TCompactProtocol.Factory();
                break;
            default:
                throw new IllegalArgumentException("Unknown protocol: " + config.getProtocol());
        }

        Optional<SSLContext> sslContext = Optional.empty();
        if (config.isSslEnabled()) {
            sslContext = Optional.of(createSslContext(config));
        }

        return new ApacheThriftMethodInvoker(
                executorService,
                delayService,
                transportFactory,
                protocolFactory,
                config.getConnectTimeout(),
                config.getRequestTimeout(),
                Optional.ofNullable(config.getSocksProxy()),
                sslContext);
    }

    private static SSLContext createSslContext(ApacheThriftClientConfig config)
    {
        try {
            KeyStore trustStore = PemReader.loadTrustStore(config.getTrustCertificate());
            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(trustStore);

            KeyManager[] keyManagers = null;
            if (config.getKey() != null) {
                Optional<String> keyPassword = Optional.ofNullable(config.getKeyPassword());
                KeyStore keyStore = PemReader.loadKeyStore(config.getTrustCertificate(), config.getKey(), keyPassword);
                KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                keyManagerFactory.init(keyStore, new char[0]);
                keyManagers = keyManagerFactory.getKeyManagers();
            }

            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(keyManagers, trustManagerFactory.getTrustManagers(), null);
            return sslContext;
        }
        catch (IOException | GeneralSecurityException e) {
            throw new IllegalArgumentException("Unable to load SSL keys", e);
        }
    }

    @PreDestroy
    @Override
    public void close()
    {
        shutdownAndAwaitTermination(executorService, 5, TimeUnit.MINUTES);
    }
}
