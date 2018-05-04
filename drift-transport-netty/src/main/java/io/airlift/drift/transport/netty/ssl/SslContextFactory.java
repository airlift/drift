/*
 * Copyright (C) 2017 Facebook, Inc.
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
package io.airlift.drift.transport.netty.ssl;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ExecutionError;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.units.Duration;

import java.io.File;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class SslContextFactory
{
    private final LoadingCache<SslContextConfig, ReloadableSslContext> cache;

    public static SslContextFactory createSslContextFactory(boolean forClient, Duration refreshTime, ScheduledExecutorService scheduledExecutor)
    {
        SslContextFactory sslContextFactory = new SslContextFactory(forClient);
        scheduledExecutor.scheduleWithFixedDelay(sslContextFactory::refresh, refreshTime.toMillis(), refreshTime.toMillis(), MILLISECONDS);
        return sslContextFactory;
    }

    private SslContextFactory(boolean forClient)
    {
        this.cache = CacheBuilder.newBuilder()
                .expireAfterAccess(1, TimeUnit.HOURS)
                .build(CacheLoader.from(key ->
                        new ReloadableSslContext(
                                forClient,
                                key.getTrustCertificatesFile(),
                                key.getClientCertificatesFile(),
                                key.getPrivateKeyFile(),
                                key.getPrivateKeyPassword(),
                                key.getSessionCacheSize(),
                                key.getSessionTimeout(),
                                key.getCiphers())));
    }

    public ReloadableSslContext get(
            File trustCertificatesFile,
            Optional<File> clientCertificatesFile,
            Optional<File> privateKeyFile,
            Optional<String> privateKeyPassword,
            long sessionCacheSize,
            Duration sessionTimeout,
            List<String> ciphers)
    {
        return get(new SslContextConfig(
                trustCertificatesFile,
                clientCertificatesFile,
                privateKeyFile,
                privateKeyPassword,
                sessionCacheSize,
                sessionTimeout,
                ciphers));
    }

    public ReloadableSslContext get(SslContextConfig sslContextConfig)
    {
        try {
            return cache.getUnchecked(sslContextConfig);
        }
        catch (UncheckedExecutionException | ExecutionError e) {
            throw new RuntimeException("Error initializing SSL context", e.getCause());
        }
    }

    private void refresh()
    {
        cache.asMap().values().forEach(ReloadableSslContext::reload);
    }

    public static class SslContextConfig
    {
        private final File trustCertificatesFile;
        private final Optional<File> clientCertificatesFile;
        private final Optional<File> privateKeyFile;
        private final Optional<String> privateKeyPassword;

        private final long sessionCacheSize;
        private final Duration sessionTimeout;
        private final List<String> ciphers;

        public SslContextConfig(
                File trustCertificatesFile,
                Optional<File> clientCertificatesFile,
                Optional<File> privateKeyFile,
                Optional<String> privateKeyPassword,
                long sessionCacheSize,
                Duration sessionTimeout, List<String> ciphers)
        {
            this.trustCertificatesFile = requireNonNull(trustCertificatesFile, "trustCertificatesFile is null");
            this.clientCertificatesFile = requireNonNull(clientCertificatesFile, "clientCertificatesFile is null");
            this.privateKeyFile = requireNonNull(privateKeyFile, "privateKeyFile is null");
            this.privateKeyPassword = requireNonNull(privateKeyPassword, "privateKeyPassword is null");
            this.sessionCacheSize = sessionCacheSize;
            this.sessionTimeout = requireNonNull(sessionTimeout, "sessionTimeout is null");
            this.ciphers = ImmutableList.copyOf(requireNonNull(ciphers, "ciphers is null"));
        }

        public File getTrustCertificatesFile()
        {
            return trustCertificatesFile;
        }

        public Optional<File> getClientCertificatesFile()
        {
            return clientCertificatesFile;
        }

        public Optional<File> getPrivateKeyFile()
        {
            return privateKeyFile;
        }

        public Optional<String> getPrivateKeyPassword()
        {
            return privateKeyPassword;
        }

        public long getSessionCacheSize()
        {
            return sessionCacheSize;
        }

        public Duration getSessionTimeout()
        {
            return sessionTimeout;
        }

        public List<String> getCiphers()
        {
            return ciphers;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SslContextConfig that = (SslContextConfig) o;
            return sessionCacheSize == that.sessionCacheSize &&
                    Objects.equals(trustCertificatesFile, that.trustCertificatesFile) &&
                    Objects.equals(clientCertificatesFile, that.clientCertificatesFile) &&
                    Objects.equals(privateKeyFile, that.privateKeyFile) &&
                    Objects.equals(privateKeyPassword, that.privateKeyPassword) &&
                    Objects.equals(sessionTimeout, that.sessionTimeout) &&
                    Objects.equals(ciphers, that.ciphers);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(trustCertificatesFile, clientCertificatesFile, privateKeyFile, privateKeyPassword, sessionCacheSize, sessionTimeout, ciphers);
        }
    }
}
