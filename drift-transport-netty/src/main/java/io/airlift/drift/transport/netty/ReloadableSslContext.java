/*
 * Copyright (C) 2017 Facebook, Inc.
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

import com.google.common.collect.ImmutableList;
import com.google.common.hash.HashCode;
import com.google.common.io.Files;
import io.airlift.units.Duration;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.google.common.hash.Hashing.sha256;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class ReloadableSslContext
        implements Supplier<SslContext>
{
    private final FileWatch trustCertificatesFileWatch;
    private final Optional<FileWatch> clientCertificatesFileWatch;
    private final Optional<FileWatch> privateKeyFileWatch;
    private final Optional<String> privateKeyPassword;

    private final long sessionCacheSize;
    private final Duration sessionTimeout;
    private final List<String> ciphers;

    private final AtomicReference<SslContextHolder> sslContext = new AtomicReference<>(new SslContextHolder(new UncheckedIOException(new IOException("Not initialized"))));

    public ReloadableSslContext(
            File trustCertificatesFile,
            Optional<File> clientCertificatesFile,
            Optional<File> privateKeyFile,
            Optional<String> privateKeyPassword,
            long sessionCacheSize,
            Duration sessionTimeout,
            List<String> ciphers)
    {
        this.trustCertificatesFileWatch = new FileWatch(requireNonNull(trustCertificatesFile, "trustCertificatesFile is null"));
        requireNonNull(clientCertificatesFile, "clientCertificatesFile is null");
        this.clientCertificatesFileWatch = clientCertificatesFile.map(FileWatch::new);
        requireNonNull(privateKeyFile, "privateKeyFile is null");
        this.privateKeyFileWatch = privateKeyFile.map(FileWatch::new);
        this.privateKeyPassword = requireNonNull(privateKeyPassword, "privateKeyPassword is null");
        this.sessionCacheSize = sessionCacheSize;
        this.sessionTimeout = requireNonNull(sessionTimeout, "sessionTimeout is null");
        this.ciphers = ImmutableList.copyOf(requireNonNull(ciphers, "ciphers is null"));
        reload();
    }

    @Override
    public SslContext get()
    {
        return sslContext.get().getSslContext();
    }

    public synchronized void reload()
    {
        try {
            // every watch must be called each time to update status
            boolean trustCertificateModified = trustCertificatesFileWatch.updateState();
            boolean clientCertificateModified = false;
            if (clientCertificatesFileWatch.isPresent()) {
                clientCertificateModified = clientCertificatesFileWatch.get().updateState();
            }
            boolean privateKeyModified = false;
            if (privateKeyFileWatch.isPresent()) {
                privateKeyModified = privateKeyFileWatch.get().updateState();
            }
            if (trustCertificateModified || clientCertificateModified || privateKeyModified) {
                SslContextBuilder sslContextBuilder = SslContextBuilder.forClient()
                        .trustManager(trustCertificatesFileWatch.getFile())
                        .keyManager(
                                clientCertificatesFileWatch.map(FileWatch::getFile).orElse(null),
                                privateKeyFileWatch.map(FileWatch::getFile).orElse(null),
                                privateKeyPassword.orElse(null))
                        .sessionCacheSize(sessionCacheSize)
                        .sessionTimeout(sessionTimeout.roundTo(SECONDS));
                if (!ciphers.isEmpty()) {
                    sslContextBuilder.ciphers(ciphers);
                }
                sslContext.set(new SslContextHolder(sslContextBuilder.build()));
            }
        }
        catch (IOException e) {
            sslContext.set(new SslContextHolder(new UncheckedIOException(e)));
        }
    }

    private static class FileWatch
    {
        private final File file;
        private long lastModified = -1;
        private long length = -1;
        private HashCode hashCode = sha256().hashBytes(new byte[0]);

        public FileWatch(File file)
        {
            this.file = requireNonNull(file, "file is null");
        }

        public File getFile()
        {
            return file;
        }

        public boolean updateState()
                throws IOException
        {
            // only check contents if length or modified time changed
            long newLastModified = file.lastModified();
            long newLength = file.length();
            if (lastModified == newLastModified && length == newLength) {
                return false;
            }

            // update stats
            lastModified = newLastModified;
            length = newLength;

            // check if contents changed
            HashCode newHashCode = Files.hash(file, sha256());
            if (Objects.equals(hashCode, newHashCode)) {
                return false;
            }
            hashCode = newHashCode;
            return true;
        }
    }

    private static class SslContextHolder
    {
        private final SslContext sslContext;
        private final UncheckedIOException exception;

        public SslContextHolder(SslContext sslContext)
        {
            this.sslContext = requireNonNull(sslContext, "sslContext is null");
            this.exception = null;
        }

        public SslContextHolder(UncheckedIOException exception)
        {
            this.exception = requireNonNull(exception, "exception is null");
            this.sslContext = null;
        }

        public SslContext getSslContext()
        {
            if (exception != null) {
                throw exception;
            }
            return sslContext;
        }
    }
}
