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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MaxDataSize;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.io.File;
import java.util.List;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class DriftNettyServerConfig
{
    private static final int DEFAULT_WORKER_THREAD_COUNT = Runtime.getRuntime().availableProcessors() * 2;

    private int port;
    private int acceptBacklog = 1024;
    private int ioThreadCount = 3;
    private int workerThreadCount = DEFAULT_WORKER_THREAD_COUNT;
    private DataSize maxFrameSize = new DataSize(16, MEGABYTE);
    private Duration requestTimeout = new Duration(1, MINUTES);

    private Duration sslContextRefreshTime = new Duration(1, MINUTES);
    private boolean allowPlaintext = true;
    private boolean sslEnabled;
    private List<String> ciphers = ImmutableList.of();
    private File trustCertificate;
    private File key;
    private String keyPassword;
    private long sessionCacheSize = 10_000;
    private Duration sessionTimeout = new Duration(1, DAYS);

    private boolean assumeClientsSupportOutOfOrderResponses;

    @Min(0)
    @Max(65535)
    public int getPort()
    {
        return port;
    }

    @Config("thrift.server.port")
    public DriftNettyServerConfig setPort(int port)
    {
        this.port = port;
        return this;
    }

    @Min(0)
    public int getAcceptBacklog()
    {
        return acceptBacklog;
    }

    /**
     * Sets the number of pending connections that the {@link java.net.ServerSocket} will
     * queue up before the server process can actually accept them. If your server may take a lot
     * of connections in a very short interval, you'll want to set this higher to avoid rejecting
     * some of the connections. Setting this to 0 will apply an implementation-specific default.
     * <p>
     * The default value is 1024.
     *
     * Actual behavior of the socket backlog is dependent on OS and JDK implementation, and it may
     * even be ignored on some systems. See JDK docs
     * <a href="http://docs.oracle.com/javase/7/docs/api/java/net/ServerSocket.html#ServerSocket%28int%2C%20int%29" target="_top">here</a>
     * for details.
     */
    @Config("thrift.server.accept-backlog")
    public DriftNettyServerConfig setAcceptBacklog(int acceptBacklog)
    {
        this.acceptBacklog = acceptBacklog;
        return this;
    }

    public int getIoThreadCount()
    {
        return ioThreadCount;
    }

    @Config("thrift.server.io-thread-count")
    public DriftNettyServerConfig setIoThreadCount(int threadCount)
    {
        this.ioThreadCount = threadCount;
        return this;
    }

    public int getWorkerThreadCount()
    {
        return workerThreadCount;
    }

    @Config("thrift.server.worker-thread-count")
    public DriftNettyServerConfig setWorkerThreadCount(int threadCount)
    {
        this.workerThreadCount = threadCount;
        return this;
    }

    @MaxDataSize("128MB")
    public DataSize getMaxFrameSize()
    {
        return maxFrameSize;
    }

    @Config("thrift.server.max-frame-size")
    public DriftNettyServerConfig setMaxFrameSize(DataSize maxFrameSize)
    {
        this.maxFrameSize = maxFrameSize;
        return this;
    }

    @NotNull
    @MinDuration("1ms")
    public Duration getRequestTimeout()
    {
        return requestTimeout;
    }

    @Config("thrift.server.request-timeout")
    public DriftNettyServerConfig setRequestTimeout(Duration requestTimeout)
    {
        this.requestTimeout = requestTimeout;
        return this;
    }

    public boolean isAllowPlaintext()
    {
        return allowPlaintext;
    }

    @Config("thrift.server.allow-plaintext")
    public DriftNettyServerConfig setAllowPlaintext(boolean allowPlaintext)
    {
        this.allowPlaintext = allowPlaintext;
        return this;
    }

    @MinDuration("1s")
    public Duration getSslContextRefreshTime()
    {
        return sslContextRefreshTime;
    }

    @Config("thrift.server.ssl-context.refresh-time")
    public DriftNettyServerConfig setSslContextRefreshTime(Duration sslContextRefreshTime)
    {
        this.sslContextRefreshTime = sslContextRefreshTime;
        return this;
    }

    public boolean isSslEnabled()
    {
        return sslEnabled;
    }

    @Config("thrift.server.ssl.enabled")
    public DriftNettyServerConfig setSslEnabled(boolean sslEnabled)
    {
        this.sslEnabled = sslEnabled;
        return this;
    }

    public File getTrustCertificate()
    {
        return trustCertificate;
    }

    @Config("thrift.server.ssl.trust-certificate")
    public DriftNettyServerConfig setTrustCertificate(File trustCertificate)
    {
        this.trustCertificate = trustCertificate;
        return this;
    }

    public File getKey()
    {
        return key;
    }

    @Config("thrift.server.ssl.key")
    public DriftNettyServerConfig setKey(File key)
    {
        this.key = key;
        return this;
    }

    public String getKeyPassword()
    {
        return keyPassword;
    }

    @Config("thrift.server.ssl.key-password")
    public DriftNettyServerConfig setKeyPassword(String keyPassword)
    {
        this.keyPassword = keyPassword;
        return this;
    }

    public long getSessionCacheSize()
    {
        return sessionCacheSize;
    }

    @Config("thrift.server.ssl.session-cache-size")
    public DriftNettyServerConfig setSessionCacheSize(long sessionCacheSize)
    {
        this.sessionCacheSize = sessionCacheSize;
        return this;
    }

    public Duration getSessionTimeout()
    {
        return sessionTimeout;
    }

    @Config("thrift.server.ssl.session-timeout")
    public DriftNettyServerConfig setSessionTimeout(Duration sessionTimeout)
    {
        this.sessionTimeout = sessionTimeout;
        return this;
    }

    public List<String> getCiphers()
    {
        return ciphers;
    }

    @Config("thrift.server.ssl.ciphers")
    public DriftNettyServerConfig setCiphers(String ciphers)
    {
        this.ciphers = Splitter
                .on(',')
                .trimResults()
                .omitEmptyStrings()
                .splitToList(requireNonNull(ciphers, "ciphers is null"));
        return this;
    }

    public boolean isAssumeClientsSupportOutOfOrderResponses()
    {
        return assumeClientsSupportOutOfOrderResponses;
    }

    @Config("thrift.server.assume-clients-support-out-of-order-responses")
    public DriftNettyServerConfig setAssumeClientsSupportOutOfOrderResponses(boolean assumeClientsSupportOutOfOrderResponses)
    {
        this.assumeClientsSupportOutOfOrderResponses = assumeClientsSupportOutOfOrderResponses;
        return this;
    }
}
