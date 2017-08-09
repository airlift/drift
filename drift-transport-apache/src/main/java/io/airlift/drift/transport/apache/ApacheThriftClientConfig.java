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
package io.airlift.drift.transport.apache;

import com.google.common.net.HostAndPort;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MaxDataSize;
import io.airlift.units.MinDuration;

import javax.validation.constraints.NotNull;

import java.io.File;

import static io.airlift.drift.transport.apache.ApacheThriftClientConfig.Protocol.BINARY;
import static io.airlift.drift.transport.apache.ApacheThriftClientConfig.Transport.FRAMED;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class ApacheThriftClientConfig
{
    public enum Transport
    {
        UNFRAMED, FRAMED, HEADER
    }

    public enum Protocol
    {
        BINARY, COMPACT
    }

    private Transport transport = FRAMED;
    private Protocol protocol = BINARY;
    private DataSize maxFrameSize = new DataSize(16, MEGABYTE);
    private DataSize maxStringSize = new DataSize(16, MEGABYTE);

    private Duration connectTimeout = new Duration(500, MILLISECONDS);
    private Duration requestTimeout = new Duration(1, MINUTES);

    private HostAndPort socksProxy;

    private boolean sslEnabled;
    private File trustCertificate;
    private File key;
    private String keyPassword;

    @NotNull
    public Transport getTransport()
    {
        return transport;
    }

    @Config("thrift.client.transport")
    public ApacheThriftClientConfig setTransport(Transport transport)
    {
        this.transport = transport;
        return this;
    }

    @NotNull
    public Protocol getProtocol()
    {
        return protocol;
    }

    @Config("thrift.client.protocol")
    public ApacheThriftClientConfig setProtocol(Protocol protocol)
    {
        this.protocol = protocol;
        return this;
    }

    @NotNull
    @MinDuration("1ms")
    public Duration getConnectTimeout()
    {
        return connectTimeout;
    }

    @Config("thrift.client.connect-timeout")
    public ApacheThriftClientConfig setConnectTimeout(Duration connectTimeout)
    {
        this.connectTimeout = connectTimeout;
        return this;
    }

    @NotNull
    @MinDuration("1ms")
    public Duration getRequestTimeout()
    {
        return requestTimeout;
    }

    @Config("thrift.client.request-timeout")
    public ApacheThriftClientConfig setRequestTimeout(Duration requestTimeout)
    {
        this.requestTimeout = requestTimeout;
        return this;
    }

    public HostAndPort getSocksProxy()
    {
        return socksProxy;
    }

    @Config("thrift.client.socks-proxy")
    public ApacheThriftClientConfig setSocksProxy(HostAndPort socksProxy)
    {
        this.socksProxy = socksProxy;
        return this;
    }

    @MaxDataSize("1023MB")
    public DataSize getMaxFrameSize()
    {
        return maxFrameSize;
    }

    @Config("thrift.client.max-frame-size")
    public ApacheThriftClientConfig setMaxFrameSize(DataSize maxFrameSize)
    {
        this.maxFrameSize = maxFrameSize;
        return this;
    }

    @MaxDataSize("1023MB")
    public DataSize getMaxStringSize()
    {
        return maxStringSize;
    }

    @Config("thrift.client.max-string-size")
    public ApacheThriftClientConfig setMaxStringSize(DataSize maxFrameSize)
    {
        this.maxStringSize = maxFrameSize;
        return this;
    }

    public boolean isSslEnabled()
    {
        return sslEnabled;
    }

    @Config("thrift.client.ssl.enabled")
    public ApacheThriftClientConfig setSslEnabled(boolean sslEnabled)
    {
        this.sslEnabled = sslEnabled;
        return this;
    }

    public File getTrustCertificate()
    {
        return trustCertificate;
    }

    @Config("thrift.client.ssl.trust-certificate")
    public ApacheThriftClientConfig setTrustCertificate(File trustCertificate)
    {
        this.trustCertificate = trustCertificate;
        return this;
    }

    public File getKey()
    {
        return key;
    }

    @Config("thrift.client.ssl.key")
    public ApacheThriftClientConfig setKey(File key)
    {
        this.key = key;
        return this;
    }

    public String getKeyPassword()
    {
        return keyPassword;
    }

    @ConfigSecuritySensitive
    @Config("thrift.client.ssl.key-password")
    public ApacheThriftClientConfig setKeyPassword(String keyPassword)
    {
        this.keyPassword = keyPassword;
        return this;
    }
}
