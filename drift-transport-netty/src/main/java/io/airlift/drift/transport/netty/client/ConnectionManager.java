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
import io.airlift.drift.transport.netty.codec.Protocol;
import io.airlift.drift.transport.netty.codec.Transport;
import io.airlift.drift.transport.netty.ssl.SslContextFactory.SslContextParameters;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.netty.channel.Channel;
import io.netty.util.concurrent.Future;

import java.io.Closeable;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

interface ConnectionManager
        extends Closeable
{
    Future<Channel> getConnection(ConnectionParameters connectionParameters, HostAndPort address);

    void returnConnection(Channel connection);

    @Override
    void close();

    class ConnectionParameters
    {
        private final Transport transport;
        private final Protocol protocol;
        private final DataSize maxFrameSize;

        private final Duration connectTimeout;
        private final Duration requestTimeout;

        private final Optional<HostAndPort> socksProxy;
        private final Optional<SslContextParameters> sslContextParameters;

        public ConnectionParameters(
                Transport transport,
                Protocol protocol,
                DataSize maxFrameSize,
                Duration connectTimeout,
                Duration requestTimeout,
                Optional<HostAndPort> socksProxy,
                Optional<SslContextParameters> sslContextParameters)
        {
            this.transport = requireNonNull(transport, "transport is null");
            this.protocol = requireNonNull(protocol, "protocol is null");
            this.maxFrameSize = requireNonNull(maxFrameSize, "maxFrameSize is null");
            this.connectTimeout = requireNonNull(connectTimeout, "connectTimeout is null");
            this.requestTimeout = requireNonNull(requestTimeout, "requestTimeout is null");
            this.socksProxy = requireNonNull(socksProxy, "socksProxy is null");
            this.sslContextParameters = requireNonNull(sslContextParameters, "sslContextParameters is null");
        }

        public Transport getTransport()
        {
            return transport;
        }

        public Protocol getProtocol()
        {
            return protocol;
        }

        public DataSize getMaxFrameSize()
        {
            return maxFrameSize;
        }

        public Duration getConnectTimeout()
        {
            return connectTimeout;
        }

        public Duration getRequestTimeout()
        {
            return requestTimeout;
        }

        public Optional<HostAndPort> getSocksProxy()
        {
            return socksProxy;
        }

        public Optional<SslContextParameters> getSslContextParameters()
        {
            return sslContextParameters;
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
            ConnectionParameters that = (ConnectionParameters) o;
            return transport == that.transport &&
                    protocol == that.protocol &&
                    Objects.equals(maxFrameSize, that.maxFrameSize) &&
                    Objects.equals(connectTimeout, that.connectTimeout) &&
                    Objects.equals(requestTimeout, that.requestTimeout) &&
                    Objects.equals(socksProxy, that.socksProxy) &&
                    Objects.equals(sslContextParameters, that.sslContextParameters);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(transport, protocol, maxFrameSize, connectTimeout, requestTimeout, socksProxy, sslContextParameters);
        }
    }
}
