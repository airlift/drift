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
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.proxy.Socks4ProxyHandler;
import io.netty.handler.ssl.SslContext;

import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.function.Supplier;

class ThriftClientInitializer
        extends ChannelInitializer<SocketChannel>
{
    private final Transport transport;
    private final Protocol protocol;
    private final DataSize maxFrameSize;
    private final Duration requestTimeout;
    private final Optional<HostAndPort> socksProxyAddress;
    private final Optional<Supplier<SslContext>> sslContextSupplier;

    public ThriftClientInitializer(
            Transport transport,
            Protocol protocol,
            DataSize maxFrameSize,
            Duration requestTimeout,
            Optional<HostAndPort> socksProxyAddress,
            Optional<Supplier<SslContext>> sslContextSupplier)
    {
        this.transport = transport;
        this.protocol = protocol;
        this.maxFrameSize = maxFrameSize;
        this.requestTimeout = requestTimeout;
        this.socksProxyAddress = socksProxyAddress;
        this.sslContextSupplier = sslContextSupplier;
    }

    @Override
    protected void initChannel(SocketChannel channel)
    {
        ChannelPipeline pipeline = channel.pipeline();

        socksProxyAddress.ifPresent(socks -> pipeline.addLast(new Socks4ProxyHandler(new InetSocketAddress(socks.getHost(), socks.getPort()))));

        sslContextSupplier.ifPresent(sslContext -> pipeline.addLast(sslContext.get().newHandler(channel.alloc())));

        transport.addFrameHandlers(pipeline, Optional.of(protocol), maxFrameSize, true);

        pipeline.addLast(new ThriftClientHandler(requestTimeout, transport, protocol));
    }
}
