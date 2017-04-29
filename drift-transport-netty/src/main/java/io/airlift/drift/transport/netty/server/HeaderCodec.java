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

import io.airlift.drift.transport.netty.HeaderTransport.HeaderFrame;
import io.airlift.drift.transport.netty.HeaderTransport.HeaderTransportProtocol;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

import java.util.OptionalInt;

import static com.google.common.base.Verify.verify;
import static io.airlift.drift.transport.netty.HeaderTransport.decodeFrame;
import static io.airlift.drift.transport.netty.HeaderTransport.encodeFrame;

public class HeaderCodec
        extends ChannelDuplexHandler
{
    @Override
    public void channelRead(ChannelHandlerContext context, Object message)
            throws Exception
    {
        if (message instanceof ByteBuf) {
            ByteBuf request = (ByteBuf) message;
            if (request.isReadable()) {
                HeaderFrame headerFrame = decodeFrame(request);
                context.fireChannelRead(new ThriftFrame(
                        OptionalInt.of(headerFrame.getFrameSequenceId()),
                        headerFrame.getMessage(),
                        headerFrame.getHeaders(),
                        headerFrame.getProtocol().createProtocolFactory(),
                        headerFrame.isSupportOutOfOrderResponse()));
                return;
            }
        }
        context.fireChannelRead(message);
    }

    @Override
    public void write(ChannelHandlerContext context, Object message, ChannelPromise promise)
            throws Exception
    {
        if (message instanceof ThriftFrame) {
            ThriftFrame thriftFrame = (ThriftFrame) message;
            verify(thriftFrame.getSequenceId().isPresent(), "Sequence id not set in response frame");
            message = encodeFrame(new HeaderFrame(
                    thriftFrame.getSequenceId().getAsInt(),
                    thriftFrame.getMessage(),
                    thriftFrame.getHeaders(),
                    HeaderTransportProtocol.create(thriftFrame.getProtocolFactory()),
                    thriftFrame.isSupportOutOfOrderResponse()));
        }
        context.write(message, promise);
    }
}
