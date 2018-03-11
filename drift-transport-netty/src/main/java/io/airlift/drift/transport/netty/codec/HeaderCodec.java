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
package io.airlift.drift.transport.netty.codec;

import io.airlift.drift.transport.netty.codec.HeaderTransport.HeaderFrame;
import io.airlift.drift.transport.netty.codec.HeaderTransport.HeaderTransportProtocol;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

import java.util.OptionalInt;

import static com.google.common.base.Verify.verify;

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
                context.fireChannelRead(toThriftFrame(HeaderTransport.decodeFrame(request)));
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
            context.write(HeaderTransport.encodeFrame(toHeaderFrame((ThriftFrame) message)), promise);
        }
        else {
            context.write(message, promise);
        }
    }

    /**
     * Create a new ThriftFrame from a HeaderFrame transferring the reference ownership.
     * @param headerFrame frame to transform; reference count ownership is transferred to this method
     * @return the translated frame; caller is responsible for releasing this object
     */
    private static ThriftFrame toThriftFrame(HeaderFrame headerFrame)
    {
        try {
            return new ThriftFrame(
                    OptionalInt.of(headerFrame.getFrameSequenceId()),
                    headerFrame.getMessage(),
                    headerFrame.getHeaders(),
                    headerFrame.getProtocol().createProtocolFactory(),
                    headerFrame.isSupportOutOfOrderResponse());
        }
        finally {
            headerFrame.release();
        }
    }

    /**
     * Create a new HeaderFrame from a ThriftFrame transferring the reference ownership.
     * @param thriftFrame frame to transform; reference count ownership is transferred to this method
     * @return the translated frame; caller is responsible for releasing this object
     */
    private static HeaderFrame toHeaderFrame(ThriftFrame thriftFrame)
    {
        try {
            verify(thriftFrame.getSequenceId().isPresent(), "Sequence id not set in response frame");
            return new HeaderFrame(
                    thriftFrame.getSequenceId().getAsInt(),
                    thriftFrame.getMessage(),
                    thriftFrame.getHeaders(),
                    HeaderTransportProtocol.create(thriftFrame.getProtocolFactory()),
                    thriftFrame.isSupportOutOfOrderResponse());
        }
        finally {
            thriftFrame.release();
        }
    }
}
