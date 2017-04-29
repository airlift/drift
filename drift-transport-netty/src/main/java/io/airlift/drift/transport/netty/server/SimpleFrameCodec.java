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

import com.google.common.collect.ImmutableMap;
import io.airlift.drift.protocol.TProtocolFactory;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.EncoderException;

import java.util.OptionalInt;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class SimpleFrameCodec
        extends ChannelDuplexHandler
{
    private final TProtocolFactory protocolFactory;
    private final boolean assumeClientsSupportOutOfOrderResponses;

    public SimpleFrameCodec(TProtocolFactory protocolFactory, boolean assumeClientsSupportOutOfOrderResponses)
    {
        this.protocolFactory = requireNonNull(protocolFactory, "protocolFactory is null");
        this.assumeClientsSupportOutOfOrderResponses = assumeClientsSupportOutOfOrderResponses;
    }

    @Override
    public void channelRead(ChannelHandlerContext context, Object message)
            throws Exception
    {
        if (message instanceof ByteBuf) {
            ByteBuf request = (ByteBuf) message;
            if (request.isReadable()) {
                context.fireChannelRead(new ThriftFrame(
                        OptionalInt.empty(),
                        request,
                        ImmutableMap.of(),
                        protocolFactory,
                        assumeClientsSupportOutOfOrderResponses));
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
            if (!thriftFrame.getHeaders().isEmpty()) {
                throw new EncoderException("Headers are only supported in header transport");
            }
            context.write(thriftFrame.getMessage(), promise);
        }
        else {
            context.write(message, promise);
        }
    }
}
