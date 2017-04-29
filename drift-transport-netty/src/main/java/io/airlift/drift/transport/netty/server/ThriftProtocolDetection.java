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

import com.google.common.primitives.Ints;
import io.airlift.drift.protocol.TBinaryProtocol;
import io.airlift.drift.protocol.TCompactProtocol;
import io.airlift.drift.protocol.TFacebookCompactProtocol;
import io.airlift.drift.protocol.TProtocolFactory;
import io.airlift.drift.transport.netty.LengthPrefixedMessageFraming;
import io.airlift.drift.transport.netty.NoMessageFraming;
import io.airlift.units.DataSize;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ThriftProtocolDetection
        extends ByteToMessageDecoder
{
    private static final int UNFRAMED_MESSAGE_FLAG = 0x8000_0000;
    private static final int UNFRAMED_MESSAGE_MASK = 0x8000_0000;

    private static final int BINARY_PROTOCOL_VERSION_MASK = 0xFFFF_0000;
    private static final int BINARY_PROTOCOL_VERSION_1 = 0x8001_0000;

    private static final int COMPACT_PROTOCOL_VERSION_MASK = 0xFF1F_0000;
    private static final int COMPACT_PROTOCOL_VERSION_1 = 0x8201_0000;
    private static final int COMPACT_PROTOCOL_VERSION_2 = 0x8202_0000;

    // 16th and 32nd bits must be 0 to differentiate framed vs unframed.
    private static final int HEADER_MAGIC = 0x0FFF_0000;
    private static final int HEADER_MAGIC_MASK = 0xFFFF_0000;

    private static final int HTTP_POST_MAGIC = Ints.fromBytes((byte) 'P', (byte) 'O', (byte) 'S', (byte) 'T');

    private final ThriftServerHandler thriftServerHandler;
    private final DataSize maxFrameSize;
    private final boolean assumeClientsSupportOutOfOrderResponses;

    public ThriftProtocolDetection(ThriftServerHandler thriftServerHandler, DataSize maxFrameSize, boolean assumeClientsSupportOutOfOrderResponses)
    {
        this.maxFrameSize = requireNonNull(maxFrameSize, "maxFrameSize is null");
        this.thriftServerHandler = requireNonNull(thriftServerHandler, "thriftServerHandler is null");
        this.assumeClientsSupportOutOfOrderResponses = assumeClientsSupportOutOfOrderResponses;
    }

    @Override
    protected void decode(ChannelHandlerContext context, ByteBuf in, List<Object> out)
            throws Exception
    {
        // minimum bytes required to detect framing
        if (in.readableBytes() < 4) {
            return;
        }

        int magic = in.getInt(in.readerIndex());

        // HTTP not supported
        if (magic == HTTP_POST_MAGIC) {
            in.clear();
            context.close();
            return;
        }

        // Unframed transport magic starts with the high byte set, whereas framed and header
        // both start with the frame size which must be a positive int
        if ((magic & UNFRAMED_MESSAGE_MASK) == UNFRAMED_MESSAGE_FLAG) {
            Optional<TProtocolFactory> protocolFactory = detectProtocolFactory(magic);
            if (!protocolFactory.isPresent()) {
                in.clear();
                context.close();
                return;
            }

            switchToUnframedTransport(context, protocolFactory.get());
            return;
        }

        // The second int is used to determine if the transport is framed or header
        if (in.readableBytes() < 8) {
            return;
        }

        int magic2 = in.getInt(in.readerIndex() + 4);
        if ((magic2 & HEADER_MAGIC_MASK) == HEADER_MAGIC) {
            switchToHeaderTransport(context);
            return;
        }

        Optional<TProtocolFactory> protocolFactory = detectProtocolFactory(magic2);
        if (!protocolFactory.isPresent()) {
            in.clear();
            context.close();
            return;
        }

        switchToFramedTransport(context, protocolFactory.get());
    }

    private static Optional<TProtocolFactory> detectProtocolFactory(int magic)
    {
        if ((magic & BINARY_PROTOCOL_VERSION_MASK) == BINARY_PROTOCOL_VERSION_1) {
            return Optional.of(new TBinaryProtocol.Factory());
        }
        if ((magic & COMPACT_PROTOCOL_VERSION_MASK) == COMPACT_PROTOCOL_VERSION_1) {
            return Optional.of(new TCompactProtocol.Factory());
        }
        if ((magic & COMPACT_PROTOCOL_VERSION_MASK) == COMPACT_PROTOCOL_VERSION_2) {
            return Optional.of(new TFacebookCompactProtocol.Factory());
        }
        return Optional.empty();
    }

    private void switchToUnframedTransport(ChannelHandlerContext context, TProtocolFactory protocolFactory)
    {
        ChannelPipeline pipeline = context.pipeline();
        new NoMessageFraming(protocolFactory, maxFrameSize).addFrameHandlers(pipeline);
        pipeline.addLast(new SimpleFrameCodec(protocolFactory, assumeClientsSupportOutOfOrderResponses));
        pipeline.addLast(new ResponseOrderingHandler());
        pipeline.addLast(thriftServerHandler);

        // remove(this) must be last because it triggers downstream processing of the current message
        pipeline.remove(this);
    }

    private void switchToFramedTransport(ChannelHandlerContext context, TProtocolFactory protocolFactory)
    {
        ChannelPipeline pipeline = context.pipeline();
        new LengthPrefixedMessageFraming(maxFrameSize).addFrameHandlers(pipeline);
        pipeline.addLast(new SimpleFrameCodec(protocolFactory, assumeClientsSupportOutOfOrderResponses));
        pipeline.addLast(new ResponseOrderingHandler());
        pipeline.addLast(thriftServerHandler);

        // remove(this) must be last because it triggers downstream processing of the current message
        pipeline.remove(this);
    }

    private void switchToHeaderTransport(ChannelHandlerContext context)
    {
        ChannelPipeline pipeline = context.pipeline();
        new LengthPrefixedMessageFraming(maxFrameSize).addFrameHandlers(pipeline);
        pipeline.addLast(new HeaderCodec());
        pipeline.addLast(new ResponseOrderingHandler());
        pipeline.addLast(thriftServerHandler);

        // remove(this) must be last because it triggers downstream processing of the current message
        pipeline.remove(this);
    }
}
