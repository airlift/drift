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

import io.airlift.drift.protocol.TProtocolReader;
import io.airlift.drift.protocol.TProtocolUtil;
import io.airlift.drift.protocol.TType;
import io.airlift.drift.transport.netty.ssl.TChannelBufferInputTransport;
import io.airlift.units.DataSize;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.TooLongFrameException;

import java.util.List;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

class ThriftUnframedDecoder
        extends ByteToMessageDecoder
{
    private final Protocol protocol;
    private final int maxFrameSize;

    public ThriftUnframedDecoder(Protocol protocol, DataSize maxFrameSize)
    {
        this.protocol = requireNonNull(protocol, "protocol is null");
        this.maxFrameSize = toIntExact(requireNonNull(maxFrameSize, "maxFrameSize is null").toBytes());
    }

    // This method is an exception to the normal reference counted rules and buffer should not be released
    @Override
    protected final void decode(ChannelHandlerContext ctx, ByteBuf buffer, List<Object> out)
    {
        int frameOffset = buffer.readerIndex();
        TChannelBufferInputTransport transport = new TChannelBufferInputTransport(buffer.retain());
        try {
            TProtocolReader protocolReader = protocol.createProtocol(transport);

            protocolReader.readMessageBegin();
            TProtocolUtil.skip(protocolReader, TType.STRUCT);
            protocolReader.readMessageEnd();

            int frameLength = buffer.readerIndex() - frameOffset;
            if (frameLength > maxFrameSize) {
                ctx.fireExceptionCaught(new TooLongFrameException("Response message exceeds max size " + maxFrameSize + ": " + frameLength + " - discarded"));
            }

            out.add(buffer.slice(frameOffset, frameLength).retain());
        }
        catch (Throwable th) {
            buffer.readerIndex(frameOffset);
        }
        finally {
            transport.release();
        }
    }
}
