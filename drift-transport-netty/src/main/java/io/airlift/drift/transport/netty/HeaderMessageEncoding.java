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
package io.airlift.drift.transport.netty;

import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.airlift.drift.TApplicationException;
import io.airlift.drift.protocol.TBinaryProtocol;
import io.airlift.drift.protocol.TCompactProtocol;
import io.airlift.drift.protocol.TFacebookCompactProtocol;
import io.airlift.drift.protocol.TProtocolFactory;
import io.airlift.drift.transport.MethodMetadata;
import io.airlift.drift.transport.TTransportException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.OptionalInt;

import static com.google.common.base.Verify.verify;
import static io.airlift.drift.TApplicationException.Type.BAD_SEQUENCE_ID;
import static java.nio.charset.StandardCharsets.UTF_8;

@ThreadSafe
class HeaderMessageEncoding
        implements MessageEncoding
{
    private static final int HEADER_MAGIC = 0x0FFF;

    private static final int HEADER_SEQUENCE_ID_OFFSET = 4;

    private static final int PROTOCOL_BINARY = 0;
    private static final int PROTOCOL_COMPACT = 2;

    private static final int FLAG_SUPPORT_OUT_OF_ORDER = 1;

    private static final int NORMAL_HEADERS = 1;
    private static final int PERSISTENT_HEADERS = 1;

    private final TProtocolFactory protocolFactory;
    private final int protocolId;

    // GZip compression is currently not supported since this is rarely desired for an RPC system
    @SuppressFBWarnings("SS_SHOULD_BE_STATIC")
    private final boolean gzip = false;

    public HeaderMessageEncoding(TProtocolFactory protocolFactory)
    {
        if (protocolFactory instanceof TBinaryProtocol.Factory) {
            protocolId = PROTOCOL_BINARY;
        }
        else if (protocolFactory instanceof TCompactProtocol.Factory || protocolFactory instanceof TFacebookCompactProtocol.Factory) {
            protocolId = PROTOCOL_COMPACT;
        }
        else {
            throw new IllegalArgumentException("Unknown protocol: " + protocolFactory.getClass().getName());
        }
        this.protocolFactory = protocolFactory;
    }

    @Override
    public ByteBuf writeRequest(ByteBufAllocator allocator, int sequenceId, MethodMetadata method, List<Object> parameters, Map<String, String> headers)
            throws Exception
    {
        ByteBuf message = MessageEncoding.encodeRequest(protocolFactory, allocator, sequenceId, method, parameters);

        //
        // describe the encoding (Thrift protocol, compression info)
        ByteBuf encodingInfo = Unpooled.buffer(3);
        encodingInfo.writeByte(protocolId);
        // number of "transforms"
        encodingInfo.writeByte(gzip ? 1 : 0);
        if (gzip) {
            // Note this is actually a vint, but there are only 3 headers for now
            encodingInfo.writeByte(0x01);
        }

        // headers
        ByteBuf encodedHeaders = encodeHeaders(headers);

        // Padding - header size must be a multiple of 4
        int headerSize = encodingInfo.readableBytes() + encodedHeaders.readableBytes();
        ByteBuf padding = getPadding(headerSize);
        headerSize += padding.readableBytes();

        // frame header (magic, flags, sequenceId, headerSize
        ByteBuf frameHeader = Unpooled.buffer(12);
        frameHeader.writeShort(HEADER_MAGIC);
        frameHeader.writeShort(FLAG_SUPPORT_OUT_OF_ORDER);
        frameHeader.writeInt(sequenceId);
        frameHeader.writeShort(headerSize >> 2);

        return Unpooled.wrappedBuffer(
                frameHeader,
                encodingInfo,
                encodedHeaders,
                padding,
                message);
    }

    private static ByteBuf getPadding(int headerSize)
    {
        int paddingSize = 4 - headerSize % 4;
        ByteBuf padding = Unpooled.buffer(paddingSize);
        padding.writeZero(paddingSize);
        return padding;
    }

    private static ByteBuf encodeHeaders(Map<String, String> headers)
    {
        if (headers.isEmpty()) {
            return Unpooled.EMPTY_BUFFER;
        }

        // 1 bytes for header type, 5 for header count vint, and 5 for each header key and value length vint
        int estimatedSize = 1 + 5 + (headers.size() * 10);
        for (Entry<String, String> entry : headers.entrySet()) {
            // assume the key and value are ASCII
            estimatedSize += entry.getKey().length() + entry.getValue().length();
        }

        ByteBuf headersBuffer = Unpooled.buffer(estimatedSize);
        // non persistent header
        headersBuffer.writeByte(0x01);
        writeVint(headersBuffer, headers.size());
        for (Entry<String, String> entry : headers.entrySet()) {
            writeString(headersBuffer, entry.getKey());
            writeString(headersBuffer, entry.getValue());
        }
        return headersBuffer;
    }

    private static void writeString(ByteBuf out, String value)
    {
        byte[] bytes = value.getBytes(UTF_8);
        writeVint(out, bytes.length);
        out.writeBytes(bytes);
    }

    private static void writeVint(ByteBuf out, int n)
    {
        while (true) {
            if ((n & ~0x7F) == 0) {
                out.writeByte(n);
                return;
            }

            out.writeByte(n | 0x80);
            n >>>= 7;
        }
    }

    @Override
    public OptionalInt extractResponseSequenceId(ByteBuf buffer)
    {
        if (buffer.readableBytes() < HEADER_SEQUENCE_ID_OFFSET + Integer.BYTES) {
            return OptionalInt.empty();
        }
        return OptionalInt.of(buffer.getInt(buffer.readerIndex() + HEADER_SEQUENCE_ID_OFFSET));
    }

    @Override
    @SuppressFBWarnings("DLS_DEAD_LOCAL_STORE")
    public Object readResponse(ByteBuf buffer, int sequenceId, MethodMetadata method)
            throws Exception
    {
        short magic = buffer.readShort();
        verify(magic == HEADER_MAGIC, "Unexpected response header magic");
        short flags = buffer.readShort();
        verify(flags == 1, "Unexpected response header flags");

        int frameSequenceId = buffer.readInt();
        if (frameSequenceId != sequenceId) {
            throw new TApplicationException(BAD_SEQUENCE_ID, method.getName() + " failed: out of sequence response");
        }
        int headerSize = buffer.readShort() << 2;

        ByteBuf messageHeader = buffer.readBytes(headerSize);
        int protocolId = messageHeader.readUnsignedByte();
        verify(protocolId == this.protocolId, "response protocol is different than request protocol");

        int numberOfTransforms = messageHeader.readUnsignedByte();
        verify(numberOfTransforms < 128, "Too many transforms for response");

        boolean gzipCompressed = false;
        for (int i = 0; i < numberOfTransforms; i++) {
            int transform = messageHeader.readUnsignedByte();
            verify(transform == 0x01, "Unsupported response transform");
            gzipCompressed = true;
        }

        // Currently we ignore response headers from the server because there is no API to fetch the headers
        Map<String, String> normalHeaders = decodeHeaders(NORMAL_HEADERS, messageHeader);
        Map<String, String> persistentHeaders = decodeHeaders(PERSISTENT_HEADERS, messageHeader);

        ByteBuf message = buffer.readBytes(buffer.readableBytes());
        if (gzipCompressed) {
            // todo decompress
            throw new TTransportException("gzip compression not implemented");
        }

        return MessageEncoding.decodeResponse(protocolFactory, message, sequenceId, method);
    }

    private static Map<String, String> decodeHeaders(int expectedHeadersType, ByteBuf messageHeader)
    {
        if (messageHeader.readableBytes() == 0) {
            return ImmutableMap.of();
        }

        byte headersType = messageHeader.readByte();
        if (headersType != expectedHeadersType) {
            return ImmutableMap.of();
        }

        ImmutableMap.Builder<String, String> headers = ImmutableMap.builder();
        int headerCount = readVarint(messageHeader);
        for (int i = 0; i < headerCount; i++) {
            String key = readString(messageHeader);
            String value = readString(messageHeader);
            headers.put(key, value);
        }
        return headers.build();
    }

    private static String readString(ByteBuf messageHeader)
    {
        int length = readVarint(messageHeader);
        return messageHeader.readBytes(length).toString(UTF_8);
    }

    private static int readVarint(ByteBuf messageHeader)
    {
        int result = 0;
        int shift = 0;

        while (true) {
            byte b = messageHeader.readByte();
            result |= (b & 0x7f) << shift;
            if ((b & 0x80) != 0x80) {
                break;
            }
            shift += 7;
        }

        return result;
    }
}
