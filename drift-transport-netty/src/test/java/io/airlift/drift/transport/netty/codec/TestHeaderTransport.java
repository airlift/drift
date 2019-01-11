/*
 * Copyright (C) 2018 Facebook, Inc.
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
package io.airlift.drift.transport.netty.codec;

import com.google.common.collect.ImmutableMap;
import io.airlift.drift.TException;
import io.airlift.drift.codec.internal.ProtocolWriter;
import io.airlift.drift.protocol.TMessage;
import io.airlift.drift.protocol.TProtocolWriter;
import io.airlift.drift.transport.netty.buffer.TestingPooledByteBufAllocator;
import io.airlift.drift.transport.netty.ssl.TChannelBufferOutputTransport;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.airlift.drift.protocol.TMessageType.CALL;
import static io.airlift.drift.protocol.TMessageType.ONEWAY;
import static io.airlift.drift.transport.netty.codec.HeaderTransport.tryDecodeFrameInfo;
import static io.airlift.drift.transport.netty.codec.Protocol.BINARY;
import static io.airlift.drift.transport.netty.codec.Protocol.FB_COMPACT;
import static io.airlift.drift.transport.netty.codec.Transport.HEADER;
import static org.testng.Assert.assertEquals;

public class TestHeaderTransport
{
    @Test
    public void testTryDecodeSequenceId()
            throws Exception
    {
        try (TestingPooledByteBufAllocator allocator = new TestingPooledByteBufAllocator()) {
            ByteBuf message = createTestFrame(allocator, "method", CALL, 0xFFAA, BINARY, true);
            try {
                assertDecodeFrameInfo(message.retainedSlice(0, 0), Optional.empty());
                assertDecodeFrameInfo(message.retainedSlice(0, 1), Optional.empty());
                assertDecodeFrameInfo(message.retainedSlice(0, 5), Optional.empty());
                assertDecodeFrameInfo(message.retainedSlice(0, 10), Optional.empty());
                assertDecodeFrameInfo(message.retainedSlice(0, 15), Optional.empty());
                assertDecodeFrameInfo(
                        message.retainedDuplicate(),
                        Optional.of(new FrameInfo("method", CALL, 0xFFAA, HEADER, BINARY, true)));
            }
            finally {
                message.release();
            }
            assertDecodeFrameInfo(
                    createTestFrame(allocator, "method1", ONEWAY, 123, FB_COMPACT, false),
                    Optional.of(new FrameInfo("method1", ONEWAY, 123, HEADER, FB_COMPACT, false)));
        }
    }

    private static void assertDecodeFrameInfo(ByteBuf message, Optional<FrameInfo> frameInfo)
    {
        try {
            assertEquals(tryDecodeFrameInfo(message), frameInfo);
        }
        finally {
            message.release();
        }
    }

    private static ByteBuf createTestFrame(ByteBufAllocator allocator, String methodName, byte messageType, int sequenceId, Protocol protocol, boolean supportOutOfOrderResponse)
            throws TException
    {
        ThriftFrame frame = new ThriftFrame(
                sequenceId,
                createTestMessage(allocator, methodName, messageType, sequenceId, protocol),
                ImmutableMap.of("header", "value"),
                HEADER,
                protocol,
                supportOutOfOrderResponse);
        return HeaderTransport.encodeFrame(frame);
    }

    private static ByteBuf createTestMessage(ByteBufAllocator allocator, String methodName, byte messageType, int sequenceId, Protocol protocol)
            throws TException
    {
        TChannelBufferOutputTransport transport = new TChannelBufferOutputTransport(allocator);
        try {
            TProtocolWriter protocolWriter = protocol.createProtocol(transport);
            protocolWriter.writeMessageBegin(new TMessage(methodName, messageType, sequenceId));

            // write the parameters
            ProtocolWriter writer = new ProtocolWriter(protocolWriter);
            writer.writeStructBegin("method_args");
            writer.writeStructEnd();

            protocolWriter.writeMessageEnd();
            return transport.getBuffer();
        }
        finally {
            transport.release();
        }
    }
}
