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

import io.airlift.drift.TApplicationException;
import io.airlift.drift.protocol.TProtocolFactory;
import io.airlift.drift.transport.MethodMetadata;
import io.airlift.drift.transport.netty.HeaderTransport.HeaderFrame;
import io.airlift.drift.transport.netty.HeaderTransport.HeaderTransportProtocol;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

import static com.google.common.base.Verify.verify;
import static io.airlift.drift.TApplicationException.Type.BAD_SEQUENCE_ID;

@ThreadSafe
class HeaderMessageEncoding
        implements MessageEncoding
{
    private final TProtocolFactory protocolFactory;
    private final HeaderTransportProtocol protocol;

    public HeaderMessageEncoding(TProtocolFactory protocolFactory)
    {
        this.protocol = HeaderTransportProtocol.create(protocolFactory);
        this.protocolFactory = protocolFactory;
    }

    @Override
    public ByteBuf writeRequest(ByteBufAllocator allocator, int sequenceId, MethodMetadata method, List<Object> parameters, Map<String, String> headers)
            throws Exception
    {
        ByteBuf message = MessageEncoding.encodeRequest(protocolFactory, allocator, sequenceId, method, parameters);
        return HeaderTransport.encodeFrame(new HeaderFrame(sequenceId, message, headers, HeaderTransportProtocol.create(protocolFactory), true));
    }

    @Override
    public OptionalInt extractResponseSequenceId(ByteBuf buffer)
    {
        return HeaderTransport.extractResponseSequenceId(buffer);
    }

    @Override
    public Object readResponse(ByteBuf buffer, int sequenceId, MethodMetadata method)
            throws Exception
    {
        HeaderFrame headerFrame = HeaderTransport.decodeFrame(buffer);

        // verify the frame sequence id matches the expected id
        if (headerFrame.getFrameSequenceId() != sequenceId) {
            throw new TApplicationException(BAD_SEQUENCE_ID, method.getName() + " failed: unexpected response sequence id");
        }

        // the response frame includes the out of order flag, but it doesn't matter if it matches

        // verify the server wrote the response using the same protocol
        // this could be allowed, but due to the poor performance of compact protocol, a change could be a big burden on clients
        verify(headerFrame.getProtocol() == this.protocol, "response protocol is different than request protocol");

        return MessageEncoding.decodeResponse(protocolFactory, headerFrame.getMessage(), sequenceId, method);
    }
}
