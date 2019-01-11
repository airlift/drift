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

import io.airlift.drift.TException;
import io.airlift.drift.protocol.TMessage;
import io.airlift.drift.protocol.TProtocol;
import io.airlift.drift.transport.netty.ssl.TChannelBufferInputTransport;
import io.netty.buffer.ByteBuf;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class SimpleFrameInfoDecoder
        implements FrameInfoDecoder
{
    private final Transport transportType;
    private final Protocol protocolType;
    private final boolean assumeClientsSupportOutOfOrderResponses;

    public SimpleFrameInfoDecoder(Transport transportType, Protocol protocolType, boolean assumeClientsSupportOutOfOrderResponses)
    {
        this.transportType = requireNonNull(transportType, "transportType is null");
        this.protocolType = requireNonNull(protocolType, "protocolType is null");
        this.assumeClientsSupportOutOfOrderResponses = assumeClientsSupportOutOfOrderResponses;
    }

    @Override
    public Optional<FrameInfo> tryDecodeFrameInfo(ByteBuf buffer)
    {
        TChannelBufferInputTransport transport = new TChannelBufferInputTransport(buffer.retainedDuplicate());
        try {
            TProtocol protocol = protocolType.createProtocol(transport);
            TMessage message;
            try {
                message = protocol.readMessageBegin();
            }
            catch (TException | RuntimeException e) {
                // not enough bytes in the input to decode sequence id
                return Optional.empty();
            }
            return Optional.of(new FrameInfo(
                    message.getName(),
                    message.getType(),
                    message.getSequenceId(),
                    transportType,
                    protocolType,
                    assumeClientsSupportOutOfOrderResponses));
        }
        finally {
            transport.release();
        }
    }
}
