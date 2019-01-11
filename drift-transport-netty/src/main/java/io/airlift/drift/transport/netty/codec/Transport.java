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

import io.airlift.units.DataSize;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.LengthFieldPrepender;

import java.util.Optional;

import static java.lang.Math.toIntExact;

public enum Transport
{
    UNFRAMED {
        @Override
        public void addFrameHandlers(ChannelPipeline pipeline, Optional<Protocol> protocol, DataSize maxFrameSize, boolean assumeClientsSupportOutOfOrderResponses)
        {
            Protocol protocolType = protocol.orElseThrow(() -> new IllegalArgumentException("UNFRAMED transport requires a protocol"));
            pipeline.addLast("thriftUnframedDecoder", new ThriftUnframedDecoder(protocolType, maxFrameSize, assumeClientsSupportOutOfOrderResponses));
            pipeline.addLast(new SimpleFrameCodec(this, protocolType, assumeClientsSupportOutOfOrderResponses));
        }
    },
    FRAMED {
        @Override
        public void addFrameHandlers(ChannelPipeline pipeline, Optional<Protocol> protocol, DataSize maxFrameSize, boolean assumeClientsSupportOutOfOrderResponses)
        {
            Protocol protocolType = protocol.orElseThrow(() -> new IllegalArgumentException("FRAMED transport requires a protocol"));
            pipeline.addLast("frameEncoder", new LengthFieldPrepender(Integer.BYTES));
            FrameInfoDecoder frameInfoDecoder = new SimpleFrameInfoDecoder(FRAMED, protocolType, assumeClientsSupportOutOfOrderResponses);
            pipeline.addLast("thriftFramedDecoder", new ThriftFramedDecoder(frameInfoDecoder, toIntExact(maxFrameSize.toBytes())));
            pipeline.addLast(new SimpleFrameCodec(this, protocolType, assumeClientsSupportOutOfOrderResponses));
        }
    },
    HEADER {
        @Override
        public void addFrameHandlers(ChannelPipeline pipeline, Optional<Protocol> protocol, DataSize maxFrameSize, boolean assumeClientsSupportOutOfOrderResponses)
        {
            pipeline.addLast("frameEncoder", new LengthFieldPrepender(Integer.BYTES));
            pipeline.addLast("thriftFramedDecoder", new ThriftFramedDecoder(HeaderTransport::tryDecodeFrameInfo, toIntExact(maxFrameSize.toBytes())));
            pipeline.addLast(new HeaderCodec());
        }
    };

    public abstract void addFrameHandlers(ChannelPipeline pipeline, Optional<Protocol> protocol, DataSize maxFrameSize, boolean assumeClientsSupportOutOfOrderResponses);
}
