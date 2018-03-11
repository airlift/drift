/*
 * Copyright (C) 2018 Facebook, Inc.
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

import io.airlift.drift.protocol.TProtocolFactory;
import io.airlift.drift.transport.netty.server.HeaderCodec;
import io.airlift.drift.transport.netty.server.SimpleFrameCodec;
import io.airlift.units.DataSize;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

import java.util.Optional;

import static java.lang.Math.toIntExact;

public enum Transport
{
    UNFRAMED {
        @Override
        public void addFrameHandlers(ChannelPipeline pipeline, Optional<Protocol> protocol, DataSize maxFrameSize, boolean assumeClientsSupportOutOfOrderResponses)
        {
            TProtocolFactory protocolFactory = protocol.get().createProtocolFactory(this);
            pipeline.addLast("thriftUnframedDecoder", new ThriftUnframedDecoder(protocolFactory, maxFrameSize));
            pipeline.addLast(new SimpleFrameCodec(protocolFactory, assumeClientsSupportOutOfOrderResponses));
        }
    },
    FRAMED {
        @Override
        public void addFrameHandlers(ChannelPipeline pipeline, Optional<Protocol> protocol, DataSize maxFrameSize, boolean assumeClientsSupportOutOfOrderResponses)
        {
            pipeline.addLast("frameEncoder", new LengthFieldPrepender(Integer.BYTES));
            pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(toIntExact(maxFrameSize.toBytes()), 0, Integer.BYTES, 0, Integer.BYTES));
            TProtocolFactory protocolFactory = protocol.get().createProtocolFactory(this);
            pipeline.addLast(new SimpleFrameCodec(protocolFactory, assumeClientsSupportOutOfOrderResponses));
        }
    },
    HEADER {
        @Override
        public void addFrameHandlers(ChannelPipeline pipeline, Optional<Protocol> protocol, DataSize maxFrameSize, boolean assumeClientsSupportOutOfOrderResponses)
        {
            pipeline.addLast("frameEncoder", new LengthFieldPrepender(Integer.BYTES));
            pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(toIntExact(maxFrameSize.toBytes()), 0, Integer.BYTES, 0, Integer.BYTES));
            pipeline.addLast(new HeaderCodec());
        }
    };

    public abstract void addFrameHandlers(ChannelPipeline pipeline, Optional<Protocol> protocol, DataSize maxFrameSize, boolean assumeClientsSupportOutOfOrderResponses);
}
