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

import io.airlift.units.DataSize;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class LengthPrefixedMessageFraming
        implements MessageFraming
{
    private final int maxFrameSize;

    public LengthPrefixedMessageFraming(DataSize maxFrameSize)
    {
        this.maxFrameSize = toIntExact(requireNonNull(maxFrameSize, "maxFrameSize is null").toBytes());
    }

    @Override
    public void addFrameHandlers(ChannelPipeline pipeline)
    {
        pipeline.addLast("frameEncoder", new LengthFieldPrepender(Integer.BYTES));
        pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(maxFrameSize, 0, Integer.BYTES, 0, Integer.BYTES));
    }
}
