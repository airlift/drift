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

import io.airlift.drift.transport.TTransport;
import io.netty.buffer.ByteBuf;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
class TChannelBufferOutputTransport
        implements TTransport
{
    private final ByteBuf outputBuffer;

    public TChannelBufferOutputTransport(ByteBuf outputBuffer)
    {
        this.outputBuffer = outputBuffer;
    }

    public ByteBuf getOutputBuffer()
    {
        return outputBuffer;
    }

    @Override
    public void write(byte[] buf, int off, int len)
    {
        outputBuffer.writeBytes(buf, off, len);
    }

    @Override
    public void read(byte[] buf, int off, int len)
    {
        throw new UnsupportedOperationException();
    }
}
