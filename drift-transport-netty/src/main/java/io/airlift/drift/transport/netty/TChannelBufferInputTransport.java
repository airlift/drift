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

import io.netty.buffer.ByteBuf;
import org.apache.thrift.transport.TTransport;

import javax.annotation.concurrent.NotThreadSafe;

import static java.util.Objects.requireNonNull;

@NotThreadSafe
class TChannelBufferInputTransport
        extends TTransport
{
    private final ByteBuf buffer;

    public TChannelBufferInputTransport(ByteBuf buffer)
    {
        this.buffer = requireNonNull(buffer, "buffer is null");
    }

    @Override
    public boolean isOpen()
    {
        return true;
    }

    @Override
    public void open() {}

    @Override
    public void close() {}

    @Override
    public int read(byte[] buf, int off, int len)
    {
        buffer.readBytes(buf, off, len);
        return len;
    }

    @Override
    public void write(byte[] buf, int off, int len)
    {
        throw new UnsupportedOperationException();
    }
}
