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

import io.airlift.drift.protocol.TProtocolFactory;
import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCounted;

import javax.annotation.CheckReturnValue;

import java.util.Map;

import static java.util.Objects.requireNonNull;
import static javax.annotation.meta.When.UNKNOWN;

public class ThriftFrame
        implements ReferenceCounted
{
    private final int sequenceId;
    private final ByteBuf message;
    private final Map<String, String> headers;
    private final TProtocolFactory protocolFactory;
    private final boolean supportOutOfOrderResponse;

    public ThriftFrame(int sequenceId, ByteBuf message, Map<String, String> headers, TProtocolFactory protocolFactory, boolean supportOutOfOrderResponse)
    {
        this.sequenceId = requireNonNull(sequenceId, "sequenceId is null");
        this.message = requireNonNull(message, "message is null");
        this.headers = requireNonNull(headers, "headers is null");
        this.protocolFactory = requireNonNull(protocolFactory, "protocolFactory is null");
        this.supportOutOfOrderResponse = supportOutOfOrderResponse;
    }

    public int getSequenceId()
    {
        return sequenceId;
    }

    /**
     * @return a retained message; caller must release this buffer
     */
    public ByteBuf getMessage()
    {
        return message.retainedDuplicate();
    }

    public Map<String, String> getHeaders()
    {
        return headers;
    }

    public TProtocolFactory getProtocolFactory()
    {
        return protocolFactory;
    }

    public boolean isSupportOutOfOrderResponse()
    {
        return supportOutOfOrderResponse;
    }

    @Override
    public int refCnt()
    {
        return message.refCnt();
    }

    @Override
    public ThriftFrame retain()
    {
        message.retain();
        return this;
    }

    @Override
    public ThriftFrame retain(int increment)
    {
        message.retain(increment);
        return this;
    }

    @Override
    public ThriftFrame touch()
    {
        message.touch();
        return this;
    }

    @Override
    public ThriftFrame touch(Object hint)
    {
        message.touch(hint);
        return this;
    }

    @CheckReturnValue(when = UNKNOWN)
    @Override
    public boolean release()
    {
        return message.release();
    }

    @Override
    public boolean release(int decrement)
    {
        return message.release(decrement);
    }
}
