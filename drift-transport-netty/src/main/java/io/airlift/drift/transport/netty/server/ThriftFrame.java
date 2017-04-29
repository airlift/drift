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
package io.airlift.drift.transport.netty.server;

import io.airlift.drift.protocol.TProtocolFactory;
import io.netty.buffer.ByteBuf;

import java.util.Map;
import java.util.OptionalInt;

import static java.util.Objects.requireNonNull;

public class ThriftFrame
{
    private final OptionalInt sequenceId;
    private final ByteBuf message;
    private final Map<String, String> headers;
    private final TProtocolFactory protocolFactory;
    private final boolean supportOutOfOrderResponse;

    public ThriftFrame(OptionalInt sequenceId, ByteBuf message, Map<String, String> headers, TProtocolFactory protocolFactory, boolean supportOutOfOrderResponse)
    {
        this.sequenceId = requireNonNull(sequenceId, "sequenceId is null");
        this.message = requireNonNull(message, "message is null");
        this.headers = requireNonNull(headers, "headers is null");
        this.protocolFactory = requireNonNull(protocolFactory, "protocolFactory is null");
        this.supportOutOfOrderResponse = supportOutOfOrderResponse;
    }

    public OptionalInt getSequenceId()
    {
        return sequenceId;
    }

    public ByteBuf getMessage()
    {
        return message;
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
}
