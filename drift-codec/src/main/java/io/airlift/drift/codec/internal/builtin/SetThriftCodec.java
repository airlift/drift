/*
 * Copyright (C) 2012 Facebook, Inc.
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
package io.airlift.drift.codec.internal.builtin;

import io.airlift.drift.codec.ThriftCodec;
import io.airlift.drift.codec.internal.TProtocolReader;
import io.airlift.drift.codec.internal.TProtocolWriter;
import io.airlift.drift.codec.metadata.ThriftType;
import org.apache.thrift.protocol.TProtocol;

import javax.annotation.concurrent.Immutable;

import java.util.Set;

import static java.util.Objects.requireNonNull;

@Immutable
public class SetThriftCodec<T>
        implements ThriftCodec<Set<T>>
{
    private final ThriftCodec<T> elementCodec;
    private final ThriftType type;

    public SetThriftCodec(ThriftType type, ThriftCodec<T> elementCodec)
    {
        this.type = requireNonNull(type, "type is null");
        this.elementCodec = requireNonNull(elementCodec, "elementCodec is null");
    }

    @Override
    public ThriftType getType()
    {
        return type;
    }

    @Override
    public Set<T> read(TProtocol protocol)
            throws Exception
    {
        requireNonNull(protocol, "protocol is null");
        return new TProtocolReader(protocol).readSet(elementCodec);
    }

    @Override
    public void write(Set<T> value, TProtocol protocol)
            throws Exception
    {
        requireNonNull(value, "value is null");
        requireNonNull(protocol, "protocol is null");
        new TProtocolWriter(protocol).writeSet(elementCodec, value);
    }
}
