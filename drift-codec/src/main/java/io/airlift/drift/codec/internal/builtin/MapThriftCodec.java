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

import java.util.Map;

import static java.util.Objects.requireNonNull;

@Immutable
public class MapThriftCodec<K, V>
        implements ThriftCodec<Map<K, V>>
{
    private final ThriftType thriftType;
    private final ThriftCodec<K> keyCodec;
    private final ThriftCodec<V> valueCodec;

    public MapThriftCodec(ThriftType type, ThriftCodec<K> keyCodec, ThriftCodec<V> valueCodec)
    {
        this.thriftType = requireNonNull(type, "type is null");
        this.keyCodec = requireNonNull(keyCodec, "keyCodec is null");
        this.valueCodec = requireNonNull(valueCodec, "valueCodec is null");
    }

    @Override
    public ThriftType getType()
    {
        return thriftType;
    }

    @Override
    public Map<K, V> read(TProtocol protocol)
            throws Exception
    {
        requireNonNull(protocol, "protocol is null");
        return new TProtocolReader(protocol).readMap(keyCodec, valueCodec);
    }

    @Override
    public void write(Map<K, V> value, TProtocol protocol)
            throws Exception
    {
        requireNonNull(value, "value is null");
        requireNonNull(protocol, "protocol is null");
        new TProtocolWriter(protocol).writeMap(keyCodec, valueCodec, value);
    }
}
