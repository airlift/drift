/*
 * Copyright (C) 2012 Facebook, Inc.
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
package io.airlift.drift.codec.internal.builtin;

import com.google.errorprone.annotations.Immutable;
import io.airlift.drift.codec.ThriftCodec;
import io.airlift.drift.codec.metadata.ThriftType;
import io.airlift.drift.protocol.TProtocolReader;
import io.airlift.drift.protocol.TProtocolWriter;

import static java.util.Objects.requireNonNull;

@Immutable
public class ShortThriftCodec
        implements ThriftCodec<Short>
{
    @Override
    public ThriftType getType()
    {
        return ThriftType.I16;
    }

    @Override
    public Short read(TProtocolReader protocol)
            throws Exception
    {
        requireNonNull(protocol, "protocol is null");
        return protocol.readI16();
    }

    @Override
    public void write(Short value, TProtocolWriter protocol)
            throws Exception
    {
        requireNonNull(value, "value is null");
        requireNonNull(protocol, "protocol is null");
        protocol.writeI16(value);
    }
}
