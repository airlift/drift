/*
 * Copyright (C) 2017 Facebook, Inc.
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
import io.airlift.drift.codec.metadata.ThriftType;
import io.airlift.drift.protocol.TProtocolReader;
import io.airlift.drift.protocol.TProtocolWriter;

import javax.annotation.concurrent.Immutable;

import java.util.OptionalDouble;

import static java.util.Objects.requireNonNull;

@Immutable
public class OptionalDoubleThriftCodec
        implements ThriftCodec<OptionalDouble>
{
    @Override
    public ThriftType getType()
    {
        return new ThriftType(ThriftType.DOUBLE, OptionalDouble.class, OptionalDouble.empty());
    }

    @Override
    public OptionalDouble read(TProtocolReader protocol)
            throws Exception
    {
        requireNonNull(protocol, "protocol is null");
        return OptionalDouble.of(protocol.readDouble());
    }

    @Override
    public void write(OptionalDouble value, TProtocolWriter protocol)
            throws Exception
    {
        requireNonNull(value, "value is null");
        requireNonNull(protocol, "protocol is null");

        // write can not be called with a missing value, and instead the write should be skipped
        // after check the result from isNull
        protocol.writeDouble(value.orElseThrow((() -> new IllegalArgumentException("value is not present"))));
    }

    @Override
    public boolean isNull(OptionalDouble value)
    {
        return value == null || !value.isPresent();
    }
}
