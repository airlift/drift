/*
 * Copyright (C) 2014 Facebook, Inc.
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
import io.airlift.drift.codec.internal.ProtocolReader;
import io.airlift.drift.codec.internal.ProtocolWriter;
import io.airlift.drift.codec.metadata.ThriftType;
import io.airlift.drift.protocol.TProtocolReader;
import io.airlift.drift.protocol.TProtocolWriter;

import javax.annotation.concurrent.Immutable;

import static java.util.Objects.requireNonNull;

@Immutable
public class DoubleArrayThriftCodec
        implements ThriftCodec<double[]>
{
    @Override
    public ThriftType getType()
    {
        return ThriftType.array(ThriftType.DOUBLE);
    }

    @Override
    public double[] read(TProtocolReader protocol)
            throws Exception
    {
        requireNonNull(protocol, "protocol is null");
        return new ProtocolReader(protocol).readDoubleArray();
    }

    @Override
    public void write(double[] value, TProtocolWriter protocol)
            throws Exception
    {
        requireNonNull(value, "value is null");
        requireNonNull(protocol, "protocol is null");
        new ProtocolWriter(protocol).writeDoubleArray(value);
    }
}
