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
package io.airlift.drift.codec.internal.coercion;

import com.google.errorprone.annotations.Immutable;
import io.airlift.drift.codec.ThriftCodec;
import io.airlift.drift.codec.metadata.ThriftType;
import io.airlift.drift.codec.metadata.TypeCoercion;
import io.airlift.drift.protocol.TProtocolReader;
import io.airlift.drift.protocol.TProtocolWriter;

/**
 * CoercionThriftCodec encapsulates a ThriftCodec and coerces the values to another type using
 * the supplied ThriftCoercion.
 */
@Immutable
public class CoercionThriftCodec<T>
        implements ThriftCodec<T>
{
    private final ThriftCodec<Object> codec;
    private final TypeCoercion typeCoercion;
    private final ThriftType thriftType;

    public CoercionThriftCodec(ThriftCodec<?> codec, TypeCoercion typeCoercion)
    {
        this.codec = (ThriftCodec<Object>) codec;
        this.typeCoercion = typeCoercion;
        this.thriftType = typeCoercion.getThriftType();
    }

    @Override
    public ThriftType getType()
    {
        return thriftType;
    }

    @Override
    public T read(TProtocolReader protocol)
            throws Exception
    {
        Object thriftValue = codec.read(protocol);
        return (T) typeCoercion.getFromThrift().invoke(null, thriftValue);
    }

    @Override
    public void write(T javaValue, TProtocolWriter protocol)
            throws Exception
    {
        Object thriftValue = typeCoercion.getToThrift().invoke(null, javaValue);
        codec.write(thriftValue, protocol);
    }
}
