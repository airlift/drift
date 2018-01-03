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
package io.airlift.drift.codec;

import io.airlift.drift.codec.internal.ProtocolReader;
import io.airlift.drift.codec.internal.ProtocolWriter;
import io.airlift.drift.codec.metadata.ThriftType;
import io.airlift.drift.protocol.TProtocolReader;
import io.airlift.drift.protocol.TProtocolWriter;

import static com.google.common.base.Preconditions.checkState;

public class UnionFieldThriftCodec
        implements ThriftCodec<UnionField>
{
    private final ThriftType type;
    private final ThriftCodec<Fruit> fruitCodec;

    public UnionFieldThriftCodec(ThriftType type, ThriftCodec<Fruit> fruitCodec)
    {
        this.type = type;
        this.fruitCodec = fruitCodec;
    }

    @Override
    public ThriftType getType()
    {
        return type;
    }

    @Override
    public UnionField read(TProtocolReader protocol)
            throws Exception
    {
        ProtocolReader reader = new ProtocolReader(protocol);

        UnionField field = new UnionField();
        reader.readStructBegin();

        boolean consumed = false;
        while (reader.nextField()) {
            checkState(!consumed, "already consumed");

            field.id = reader.getFieldId();
            switch (field.id) {
                case 1:
                    field.stringValue = reader.readStringField();
                    consumed = true;
                    break;
                case 2:
                    field.longValue = reader.readI64Field();
                    consumed = true;
                    break;
                case 3:
                    field.fruitValue = reader.readEnumField(fruitCodec);
                    consumed = true;
                    break;
                default:
                    field.id = -1;
                    reader.skipFieldData();
            }
        }
        reader.readStructEnd();

        return field;
    }

    @Override
    public void write(UnionField value, TProtocolWriter protocol)
            throws Exception
    {
        ProtocolWriter writer = new ProtocolWriter(protocol);

        writer.writeStructBegin("union");

        switch (value.id) {
            case 1:
                writer.writeStringField("stringValue", (short) 1, value.stringValue);
                break;
            case 2:
                writer.writeI64Field("longValue", (short) 2, value.longValue);
                break;
            case 3:
                writer.writeEnumField("fruitValue", (short) 3, fruitCodec, value.fruitValue);
                break;
        }
        writer.writeStructEnd();
    }
}
