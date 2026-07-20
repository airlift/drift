/*
 * Copyright (C) 2017 Facebook, Inc.
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
package io.airlift.drift.protocol;

import io.airlift.drift.TException;

public final class TProtocolUtil
{
    private TProtocolUtil() {}

    public static void skip(TProtocolReader protocol, byte type)
            throws TException
    {
        switch (type) {
            case TType.STOP -> {}
            case TType.BOOL -> protocol.readBool();
            case TType.BYTE -> protocol.readByte();
            case TType.I16 -> protocol.readI16();
            case TType.I32 -> protocol.readI32();
            case TType.I64 -> protocol.readI64();
            case TType.FLOAT -> protocol.readFloat();
            case TType.DOUBLE -> protocol.readDouble();
            case TType.STRING -> protocol.readBinary();
            case TType.STRUCT -> {
                protocol.readStructBegin();
                while (true) {
                    TField field = protocol.readFieldBegin();
                    if (field.getType() == TType.STOP) {
                        break;
                    }
                    skip(protocol, field.getType());
                    protocol.readFieldEnd();
                }
                protocol.readStructEnd();
            }
            case TType.MAP -> {
                TMap map = protocol.readMapBegin();
                for (int i = 0; i < map.getSize(); i++) {
                    skip(protocol, map.getKeyType());
                    skip(protocol, map.getValueType());
                }
                protocol.readMapEnd();
            }
            case TType.SET -> {
                TSet set = protocol.readSetBegin();
                for (int i = 0; i < set.getSize(); i++) {
                    skip(protocol, set.getType());
                }
                protocol.readSetEnd();
            }
            case TType.LIST -> {
                TList list = protocol.readListBegin();
                for (int i = 0; i < list.getSize(); i++) {
                    skip(protocol, list.getType());
                }
                protocol.readListEnd();
            }
            default -> throw new TProtocolException("Unknown type: " + type);
        }
    }
}
