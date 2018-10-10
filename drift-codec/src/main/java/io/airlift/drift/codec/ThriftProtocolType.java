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

import io.airlift.drift.protocol.TType;

public enum ThriftProtocolType
{
    UNKNOWN(TType.STOP),
    BOOL(TType.BOOL),
    BYTE(TType.BYTE),
    FLOAT(TType.FLOAT),
    DOUBLE(TType.DOUBLE),
    I16(TType.I16),
    I32(TType.I32),
    I64(TType.I64),
    STRING(TType.STRING),
    STRUCT(TType.STRUCT),
    MAP(TType.MAP),
    SET(TType.SET),
    LIST(TType.LIST),
    ENUM(TType.I32),
    BINARY(TType.STRING);

    private final byte type;

    ThriftProtocolType(byte type)
    {
        this.type = type;
    }

    public byte getType()
    {
        return type;
    }
}
