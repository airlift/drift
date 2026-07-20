/*
 * Copyright (C) 2013 Facebook, Inc.
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
package io.airlift.drift.idl.generator;

import com.google.common.collect.ImmutableMap;
import io.airlift.drift.codec.metadata.ThriftType;

import java.util.Map;

public class ThriftTypeRenderer
{
    private final Map<ThriftType, String> typeNames;

    public ThriftTypeRenderer(Map<ThriftType, String> typeNames)
    {
        this.typeNames = ImmutableMap.copyOf(typeNames);
    }

    public String toString(ThriftType type)
    {
        return switch (type.getProtocolType()) {
            case BOOL -> "bool";
            case BYTE -> "byte";
            case DOUBLE -> "double";
            case I16 -> "i16";
            case I32 -> "i32";
            case I64 -> "i64";
            case ENUM -> prefix(type) + type.getEnumMetadata().getEnumName();
            case MAP -> String.format("map<%s, %s>", toString(type.getKeyTypeReference().get()), toString(type.getValueTypeReference().get()));
            case SET -> String.format("set<%s>", toString(type.getValueTypeReference().get()));
            case LIST -> String.format("list<%s>", toString(type.getValueTypeReference().get()));
            // VOID is encoded as a struct
            case STRUCT -> type.equals(ThriftType.VOID) ? "void" : prefix(type) + type.getStructMetadata().getStructName();
            case STRING -> "string";
            case BINARY -> "binary";
            case UNKNOWN -> throw new IllegalArgumentException("Bad protocol type: UNKNOWN");
        };
    }

    private String prefix(ThriftType type)
    {
        String result = typeNames.get(type);
        return (result == null) ? "" : (result + ".");
    }
}
