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
        switch (type.getProtocolType()) {
            case BOOL:
                return "bool";
            case BYTE:
                return "byte";
            case FLOAT:
                return "float";
            case DOUBLE:
                return "double";
            case I16:
                return "i16";
            case I32:
                return "i32";
            case I64:
                return "i64";
            case ENUM:
                return prefix(type) + type.getEnumMetadata().getEnumName();
            case MAP:
                return String.format("map<%s, %s>", toString(type.getKeyTypeReference().get()), toString(type.getValueTypeReference().get()));
            case SET:
                return String.format("set<%s>", toString(type.getValueTypeReference().get()));
            case LIST:
                return String.format("list<%s>", toString(type.getValueTypeReference().get()));
            case STRUCT:
                // VOID is encoded as a struct
                return type.equals(ThriftType.VOID) ? "void" : prefix(type) + type.getStructMetadata().getStructName();
            case STRING:
                return "string";
            case BINARY:
                return "binary";
        }
        throw new IllegalStateException("Bad protocol type" + type.getProtocolType());
    }

    private String prefix(ThriftType type)
    {
        String result = typeNames.get(type);
        return (result == null) ? "" : (result + ".");
    }
}
