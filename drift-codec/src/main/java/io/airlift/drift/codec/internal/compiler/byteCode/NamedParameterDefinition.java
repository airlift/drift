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
package io.airlift.drift.codec.internal.compiler.byteCode;

import javax.annotation.concurrent.Immutable;

import static com.google.common.base.MoreObjects.toStringHelper;

@Immutable
public class NamedParameterDefinition
{
    public static NamedParameterDefinition arg(Class<?> type)
    {
        return new NamedParameterDefinition(null, ParameterizedType.type(type));
    }

    public static NamedParameterDefinition arg(String name, Class<?> type)
    {
        return new NamedParameterDefinition(name, ParameterizedType.type(type));
    }

    public static NamedParameterDefinition arg(ParameterizedType type)
    {
        return new NamedParameterDefinition(null, type);
    }

    public static NamedParameterDefinition arg(String name, ParameterizedType type)
    {
        return new NamedParameterDefinition(name, type);
    }

    private final String name;
    private final ParameterizedType type;

    NamedParameterDefinition(String name, ParameterizedType type)
    {
        this.name = name;
        this.type = type;
    }

    public String getName()
    {
        return name;
    }

    public ParameterizedType getType()
    {
        return type;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("type", type)
                .add("name", name)
                .toString();
    }
}
