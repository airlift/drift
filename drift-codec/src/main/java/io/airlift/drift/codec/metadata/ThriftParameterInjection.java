/*
 * Copyright (C) 2012 Facebook, Inc.
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
package io.airlift.drift.codec.metadata;

import javax.annotation.concurrent.Immutable;

import java.lang.reflect.Type;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public class ThriftParameterInjection
        implements ThriftInjection
{
    private final short id;
    private final String name;
    private final int parameterIndex;
    private final Type javaType;

    public ThriftParameterInjection(
            short id,
            String name,
            int parameterIndex,
            Type javaType)
    {
        checkArgument(parameterIndex >= 0, "parameterIndex is negative");

        this.javaType = requireNonNull(javaType, "javaType is null");
        this.name = requireNonNull(name, "name is null");

        this.id = id;
        this.parameterIndex = parameterIndex;
    }

    @Override
    public short getId()
    {
        return id;
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public FieldKind getFieldKind()
    {
        return FieldKind.THRIFT_FIELD;
    }

    public int getParameterIndex()
    {
        return parameterIndex;
    }

    public Type getJavaType()
    {
        return javaType;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("id", id)
                .add("name", name)
                .add("parameterIndex", parameterIndex)
                .add("javaType", javaType)
                .toString();
    }
}
