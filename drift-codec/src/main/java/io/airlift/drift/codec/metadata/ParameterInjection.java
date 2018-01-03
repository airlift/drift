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
package io.airlift.drift.codec.metadata;

import io.airlift.drift.annotations.ThriftField;

import java.lang.reflect.Type;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.drift.codec.metadata.FieldKind.THRIFT_FIELD;
import static io.airlift.drift.codec.metadata.ReflectionHelper.resolveFieldType;
import static java.util.Objects.requireNonNull;

class ParameterInjection
        extends Injection
{
    private final int parameterIndex;
    private final String extractedName;
    private final Type parameterJavaType;
    private final Type thriftStructType;

    ParameterInjection(Type thriftStructType, int parameterIndex, ThriftField annotation, String extractedName, Type parameterJavaType)
    {
        super(annotation, THRIFT_FIELD);
        this.thriftStructType = thriftStructType;
        requireNonNull(parameterJavaType, "parameterJavaType is null");

        this.parameterIndex = parameterIndex;
        this.extractedName = extractedName;
        this.parameterJavaType = parameterJavaType;
        if (void.class.equals(parameterJavaType)) {
            throw new AssertionError();
        }
        checkArgument(getName() != null || extractedName != null, "Parameter must have an explicit name or an extractedName");
    }

    public int getParameterIndex()
    {
        return parameterIndex;
    }

    @Override
    public String extractName()
    {
        return extractedName;
    }

    @Override
    public Type getJavaType()
    {
        return resolveFieldType(thriftStructType, parameterJavaType);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("parameterIndex", parameterIndex)
                .add("extractedName", extractedName)
                .add("parameterJavaType", parameterJavaType)
                .toString();
    }
}
