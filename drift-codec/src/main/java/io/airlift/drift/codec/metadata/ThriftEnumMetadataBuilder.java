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

import io.airlift.drift.annotations.ThriftEnum;

import static com.google.common.base.Preconditions.checkArgument;

// This class is not thread safe
public class ThriftEnumMetadataBuilder<T extends Enum<T>>
{
    private final Class<T> enumClass;
    private final String enumName;

    public static <T extends Enum<T>> ThriftEnumMetadata<T> thriftEnumMetadata(Class<T> enumClass)
    {
        return new ThriftEnumMetadataBuilder<>(enumClass).build();
    }

    private ThriftEnumMetadataBuilder(Class<T> enumClass)
    {
        this.enumClass = enumClass;
        this.enumName = extractEnumName(enumClass);
    }

    private ThriftEnumMetadata<T> build()
    {
        return new ThriftEnumMetadata<>(enumName, enumClass);
    }

    private String extractEnumName(Class<T> enumClass)
    {
        ThriftEnum annotation = enumClass.getAnnotation(ThriftEnum.class);
        checkArgument(annotation != null, "Enum class %s is not annotated with @ThriftEnum", enumClass.getName());
        if (!annotation.value().isEmpty()) {
            return annotation.value();
        }
        return enumClass.getSimpleName();
    }
}
