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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;
import io.airlift.drift.annotations.ThriftEnumUnknownValue;
import io.airlift.drift.annotations.ThriftEnumValue;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@Immutable
public class ThriftEnumMetadata<T extends Enum<T>>
{
    private final Class<T> enumClass;
    private final Map<Integer, T> byEnumValue;
    private final Map<T, Integer> byEnumConstant;
    private final Optional<T> unknownEnumConstant;
    private final String enumName;
    private final ImmutableList<String> documentation;
    private final ImmutableMap<T, ImmutableList<String>> elementDocs;

    public ThriftEnumMetadata(
            String enumName,
            Class<T> enumClass)
            throws RuntimeException
    {
        this.enumName = requireNonNull(enumName, "enumName must not be null");
        this.enumClass = requireNonNull(enumClass, "enumClass must not be null");

        Method enumValueMethod = null;
        for (Method method : enumClass.getMethods()) {
            if (method.isAnnotationPresent(ThriftEnumValue.class)) {
                checkArgument(
                        Modifier.isPublic(method.getModifiers()),
                        "Enum class %s @ThriftEnumValue method is not public: %s",
                        enumClass.getName(),
                        method);
                checkArgument(
                        !Modifier.isStatic(method.getModifiers()),
                        "Enum class %s @ThriftEnumValue method is static: %s",
                        enumClass.getName(),
                        method);
                checkArgument(
                        method.getTypeParameters().length == 0,
                        "Enum class %s @ThriftEnumValue method has parameters: %s",
                        enumClass.getName(),
                        method);
                Class<?> returnType = method.getReturnType();
                checkArgument(
                        returnType == int.class || returnType == Integer.class,
                        "Enum class %s @ThriftEnumValue method does not return int or Integer: %s",
                        enumClass.getName(),
                        method);
                checkArgument(
                        enumValueMethod == null,
                        "Enum class %s has multiple methods annotated with @ThriftEnumValue",
                        enumClass.getName());
                enumValueMethod = method;
            }
        }
        checkArgument(
                enumValueMethod != null,
                "Enum class %s must have a method annotated with @ThriftEnumValue",
                enumClass.getName());

        Set<Integer> values = new HashSet<>();
        ImmutableMap.Builder<T, ImmutableList<String>> elementDocs = ImmutableMap.builder();
        ImmutableMap.Builder<Integer, T> byEnumValue = ImmutableMap.builder();
        ImmutableMap.Builder<T, Integer> byEnumConstant = ImmutableMap.builder();
        Optional<T> unknownEnumConstant = Optional.empty();
        for (T enumConstant : enumClass.getEnumConstants()) {
            Integer value;
            try {
                value = (Integer) enumValueMethod.invoke(enumConstant);
            }
            catch (Exception e) {
                throw new RuntimeException(format("Enum class %s element %s get value method threw an exception", enumClass.getName(), enumConstant), e);
            }
            checkArgument(value != null, "Enum class %s element %s returned null for enum value", enumClass.getName(), enumConstant);
            checkArgument(values.add(value), "Enum class %s returned duplicate enum values: %s", enumClass.getName(), value);

            byEnumValue.put(value, enumConstant);
            byEnumConstant.put(enumConstant, value);

            if (isThriftEnumUnknownValue(enumClass, enumConstant)) {
                checkArgument(!unknownEnumConstant.isPresent(), "Enum class %s has multiple constants annotated with @ThriftEnumUnknownValue", enumClass.getName());
                unknownEnumConstant = Optional.of(enumConstant);
            }
            elementDocs.put(enumConstant, ThriftCatalog.getThriftDocumentation(enumConstant));
        }
        this.byEnumValue = byEnumValue.build();
        this.byEnumConstant = byEnumConstant.build();
        this.unknownEnumConstant = unknownEnumConstant;
        this.elementDocs = elementDocs.build();
        this.documentation = ThriftCatalog.getThriftDocumentation(enumClass);
    }

    private static <T extends Enum<T>> boolean isThriftEnumUnknownValue(Class<T> enumClass, T enumConstant)
    {
        try {
            return enumConstant.getClass().getField(enumConstant.name()).isAnnotationPresent(ThriftEnumUnknownValue.class);
        }
        catch (NoSuchFieldException ignored) {
            throw new IllegalArgumentException(format("Enum class %s does not have field for enum constant: %s", enumClass.getName(), enumConstant));
        }
    }

    public String getEnumName()
    {
        return enumName;
    }

    public Class<T> getEnumClass()
    {
        return enumClass;
    }

    public Map<Integer, T> getByEnumValue()
    {
        return byEnumValue;
    }

    public Map<T, Integer> getByEnumConstant()
    {
        return byEnumConstant;
    }

    public Optional<T> getUnknownEnumConstant()
    {
        return unknownEnumConstant;
    }

    public ImmutableList<String> getDocumentation()
    {
        return documentation;
    }

    public Map<T, ImmutableList<String>> getElementsDocumentation()
    {
        return elementDocs;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ThriftEnumMetadata<?> that = (ThriftEnumMetadata<?>) o;
        return Objects.equals(enumClass, that.enumClass);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(enumClass);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("enumClass", enumClass)
                .add("byThriftValue", byEnumValue)
                .toString();
    }
}
