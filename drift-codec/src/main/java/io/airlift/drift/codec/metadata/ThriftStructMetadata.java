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
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.reflect.TypeToken;
import com.google.errorprone.annotations.Immutable;

import java.lang.reflect.Type;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.Maps.uniqueIndex;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

@Immutable
public class ThriftStructMetadata
{
    public enum MetadataType
    {
        STRUCT, UNION
    }

    private final String structName;

    private final Map<String, String> idlAnnotations;
    private final MetadataType metadataType;
    private final Optional<ThriftMethodInjection> builderMethod;
    private final ImmutableList<String> documentation;

    private final SortedMap<Short, ThriftFieldMetadata> fields;

    private final Optional<ThriftConstructorInjection> constructorInjection;

    private final List<ThriftMethodInjection> methodInjections;
    private final Type structType;
    private final Type builderType;

    public ThriftStructMetadata(
            String structName,
            Map<String, String> idlAnnotations,
            Type structType,
            Type builderType,
            MetadataType metadataType,
            Optional<ThriftMethodInjection> builderMethod,
            List<String> documentation,
            List<ThriftFieldMetadata> fields,
            Optional<ThriftConstructorInjection> constructorInjection,
            List<ThriftMethodInjection> methodInjections)
    {
        this.builderType = builderType;
        this.builderMethod = requireNonNull(builderMethod, "builderMethod is null");
        this.structName = requireNonNull(structName, "structName is null");
        this.idlAnnotations = requireNonNull(idlAnnotations, "idlAnnotations is null");
        this.metadataType = requireNonNull(metadataType, "metadataType is null");
        this.structType = requireNonNull(structType, "structType is null");
        this.constructorInjection = requireNonNull(constructorInjection, "constructorInjection is null");
        this.documentation = ImmutableList.copyOf(requireNonNull(documentation, "documentation is null"));
        this.fields = ImmutableSortedMap.copyOf(uniqueIndex(requireNonNull(fields, "fields is null"), ThriftFieldMetadata::getId));
        this.methodInjections = ImmutableList.copyOf(requireNonNull(methodInjections, "methodInjections is null"));
    }

    public String getStructName()
    {
        return structName;
    }

    public Type getStructType()
    {
        return structType;
    }

    public Class<?> getStructClass()
    {
        return TypeToken.of(structType).getRawType();
    }

    public Type getBuilderType()
    {
        return builderType;
    }

    public Class<?> getBuilderClass()
    {
        return builderType == null ? null : TypeToken.of(builderType).getRawType();
    }

    public MetadataType getMetadataType()
    {
        return metadataType;
    }

    public Optional<ThriftMethodInjection> getBuilderMethod()
    {
        return builderMethod;
    }

    public Map<String, String> getIdlAnnotations()
    {
        return idlAnnotations;
    }

    public ThriftFieldMetadata getField(int id)
    {
        return fields.get((short) id);
    }

    public ImmutableList<String> getDocumentation()
    {
        return documentation;
    }

    public Collection<ThriftFieldMetadata> getFields(FieldKind type)
    {
        return getFields().stream()
                .filter(field -> field.getType() == type)
                .collect(toList());
    }

    public Collection<ThriftFieldMetadata> getFields()
    {
        return fields.values();
    }

    public Optional<ThriftConstructorInjection> getConstructorInjection()
    {
        return constructorInjection;
    }

    public List<ThriftMethodInjection> getMethodInjections()
    {
        return methodInjections;
    }

    public boolean isException()
    {
        return Exception.class.isAssignableFrom(getStructClass());
    }

    public boolean isUnion()
    {
        return !isException() && getMetadataType() == MetadataType.UNION;
    }

    public boolean isStruct()
    {
        return !isException() && getMetadataType() == MetadataType.STRUCT;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("structName", structName)
                .add("builderMethod", builderMethod)
                .add("fields", fields)
                .add("constructorInjection", constructorInjection)
                .add("methodInjections", methodInjections)
                .add("structType", structType)
                .add("builderType", builderType)
                .toString();
    }
}
