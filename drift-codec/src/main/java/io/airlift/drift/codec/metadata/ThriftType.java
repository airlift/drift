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

import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;
import com.google.errorprone.annotations.Immutable;
import io.airlift.drift.codec.ThriftProtocolType;

import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * ThriftType contains all metadata necessary for converting the java type to and from Thrift.
 */
@Immutable
public class ThriftType
{
    public static final ThriftType BOOL = new ThriftType(ThriftProtocolType.BOOL, boolean.class);
    public static final ThriftType BYTE = new ThriftType(ThriftProtocolType.BYTE, byte.class);
    public static final ThriftType DOUBLE = new ThriftType(ThriftProtocolType.DOUBLE, double.class);
    public static final ThriftType I16 = new ThriftType(ThriftProtocolType.I16, short.class);
    public static final ThriftType I32 = new ThriftType(ThriftProtocolType.I32, int.class);
    public static final ThriftType I64 = new ThriftType(ThriftProtocolType.I64, long.class);
    public static final ThriftType STRING = new ThriftType(ThriftProtocolType.STRING, String.class);
    public static final ThriftType BINARY = new ThriftType(ThriftProtocolType.BINARY, ByteBuffer.class);
    public static final ThriftType VOID = new ThriftType(ThriftProtocolType.STRUCT, void.class);

    public static final ThriftTypeReference BOOL_REF = new DefaultThriftTypeReference(BOOL);
    public static final ThriftTypeReference BYTE_REF = new DefaultThriftTypeReference(BYTE);
    public static final ThriftTypeReference DOUBLE_REF = new DefaultThriftTypeReference(DOUBLE);
    public static final ThriftTypeReference I16_REF = new DefaultThriftTypeReference(I16);
    public static final ThriftTypeReference I32_REF = new DefaultThriftTypeReference(I32);
    public static final ThriftTypeReference I64_REF = new DefaultThriftTypeReference(I64);
    public static final ThriftTypeReference STRING_REF = new DefaultThriftTypeReference(STRING);
    public static final ThriftTypeReference BINARY_REF = new DefaultThriftTypeReference(BINARY);
    public static final ThriftTypeReference VOID_REF = new DefaultThriftTypeReference(VOID);

    public static ThriftType struct(ThriftStructMetadata structMetadata)
    {
        return new ThriftType(structMetadata);
    }

    public static <K, V> ThriftType map(ThriftType keyType, ThriftType valueType)
    {
        requireNonNull(keyType, "keyType is null");
        requireNonNull(valueType, "valueType is null");

        return map(new DefaultThriftTypeReference(keyType), new DefaultThriftTypeReference(valueType));
    }

    public static <K, V> ThriftType map(ThriftTypeReference keyTypeReference,
            ThriftTypeReference valueTypeReference)
    {
        requireNonNull(keyTypeReference, "keyTypeReference is null");
        requireNonNull(valueTypeReference, "valueTypeReference is null");

        @SuppressWarnings("serial")
        Type javaType = new TypeToken<Map<K, V>>() {}
                .where(new TypeParameter<K>() {}, (TypeToken<K>) TypeToken.of(keyTypeReference.getJavaType()))
                .where(new TypeParameter<V>() {}, (TypeToken<V>) TypeToken.of(valueTypeReference.getJavaType()))
                .getType();
        return new ThriftType(ThriftProtocolType.MAP, javaType, keyTypeReference, valueTypeReference, null);
    }

    public static <E> ThriftType set(ThriftType valueType)
    {
        requireNonNull(valueType, "valueType is null");

        return set(new DefaultThriftTypeReference(valueType));
    }

    public static <E> ThriftType set(ThriftTypeReference valueTypeReference)
    {
        requireNonNull(valueTypeReference, "valueTypeReference is null");

        @SuppressWarnings("serial")
        Type javaType = new TypeToken<Set<E>>() {}
                .where(new TypeParameter<E>() {}, (TypeToken<E>) TypeToken.of(valueTypeReference.getJavaType()))
                .getType();
        return new ThriftType(ThriftProtocolType.SET, javaType, null, valueTypeReference, null);
    }

    public static <E> ThriftType list(ThriftType valueType)
    {
        requireNonNull(valueType, "valueType is null");

        return list(new DefaultThriftTypeReference(valueType));
    }

    public static <E> ThriftType list(ThriftTypeReference valueTypeReference)
    {
        requireNonNull(valueTypeReference, "valueTypeReference is null");

        @SuppressWarnings("serial")
        Type javaType = new TypeToken<List<E>>() {}
                .where(new TypeParameter<E>() {}, (TypeToken<E>) TypeToken.of(valueTypeReference.getJavaType()))
                .getType();
        return new ThriftType(ThriftProtocolType.LIST, javaType, null, valueTypeReference, null);
    }

    public static ThriftType array(ThriftType valueType)
    {
        requireNonNull(valueType, "valueType is null");

        return array(new DefaultThriftTypeReference(valueType));
    }

    public static ThriftType array(ThriftTypeReference valueTypeReference)
    {
        requireNonNull(valueTypeReference, "valueTypeReference is null");

        Class<?> javaType = ReflectionHelper.getArrayOfType(valueTypeReference.getJavaType());
        return new ThriftType(ThriftProtocolType.LIST, javaType, null, valueTypeReference, null);
    }

    public static ThriftType optional(ThriftType valueType)
    {
        requireNonNull(valueType, "valueType is null");

        return optional(new DefaultThriftTypeReference(valueType));
    }

    public static <T> ThriftType optional(ThriftTypeReference valueTypeReference)
    {
        requireNonNull(valueTypeReference, "valueTypeReference is null");

        @SuppressWarnings("serial")
        Type javaType = new TypeToken<Optional<T>>() {}
                .where(new TypeParameter<T>() {}, (TypeToken<T>) TypeToken.of(valueTypeReference.getJavaType()))
                .getType();
        return new ThriftType(valueTypeReference.getProtocolType(), javaType, null, valueTypeReference, Optional.empty());
    }

    public static ThriftType enumType(ThriftEnumMetadata<?> enumMetadata)
    {
        requireNonNull(enumMetadata, "enumMetadata is null");
        return new ThriftType(enumMetadata);
    }

    private final ThriftProtocolType protocolType;
    private final Type javaType;
    private final ThriftTypeReference keyTypeReference;
    private final ThriftTypeReference valueTypeReference;
    private final ThriftStructMetadata structMetadata;
    private final ThriftEnumMetadata<?> enumMetadata;
    private final ThriftType uncoercedType;
    private final Object nullValue;

    private ThriftType(ThriftProtocolType protocolType, Type javaType)
    {
        requireNonNull(protocolType, "protocolType is null");
        requireNonNull(javaType, "javaType is null");

        this.protocolType = protocolType;
        this.javaType = javaType;
        keyTypeReference = null;
        valueTypeReference = null;
        structMetadata = null;
        enumMetadata = null;
        uncoercedType = null;
        nullValue = null;
    }

    private ThriftType(ThriftProtocolType protocolType,
            Type javaType,
            ThriftTypeReference keyTypeReference,
            ThriftTypeReference valueTypeReference,
            Object nullValue)
    {
        requireNonNull(protocolType, "protocolType is null");
        requireNonNull(javaType, "javaType is null");
        requireNonNull(valueTypeReference, "valueTypeReference is null");

        this.protocolType = protocolType;
        this.javaType = javaType;
        this.keyTypeReference = keyTypeReference;
        this.valueTypeReference = valueTypeReference;
        this.structMetadata = null;
        this.enumMetadata = null;
        this.uncoercedType = null;
        this.nullValue = nullValue;
    }

    private ThriftType(ThriftStructMetadata structMetadata)
    {
        requireNonNull(structMetadata, "structMetadata is null");

        this.protocolType = ThriftProtocolType.STRUCT;
        this.javaType = structMetadata.getStructType();
        keyTypeReference = null;
        valueTypeReference = null;
        this.structMetadata = structMetadata;
        this.enumMetadata = null;
        this.uncoercedType = null;
        this.nullValue = null;
    }

    private ThriftType(ThriftEnumMetadata<?> enumMetadata)
    {
        requireNonNull(enumMetadata, "enumMetadata is null");

        this.protocolType = ThriftProtocolType.ENUM;
        this.javaType = enumMetadata.getEnumClass();
        keyTypeReference = null;
        valueTypeReference = null;
        this.structMetadata = null;
        this.enumMetadata = enumMetadata;
        this.uncoercedType = null;
        this.nullValue = null;
    }

    public ThriftType(ThriftType uncoercedType, Type javaType)
    {
        this(uncoercedType, javaType, null);
    }

    public ThriftType(ThriftType uncoercedType, Type javaType, Object nullValue)
    {
        this.javaType = javaType;
        this.uncoercedType = uncoercedType;

        this.protocolType = uncoercedType.getProtocolType();
        keyTypeReference = null;
        valueTypeReference = null;
        structMetadata = null;
        enumMetadata = null;
        this.nullValue = nullValue;
    }

    public Type getJavaType()
    {
        return javaType;
    }

    public ThriftProtocolType getProtocolType()
    {
        return protocolType;
    }

    public ThriftTypeReference getKeyTypeReference()
    {
        checkState(keyTypeReference != null, "%s does not have a key", protocolType);
        return keyTypeReference;
    }

    public ThriftTypeReference getValueTypeReference()
    {
        checkState(valueTypeReference != null, "%s does not have a get", protocolType);
        return valueTypeReference;
    }

    public ThriftStructMetadata getStructMetadata()
    {
        checkState(structMetadata != null, "%s does not have struct metadata", protocolType);
        return structMetadata;
    }

    public ThriftEnumMetadata<?> getEnumMetadata()
    {
        checkState(enumMetadata != null, "%s does not have enum metadata", protocolType);
        return enumMetadata;
    }

    public boolean isCoerced()
    {
        return uncoercedType != null;
    }

    public Object getNullValue()
    {
        return nullValue;
    }

    public ThriftType coerceTo(Type javaType)
    {
        if (javaType == this.javaType) {
            return this;
        }

        checkState(
                protocolType != ThriftProtocolType.STRUCT &&
                        protocolType != ThriftProtocolType.SET &&
                        protocolType != ThriftProtocolType.LIST &&
                        protocolType != ThriftProtocolType.MAP,
                "Coercion is not supported for %s", protocolType);

        return new ThriftType(this, javaType);
    }

    public ThriftType getUncoercedType()
    {
        return uncoercedType;
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
        ThriftType that = (ThriftType) o;
        return protocolType == that.protocolType &&
                Objects.equals(javaType, that.javaType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(protocolType, javaType);
    }

    @Override
    public String toString()
    {
        String structMetadataName = (structMetadata != null) ? structMetadata.getStructClass().getName() : null;
        return toStringHelper(this).omitNullValues()
                .add("protocolType", protocolType)
                .add("javaType", javaType)
                .add("structMetadata", structMetadataName)
                .add("keyTypeReference", keyTypeReference)
                .add("valueTypeReference", valueTypeReference)
                .toString();
    }
}
