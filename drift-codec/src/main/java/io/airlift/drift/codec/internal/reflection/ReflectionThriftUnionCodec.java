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
package io.airlift.drift.codec.internal.reflection;

import com.google.common.collect.Maps;
import io.airlift.drift.codec.ThriftCodec;
import io.airlift.drift.codec.ThriftCodecManager;
import io.airlift.drift.codec.internal.ProtocolReader;
import io.airlift.drift.codec.internal.ProtocolWriter;
import io.airlift.drift.codec.metadata.FieldKind;
import io.airlift.drift.codec.metadata.ThriftConstructorInjection;
import io.airlift.drift.codec.metadata.ThriftFieldInjection;
import io.airlift.drift.codec.metadata.ThriftFieldMetadata;
import io.airlift.drift.codec.metadata.ThriftInjection;
import io.airlift.drift.codec.metadata.ThriftMethodInjection;
import io.airlift.drift.codec.metadata.ThriftStructMetadata;
import io.airlift.drift.protocol.TProtocolReader;
import io.airlift.drift.protocol.TProtocolWriter;

import javax.annotation.concurrent.Immutable;

import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Maps.uniqueIndex;
import static io.airlift.drift.codec.internal.reflection.ReflectionThriftStructCodec.invokeConstructor;
import static io.airlift.drift.codec.internal.reflection.ReflectionThriftStructCodec.invokeMethod;
import static io.airlift.drift.codec.internal.reflection.ReflectionThriftStructCodec.validateCreatedInstance;
import static io.airlift.drift.codec.metadata.FieldKind.THRIFT_FIELD;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@Immutable
public class ReflectionThriftUnionCodec<T>
        extends AbstractReflectionThriftCodec<T>
{
    private final Map<Short, ThriftFieldMetadata> metadataMap;
    private final Map.Entry<ThriftFieldMetadata, ThriftCodec<?>> idField;

    public ReflectionThriftUnionCodec(ThriftCodecManager manager, ThriftStructMetadata metadata)
    {
        super(manager, metadata);

        ThriftFieldMetadata idField = getOnlyElement(metadata.getFields(FieldKind.THRIFT_UNION_ID));

        this.idField = Maps.immutableEntry(idField, manager.getCodec(idField.getThriftType()));
        requireNonNull(this.idField.getValue(), () -> "No codec for ID field found: " + idField);

        this.metadataMap = uniqueIndex(metadata.getFields(), ThriftFieldMetadata::getId);
    }

    @Override
    public T read(TProtocolReader protocol)
            throws Exception
    {
        ProtocolReader reader = new ProtocolReader(protocol);
        reader.readStructBegin();

        Map.Entry<Short, Object> data = null;
        Short fieldId = null;
        while (reader.nextField()) {
            checkState(fieldId == null, "Received Union with more than one value (seen id %s, now id %s)", fieldId, reader.getFieldId());

            fieldId = reader.getFieldId();

            // do we have a codec for this field
            ThriftCodec<?> codec = fields.get(fieldId);
            if (codec == null) {
                reader.skipFieldData();
            }
            else {
                // is this field readable
                ThriftFieldMetadata field = metadata.getField(fieldId);
                if (field.isWriteOnly() || field.getType() != THRIFT_FIELD) {
                    reader.skipFieldData();
                    continue;
                }

                // read the value
                Object value = reader.readField(codec);
                if (value == null) {
                    continue;
                }

                data = Maps.immutableEntry(fieldId, value);
            }
        }
        reader.readStructEnd();

        // build the struct
        return constructStruct(data);
    }

    @Override
    public void write(T instance, TProtocolWriter protocol)
            throws Exception
    {
        ProtocolWriter writer = new ProtocolWriter(protocol);

        Short idValue = (Short) getFieldValue(instance, idField.getKey());

        writer.writeStructBegin(metadata.getStructName());

        if (metadataMap.containsKey(idValue)) {
            ThriftFieldMetadata fieldMetadata = metadataMap.get(idValue);

            if (fieldMetadata.isReadOnly() || fieldMetadata.getType() != THRIFT_FIELD) {
                throw new IllegalStateException(format("Field %s is not readable", fieldMetadata.getName()));
            }

            Object fieldValue = getFieldValue(instance, fieldMetadata);

            // write the field
            if (fieldValue != null) {
                @SuppressWarnings("unchecked")
                ThriftCodec<Object> codec = (ThriftCodec<Object>) fields.get(fieldMetadata.getId());
                writer.writeField(fieldMetadata.getName(), fieldMetadata.getId(), codec, fieldValue);
            }
        }
        writer.writeStructEnd();
    }

    @SuppressWarnings("unchecked")
    private T constructStruct(Map.Entry<Short, Object> data)
            throws Exception
    {
        // construct instance
        Object instance = null;

        ThriftFieldMetadata fieldMetadata = null;

        if (data != null) {
            fieldMetadata = metadataMap.get(data.getKey());
            if (fieldMetadata != null && fieldMetadata.getConstructorInjection().isPresent()) {
                instance = invokeConstructor(fieldMetadata.getConstructorInjection().get().getConstructor(), new Object[] {data.getValue()});
            }
        }

        if (instance == null && metadata.getConstructorInjection().isPresent()) {
            ThriftConstructorInjection constructor = metadata.getConstructorInjection().get();
            // must be no-args
            instance = invokeConstructor(constructor.getConstructor(), new Object[0]);
        }

        if (fieldMetadata != null) {
            // inject fields
            for (ThriftInjection injection : fieldMetadata.getInjections()) {
                if (injection instanceof ThriftFieldInjection) {
                    ThriftFieldInjection fieldInjection = (ThriftFieldInjection) injection;
                    if (data.getValue() != null) {
                        fieldInjection.getField().set(instance, data.getValue());
                    }
                }
            }

            if (fieldMetadata.getMethodInjection().isPresent() && data.getValue() != null) {
                invokeMethod(fieldMetadata.getMethodInjection().get().getMethod(), instance, new Object[] {data.getValue()});
            }
        }

        if (data != null) {
            // inject id value
            for (ThriftInjection injection : idField.getKey().getInjections()) {
                if (injection instanceof ThriftFieldInjection) {
                    ThriftFieldInjection fieldInjection = (ThriftFieldInjection) injection;
                    fieldInjection.getField().set(instance, data.getKey());
                }
            }

            // builder method
            if (metadata.getBuilderMethod().isPresent()) {
                ThriftMethodInjection builderMethod = metadata.getBuilderMethod().get();
                instance = invokeMethod(builderMethod.getMethod(), instance, new Object[] {data.getValue()});
                validateCreatedInstance(metadata.getStructClass(), instance);
            }
        }

        return (T) instance;
    }
}
