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

import io.airlift.drift.annotations.ThriftField;
import io.airlift.drift.codec.ThriftCodec;
import io.airlift.drift.codec.ThriftCodecManager;
import io.airlift.drift.codec.internal.ProtocolReader;
import io.airlift.drift.codec.internal.ProtocolWriter;
import io.airlift.drift.codec.metadata.ThriftConstructorInjection;
import io.airlift.drift.codec.metadata.ThriftFieldInjection;
import io.airlift.drift.codec.metadata.ThriftFieldMetadata;
import io.airlift.drift.codec.metadata.ThriftInjection;
import io.airlift.drift.codec.metadata.ThriftMethodInjection;
import io.airlift.drift.codec.metadata.ThriftParameterInjection;
import io.airlift.drift.codec.metadata.ThriftStructMetadata;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolException;

import javax.annotation.concurrent.Immutable;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.base.Verify.verify;
import static io.airlift.drift.codec.metadata.FieldKind.THRIFT_FIELD;

@Immutable
public class ReflectionThriftStructCodec<T>
        extends AbstractReflectionThriftCodec<T>
{
    public ReflectionThriftStructCodec(ThriftCodecManager manager, ThriftStructMetadata metadata)
    {
        super(manager, metadata);
    }

    @Override
    public T read(TProtocol protocol)
            throws Exception
    {
        ProtocolReader reader = new ProtocolReader(protocol);
        reader.readStructBegin();

        Map<Short, Object> data = new HashMap<>(metadata.getFields().size());
        while (reader.nextField()) {
            short fieldId = reader.getFieldId();

            // do we have a codec for this field
            ThriftCodec<?> codec = fields.get(fieldId);
            if (codec == null) {
                reader.skipFieldData();
                continue;
            }

            // is this field readable
            ThriftFieldMetadata field = metadata.getField(fieldId);
            if (field.isReadOnly() || field.getType() != THRIFT_FIELD) {
                reader.skipFieldData();
                continue;
            }

            // read the value
            Object value = reader.readField(codec);
            if (value == null) {
                if (field.getRequiredness() == ThriftField.Requiredness.REQUIRED) {
                    throw new TProtocolException("required field was not set");
                }
                else {
                    continue;
                }
            }

            data.put(fieldId, value);
        }
        reader.readStructEnd();

        // build the struct
        return constructStruct(data);
    }

    @Override
    public void write(T instance, TProtocol protocol)
            throws Exception
    {
        ProtocolWriter writer = new ProtocolWriter(protocol);
        writer.writeStructBegin(metadata.getStructName());

        for (ThriftFieldMetadata fieldMetadata : metadata.getFields(THRIFT_FIELD)) {
            // is the field readable?
            if (fieldMetadata.isWriteOnly()) {
                continue;
            }

            // get the field value
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
    private T constructStruct(Map<Short, Object> data)
            throws Exception
    {
        // construct instance
        Object instance;
        {
            ThriftConstructorInjection constructor = metadata.getConstructorInjection().get();
            Object[] parametersValues = new Object[constructor.getParameters().size()];
            for (ThriftParameterInjection parameter : constructor.getParameters()) {
                Object value = data.get(parameter.getId());
                parametersValues[parameter.getParameterIndex()] = value;
            }

            instance = invokeConstructor(constructor.getConstructor(), parametersValues);
        }

        // inject fields
        for (ThriftFieldMetadata fieldMetadata : metadata.getFields(THRIFT_FIELD)) {
            for (ThriftInjection injection : fieldMetadata.getInjections()) {
                if (injection instanceof ThriftFieldInjection) {
                    ThriftFieldInjection fieldInjection = (ThriftFieldInjection) injection;
                    Object value = data.get(fieldInjection.getId());
                    if (value != null) {
                        fieldInjection.getField().set(instance, value);
                    }
                }
            }
        }

        // inject methods
        for (ThriftMethodInjection methodInjection : metadata.getMethodInjections()) {
            boolean shouldInvoke = false;
            Object[] parametersValues = new Object[methodInjection.getParameters().size()];
            for (ThriftParameterInjection parameter : methodInjection.getParameters()) {
                Object value = data.get(parameter.getId());
                if (value != null) {
                    parametersValues[parameter.getParameterIndex()] = value;
                    shouldInvoke = true;
                }
            }

            if (shouldInvoke) {
                invokeMethod(methodInjection.getMethod(), instance, parametersValues);
            }
        }

        // builder method
        if (metadata.getBuilderMethod().isPresent()) {
            ThriftMethodInjection builderMethod = metadata.getBuilderMethod().get();
            Object[] parametersValues = new Object[builderMethod.getParameters().size()];
            for (ThriftParameterInjection parameter : builderMethod.getParameters()) {
                Object value = data.get(parameter.getId());
                parametersValues[parameter.getParameterIndex()] = value;
            }

            instance = invokeMethod(builderMethod.getMethod(), instance, parametersValues);
            validateCreatedInstance(metadata.getStructClass(), instance);
        }

        return (T) instance;
    }

    static void validateCreatedInstance(Class<?> clazz, Object instance)
    {
        verify(instance != null, "Builder method returned a null instance");

        verify(
                clazz.isInstance(instance),
                "Builder method returned instance of type %s, but an instance of %s is required",
                instance.getClass().getName(),
                clazz.getName());
    }

    static <T> T invokeConstructor(Constructor<T> constructor, Object[] args)
            throws Exception
    {
        try {
            return constructor.newInstance(args);
        }
        catch (InvocationTargetException e) {
            throwIfUnchecked(e.getCause());
            throwIfInstanceOf(e.getCause(), Exception.class);
            throw e;
        }
    }

    static Object invokeMethod(Method method, Object instance, Object[] args)
            throws Exception
    {
        try {
            return method.invoke(instance, args);
        }
        catch (InvocationTargetException e) {
            throwIfUnchecked(e.getCause());
            throwIfInstanceOf(e.getCause(), Exception.class);
            throw e;
        }
    }
}
