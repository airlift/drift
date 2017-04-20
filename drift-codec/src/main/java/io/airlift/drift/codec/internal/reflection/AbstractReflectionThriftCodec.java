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

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSortedMap;
import io.airlift.drift.codec.ThriftCodec;
import io.airlift.drift.codec.ThriftCodecManager;
import io.airlift.drift.codec.metadata.ThriftExtraction;
import io.airlift.drift.codec.metadata.ThriftFieldExtractor;
import io.airlift.drift.codec.metadata.ThriftFieldMetadata;
import io.airlift.drift.codec.metadata.ThriftMethodExtractor;
import io.airlift.drift.codec.metadata.ThriftStructMetadata;
import io.airlift.drift.codec.metadata.ThriftType;

import java.lang.reflect.InvocationTargetException;
import java.util.SortedMap;

import static io.airlift.drift.codec.metadata.FieldKind.THRIFT_FIELD;

public abstract class AbstractReflectionThriftCodec<T>
        implements ThriftCodec<T>
{
    protected final ThriftStructMetadata metadata;
    protected final SortedMap<Short, ThriftCodec<?>> fields;

    protected AbstractReflectionThriftCodec(ThriftCodecManager manager, ThriftStructMetadata metadata)
    {
        this.metadata = metadata;

        ImmutableSortedMap.Builder<Short, ThriftCodec<?>> fields = ImmutableSortedMap.naturalOrder();
        for (ThriftFieldMetadata fieldMetadata : metadata.getFields(THRIFT_FIELD)) {
            fields.put(fieldMetadata.getId(), manager.getCodec(fieldMetadata.getThriftType()));
        }
        this.fields = fields.build();
    }

    @Override
    public ThriftType getType()
    {
        return ThriftType.struct(metadata);
    }

    protected Object getFieldValue(Object instance, ThriftFieldMetadata field)
            throws Exception
    {
        try {
            if (field.getExtraction().isPresent()) {
                ThriftExtraction extraction = field.getExtraction().get();
                if (extraction instanceof ThriftFieldExtractor) {
                    ThriftFieldExtractor thriftFieldExtractor = (ThriftFieldExtractor) extraction;
                    return thriftFieldExtractor.getField().get(instance);
                }
                else if (extraction instanceof ThriftMethodExtractor) {
                    ThriftMethodExtractor thriftMethodExtractor = (ThriftMethodExtractor) extraction;
                    return thriftMethodExtractor.getMethod().invoke(instance);
                }
                throw new IllegalAccessException("Unsupported field extractor type " + extraction.getClass().getName());
            }
            throw new IllegalAccessException("No extraction present for " + field);
        }
        catch (InvocationTargetException e) {
            if (e.getTargetException() != null) {
                Throwables.propagateIfInstanceOf(e.getTargetException(), Exception.class);
            }
            throw e;
        }
    }
}
