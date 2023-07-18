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

import com.google.common.reflect.TypeToken;
import com.google.errorprone.annotations.Immutable;

import java.lang.reflect.Field;
import java.lang.reflect.Type;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public class ThriftFieldExtractor
        implements ThriftExtraction
{
    private final short id;
    private final String name;
    private final Field field;
    private final FieldKind fieldKind;
    private final Class<?> type;

    public ThriftFieldExtractor(
            short fieldId, String fieldName, FieldKind fieldKind, Field field, Type fieldType)
    {
        this.name = requireNonNull(fieldName, "name is null");
        this.field = requireNonNull(field, "field is null");
        this.fieldKind = requireNonNull(fieldKind, "type is null");
        this.type = TypeToken.of(requireNonNull(fieldType, "structType is null")).getRawType();

        switch (fieldKind) {
            case THRIFT_FIELD:
                // nothing to check
                break;
            case THRIFT_UNION_ID:
                checkArgument(fieldId == Short.MIN_VALUE, "fieldId must be Short.MIN_VALUE for thrift_union_id");
                break;
        }

        this.id = fieldId;
    }

    @Override
    public FieldKind getFieldKind()
    {
        return fieldKind;
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

    public Field getField()
    {
        return field;
    }

    public Class<?> getType()
    {
        return type;
    }

    public boolean isGeneric()
    {
        return field.getType() != field.getGenericType();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("id", id)
                .add("name", name)
                .add("field", field)
                .add("fieldKind", fieldKind)
                .add("type", type)
                .toString();
    }
}
