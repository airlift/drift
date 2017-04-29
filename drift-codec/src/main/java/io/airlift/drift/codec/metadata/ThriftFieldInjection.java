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

import java.lang.reflect.Field;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public class ThriftFieldInjection
        implements ThriftInjection
{
    private final short id;
    private final String name;
    private final Field field;
    private final FieldKind fieldKind;

    public ThriftFieldInjection(short id, String name, Field field, FieldKind fieldKind)
    {
        this.name = requireNonNull(name, "name is null");
        this.field = requireNonNull(field, "field is null");
        this.fieldKind = requireNonNull(fieldKind, "fieldKind is null");

        switch (fieldKind) {
            case THRIFT_FIELD:
                // Nothing to check
                break;
            case THRIFT_UNION_ID:
                checkArgument(id == Short.MIN_VALUE, "fieldId must be Short.MIN_VALUE for thrift_union_id");
                break;
        }

        this.id = id;
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

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("id", id)
                .add("name", name)
                .add("field", field)
                .add("fieldKind", fieldKind)
                .toString();
    }
}
