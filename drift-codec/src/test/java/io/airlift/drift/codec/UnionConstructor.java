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
package io.airlift.drift.codec;

import io.airlift.drift.annotations.ThriftConstructor;
import io.airlift.drift.annotations.ThriftField;
import io.airlift.drift.annotations.ThriftUnion;
import io.airlift.drift.annotations.ThriftUnionId;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

@ThriftUnion("Union")
public final class UnionConstructor
{
    private Object value;
    private short id = -1;
    private String name;

    @ThriftConstructor
    public UnionConstructor() {}

    @ThriftConstructor
    public UnionConstructor(String stringValue)
    {
        this.value = stringValue;
        this.id = 1;
        this.name = "stringValue";
    }

    @ThriftConstructor
    public UnionConstructor(Long longValue)
    {
        this.value = longValue;
        this.id = 2;
        this.name = "longValue";
    }

    @ThriftConstructor
    public UnionConstructor(Fruit fruitValue)
    {
        this.value = fruitValue;
        this.id = 3;
        this.name = "fruitValue";
    }

    @ThriftUnionId
    public short getThriftId()
    {
        return this.id;
    }

    public String getThriftName()
    {
        return this.name;
    }

    @ThriftField(value = 1, name = "stringValue")
    public String getStringValue()
    {
        if (id != 1) {
            throw new IllegalStateException("not a stringValue");
        }
        return (String) value;
    }

    @ThriftField(value = 2, name = "longValue")
    public Long getLongValue()
    {
        if (id != 2) {
            throw new IllegalStateException("not a longValue");
        }
        return (Long) value;
    }

    @ThriftField(value = 3, name = "fruitValue")
    public Fruit getFruitValue()
    {
        if (id != 3) {
            throw new IllegalStateException("not a fruitValue");
        }
        return (Fruit) value;
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
        UnionConstructor that = (UnionConstructor) o;
        return id == that.id &&
                Objects.equals(value, that.value) &&
                Objects.equals(name, that.name);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(value, id, name);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("value", value)
                .add("id", id)
                .add("name", name)
                .toString();
    }
}
