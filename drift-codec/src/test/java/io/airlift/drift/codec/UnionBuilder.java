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

import com.google.common.base.MoreObjects.ToStringHelper;
import io.airlift.drift.annotations.ThriftConstructor;
import io.airlift.drift.annotations.ThriftField;
import io.airlift.drift.annotations.ThriftUnion;
import io.airlift.drift.annotations.ThriftUnionId;
import io.airlift.drift.codec.UnionBuilder.Builder;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

@ThriftUnion(value = "Union", builder = Builder.class)
public final class UnionBuilder
{
    private final Object value;

    @ThriftUnionId
    public final int type;

    UnionBuilder(Object value, int type)
    {
        this.value = value;
        this.type = type;
    }

    @ThriftField(1)
    public String getStringValue()
    {
        if (type != 1) {
            throw new IllegalStateException("not a stringValue");
        }
        return (String) value;
    }

    @ThriftField(2)
    public Long getLongValue()
    {
        if (type != 2) {
            throw new IllegalStateException("not a longValue");
        }
        return (Long) value;
    }

    @ThriftField(3)
    public Fruit getFruitValue()
    {
        if (type != 3) {
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
        UnionBuilder that = (UnionBuilder) o;
        return type == that.type &&
                Objects.equals(value, that.value);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(value, type);
    }

    @Override
    public String toString()
    {
        ToStringHelper helper = toStringHelper(this);

        if (type == 1) {
            helper.add("stringValue", value);
        }
        else if (type == 2) {
            helper.add("longValue", value);
        }
        else if (type == 3) {
            helper.add("fruitValue", value);
        }
        return helper.toString();
    }

    public static class Builder
    {
        private Object value;
        private int type;

        @ThriftField
        public void setStringValue(String stringValue)
        {
            this.value = stringValue;
            this.type = 1;
        }

        @ThriftField
        public void setLongValue(Long longValue)
        {
            this.value = longValue;
            this.type = 2;
        }

        @ThriftField
        public void setFruitValue(Fruit fruitValue)
        {
            this.value = fruitValue;
            this.type = 3;
        }

        @ThriftConstructor
        public UnionBuilder create()
        {
            return new UnionBuilder(value, type);
        }
    }
}
