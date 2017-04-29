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
import io.airlift.drift.annotations.ThriftField;
import io.airlift.drift.annotations.ThriftUnion;
import io.airlift.drift.annotations.ThriftUnionId;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

@ThriftUnion("Union")
public final class UnionMethod
{
    private Object value;
    @ThriftUnionId
    public int type;

    public UnionMethod()
    {
    }

    @ThriftField
    public void setData(String stringValue, Long longValue, Fruit fruitValue)
    {
        this.value = stringValue;
        this.type = 1;
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
        UnionMethod that = (UnionMethod) o;
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
}
