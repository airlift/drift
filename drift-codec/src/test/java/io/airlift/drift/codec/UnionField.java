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
package io.airlift.drift.codec;

import io.airlift.drift.annotations.ThriftField;
import io.airlift.drift.annotations.ThriftUnion;
import io.airlift.drift.annotations.ThriftUnionId;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

@ThriftUnion("Union")
public final class UnionField
{
    @ThriftField(1)
    public String stringValue;

    @ThriftField(2)
    public Long longValue;

    @ThriftField(3)
    public Fruit fruitValue;

    @ThriftUnionId
    public short id;

    public UnionField()
    {
    }

    public UnionField(String stringValue)
    {
        this.id = 1;
        this.stringValue = stringValue;
    }

    public UnionField(Long longValue)
    {
        this.id = 2;
        this.longValue = longValue;
    }

    public UnionField(Fruit fruitValue)
    {
        this.id = 3;
        this.fruitValue = fruitValue;
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
        UnionField that = (UnionField) o;
        return id == that.id &&
                Objects.equals(stringValue, that.stringValue) &&
                Objects.equals(longValue, that.longValue) &&
                fruitValue == that.fruitValue;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(stringValue, longValue, fruitValue, id);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("id", id)
                .add("stringValue", stringValue)
                .add("longValue", longValue)
                .add("fruitValue", fruitValue)
                .toString();
    }
}
