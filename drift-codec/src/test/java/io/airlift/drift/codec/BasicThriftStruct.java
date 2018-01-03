/*
 * Copyright (C) 2014 Facebook, Inc.
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

import io.airlift.drift.annotations.ThriftConstructor;
import io.airlift.drift.annotations.ThriftField;
import io.airlift.drift.annotations.ThriftStruct;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

@ThriftStruct("ThriftStructForTesting")
public final class BasicThriftStruct
{
    @ThriftConstructor
    public BasicThriftStruct(
            @ThriftField(value = 1, name = "foo") String foo,
            @ThriftField(value = 2, name = "bar") String bar,
            @ThriftField(value = 3, name = "baz") String baz,
            @ThriftField(value = 4, name = "qux") Long qux)
    {
        this.foo = foo;
        this.bar = bar;
        this.baz = baz;
        this.qux = qux;
    }

    private final String foo;

    @ThriftField(value = 1, name = "foo")
    public String getFoo()
    {
        return foo;
    }

    private final String bar;

    @ThriftField(value = 2, name = "bar")
    public String getBar()
    {
        return bar;
    }

    private final String baz;

    @ThriftField(value = 3, name = "baz")
    public String getBaz()
    {
        return baz;
    }

    private final Long qux;

    @ThriftField(value = 4, name = "qux")
    public Long getQux()
    {
        return qux;
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
        BasicThriftStruct that = (BasicThriftStruct) o;
        return Objects.equals(foo, that.foo) &&
                Objects.equals(bar, that.bar) &&
                Objects.equals(baz, that.baz) &&
                Objects.equals(qux, that.qux);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(foo, bar, baz, qux);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("foo", foo)
                .add("bar", bar)
                .add("baz", baz)
                .add("qux", qux)
                .toString();
    }
}
