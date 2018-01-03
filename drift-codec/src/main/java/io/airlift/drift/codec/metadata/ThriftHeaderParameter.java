/*
 * Copyright (C) 2017 Facebook, Inc.
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

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class ThriftHeaderParameter
{
    private final int index;
    private final String name;

    public ThriftHeaderParameter(int index, String name)
    {
        checkArgument(index >= 0, "index is negative");

        this.index = index;
        this.name = requireNonNull(name, "name is null");
    }

    public int getIndex()
    {
        return index;
    }

    public String getName()
    {
        return name;
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
        ThriftHeaderParameter that = (ThriftHeaderParameter) o;
        return index == that.index &&
                Objects.equals(name, that.name);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(index, name);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("index", index)
                .add("name", name)
                .toString();
    }
}
