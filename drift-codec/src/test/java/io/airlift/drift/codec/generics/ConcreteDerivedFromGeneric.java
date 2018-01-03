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
package io.airlift.drift.codec.generics;

import io.airlift.drift.annotations.ThriftConstructor;
import io.airlift.drift.annotations.ThriftField;
import io.airlift.drift.annotations.ThriftStruct;

import java.util.Objects;

@ThriftStruct
public final class ConcreteDerivedFromGeneric
        extends GenericThriftStructBase<Double>
{
    private final Double concreteProperty;

    @ThriftConstructor
    public ConcreteDerivedFromGeneric(
            @ThriftField(1) Double genericProperty,
            @ThriftField(2) Double concreteProperty)
    {
        super(genericProperty);
        this.concreteProperty = concreteProperty;
    }

    @ThriftField(2)
    public Double getConcreteProperty()
    {
        return concreteProperty;
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
        if (!super.equals(o)) {
            return false;
        }
        ConcreteDerivedFromGeneric that = (ConcreteDerivedFromGeneric) o;
        return Objects.equals(concreteProperty, that.concreteProperty);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(super.hashCode(), concreteProperty);
    }
}
