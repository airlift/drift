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
package io.airlift.drift.codec.generics;

import io.airlift.drift.annotations.ThriftField;
import io.airlift.drift.annotations.ThriftStruct;

import java.util.Objects;

@ThriftStruct
public final class ConcreteThriftStructDerivedFromGenericField
        extends GenericThriftStructFieldBase<String>
{
    @ThriftField(2)
    public String concreteField;

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
        ConcreteThriftStructDerivedFromGenericField that = (ConcreteThriftStructDerivedFromGenericField) o;
        return Objects.equals(concreteField, that.concreteField);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(concreteField);
    }
}
