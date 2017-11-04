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

import io.airlift.drift.annotations.ThriftField;
import io.airlift.drift.annotations.ThriftStruct;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;

import static com.google.common.base.MoreObjects.toStringHelper;

@ThriftStruct
public class OptionalField
{
    @ThriftField(10)
    public Optional<Boolean> aBooleanOptional = Optional.empty();
    @ThriftField(11)
    public Optional<Byte> aByteOptional = Optional.empty();
    @ThriftField(12)
    public Optional<Short> aShortOptional = Optional.empty();
    @ThriftField(13)
    public Optional<Integer> aIntegerOptional = Optional.empty();
    @ThriftField(14)
    public Optional<Long> aLongOptional = Optional.empty();
    @ThriftField(15)
    public Optional<Double> aDoubleOptional = Optional.empty();
    @ThriftField(16)
    public Optional<String> aStringOptional = Optional.empty();
    @ThriftField(17)
    public Optional<BonkField> aStructOptional = Optional.empty();
    @ThriftField(18)
    public Optional<Fruit> aEnumOptional = Optional.empty();
    @ThriftField(19)
    public Optional<Letter> aCustomEnumOptional = Optional.empty();
    @ThriftField(20)
    public OptionalDouble aOptionalDouble = OptionalDouble.empty();
    @ThriftField(21)
    public OptionalInt aOptionalInt = OptionalInt.empty();
    @ThriftField(22)
    public OptionalLong aOptionalLong = OptionalLong.empty();

    @ThriftField(110)
    public Optional<List<Boolean>> aListBooleanOptional = Optional.empty();
    @ThriftField(111)
    public Optional<List<Byte>> aListByteOptional = Optional.empty();
    @ThriftField(112)
    public Optional<List<Short>> aListShortOptional = Optional.empty();
    @ThriftField(113)
    public Optional<List<Integer>> aListIntegerOptional = Optional.empty();
    @ThriftField(114)
    public Optional<List<Long>> aListLongOptional = Optional.empty();
    @ThriftField(115)
    public Optional<List<Double>> aListDoubleOptional = Optional.empty();
    @ThriftField(116)
    public Optional<List<String>> aListStringOptional = Optional.empty();
    @ThriftField(117)
    public Optional<List<BonkField>> aListStructOptional = Optional.empty();
    @ThriftField(118)
    public Optional<List<Fruit>> aListEnumOptional = Optional.empty();
    @ThriftField(119)
    public Optional<List<Letter>> aListCustomEnumOptional = Optional.empty();

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OptionalField that = (OptionalField) o;
        return Objects.equals(aBooleanOptional, that.aBooleanOptional) &&
                Objects.equals(aByteOptional, that.aByteOptional) &&
                Objects.equals(aShortOptional, that.aShortOptional) &&
                Objects.equals(aIntegerOptional, that.aIntegerOptional) &&
                Objects.equals(aLongOptional, that.aLongOptional) &&
                Objects.equals(aDoubleOptional, that.aDoubleOptional) &&
                Objects.equals(aStringOptional, that.aStringOptional) &&
                Objects.equals(aStructOptional, that.aStructOptional) &&
                Objects.equals(aEnumOptional, that.aEnumOptional) &&
                Objects.equals(aCustomEnumOptional, that.aCustomEnumOptional) &&
                Objects.equals(aOptionalDouble, that.aOptionalDouble) &&
                Objects.equals(aOptionalInt, that.aOptionalInt) &&
                Objects.equals(aOptionalLong, that.aOptionalLong) &&
                Objects.equals(aListBooleanOptional, that.aListBooleanOptional) &&
                Objects.equals(aListByteOptional, that.aListByteOptional) &&
                Objects.equals(aListShortOptional, that.aListShortOptional) &&
                Objects.equals(aListIntegerOptional, that.aListIntegerOptional) &&
                Objects.equals(aListLongOptional, that.aListLongOptional) &&
                Objects.equals(aListDoubleOptional, that.aListDoubleOptional) &&
                Objects.equals(aListStringOptional, that.aListStringOptional) &&
                Objects.equals(aListStructOptional, that.aListStructOptional) &&
                Objects.equals(aListEnumOptional, that.aListEnumOptional) &&
                Objects.equals(aListCustomEnumOptional, that.aListCustomEnumOptional);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                aBooleanOptional,
                aByteOptional,
                aShortOptional,
                aIntegerOptional,
                aLongOptional,
                aDoubleOptional,
                aStringOptional,
                aStructOptional,
                aEnumOptional,
                aCustomEnumOptional,
                aOptionalDouble,
                aOptionalInt,
                aOptionalLong,
                aListBooleanOptional,
                aListByteOptional,
                aListShortOptional,
                aListIntegerOptional,
                aListLongOptional,
                aListDoubleOptional,
                aListStringOptional,
                aListStructOptional,
                aListEnumOptional,
                aListCustomEnumOptional);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("aBooleanOptional", aBooleanOptional)
                .add("aByteOptional", aByteOptional)
                .add("aShortOptional", aShortOptional)
                .add("aIntegerOptional", aIntegerOptional)
                .add("aLongOptional", aLongOptional)
                .add("aDoubleOptional", aDoubleOptional)
                .add("aStringOptional", aStringOptional)
                .add("aStructOptional", aStructOptional)
                .add("aEnumOptional", aEnumOptional)
                .add("aCustomEnumOptional", aCustomEnumOptional)
                .add("aOptionalDouble", aOptionalDouble)
                .add("aOptionalInt", aOptionalInt)
                .add("aOptionalLong", aOptionalLong)
                .add("aListBooleanOptional", aListBooleanOptional)
                .add("aListByteOptional", aListByteOptional)
                .add("aListShortOptional", aListShortOptional)
                .add("aListIntegerOptional", aListIntegerOptional)
                .add("aListLongOptional", aListLongOptional)
                .add("aListDoubleOptional", aListDoubleOptional)
                .add("aListStringOptional", aListStringOptional)
                .add("aListStructOptional", aListStructOptional)
                .add("aListEnumOptional", aListEnumOptional)
                .add("aListCustomEnumOptional", aListCustomEnumOptional)
                .toString();
    }
}
