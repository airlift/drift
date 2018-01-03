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

import io.airlift.drift.annotations.ThriftConstructor;
import io.airlift.drift.annotations.ThriftField;
import io.airlift.drift.annotations.ThriftStruct;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@ThriftStruct
public class OptionalStruct
{
    private final Optional<Boolean> aBooleanOptional;
    private final Optional<Byte> aByteOptional;
    private final Optional<Short> aShortOptional;
    private final Optional<Integer> aIntegerOptional;
    private final Optional<Long> aLongOptional;
    private final Optional<Double> aDoubleOptional;
    private final Optional<String> aStringOptional;
    private final Optional<BonkField> aStructOptional;
    private final Optional<Fruit> aEnumOptional;
    private final Optional<Letter> aCustomEnumOptional;

    private final OptionalDouble aOptionalDouble;
    private final OptionalInt aOptionalInt;
    private final OptionalLong aOptionalLong;

    private final Optional<List<Boolean>> aListBooleanOptional;
    private final Optional<List<Byte>> aListByteOptional;
    private final Optional<List<Short>> aListShortOptional;
    private final Optional<List<Integer>> aListIntegerOptional;
    private final Optional<List<Long>> aListLongOptional;
    private final Optional<List<Double>> aListDoubleOptional;
    private final Optional<List<String>> aListStringOptional;
    private final Optional<List<BonkField>> aListStructOptional;
    private final Optional<List<Fruit>> aListEnumOptional;
    private final Optional<List<Letter>> aListCustomEnumOptional;

    public OptionalStruct()
    {
        aBooleanOptional = Optional.empty();
        aByteOptional = Optional.empty();
        aShortOptional = Optional.empty();
        aIntegerOptional = Optional.empty();
        aLongOptional = Optional.empty();
        aDoubleOptional = Optional.empty();
        aStringOptional = Optional.empty();
        aStructOptional = Optional.empty();
        aEnumOptional = Optional.empty();
        aCustomEnumOptional = Optional.empty();

        aOptionalDouble = OptionalDouble.empty();
        aOptionalInt = OptionalInt.empty();
        aOptionalLong = OptionalLong.empty();

        aListBooleanOptional = Optional.empty();
        aListByteOptional = Optional.empty();
        aListShortOptional = Optional.empty();
        aListIntegerOptional = Optional.empty();
        aListLongOptional = Optional.empty();
        aListDoubleOptional = Optional.empty();
        aListStringOptional = Optional.empty();
        aListStructOptional = Optional.empty();
        aListEnumOptional = Optional.empty();
        aListCustomEnumOptional = Optional.empty();
    }

    @ThriftConstructor
    public OptionalStruct(
            Optional<Boolean> aBooleanOptional,
            Optional<Byte> aByteOptional,
            Optional<Short> aShortOptional,
            Optional<Integer> aIntegerOptional,
            Optional<Long> aLongOptional,
            Optional<Double> aDoubleOptional,
            Optional<String> aStringOptional,
            Optional<BonkField> aStructOptional,
            Optional<Fruit> aEnumOptional,
            Optional<Letter> aCustomEnumOptional,

            OptionalDouble aOptionalDouble,
            OptionalInt aOptionalInt,
            OptionalLong aOptionalLong,

            Optional<List<Boolean>> aListBooleanOptional,
            Optional<List<Byte>> aListByteOptional,
            Optional<List<Short>> aListShortOptional,
            Optional<List<Integer>> aListIntegerOptional,
            Optional<List<Long>> aListLongOptional,
            Optional<List<Double>> aListDoubleOptional,
            Optional<List<String>> aListStringOptional,
            Optional<List<BonkField>> aListStructOptional,
            Optional<List<Fruit>> aListEnumOptional,
            Optional<List<Letter>> aListCustomEnumOptional)
    {
        this.aBooleanOptional = requireNonNull(aBooleanOptional, "aBooleanOptional is null");
        this.aByteOptional = requireNonNull(aByteOptional, "aByteOptional is null");
        this.aShortOptional = requireNonNull(aShortOptional, "aShortOptional is null");
        this.aIntegerOptional = requireNonNull(aIntegerOptional, "aIntegerOptional is null");
        this.aLongOptional = requireNonNull(aLongOptional, "aLongOptional is null");
        this.aDoubleOptional = requireNonNull(aDoubleOptional, "aDoubleOptional is null");
        this.aStringOptional = requireNonNull(aStringOptional, "aStringOptional is null");
        this.aStructOptional = requireNonNull(aStructOptional, "aStructOptional is null");
        this.aEnumOptional = requireNonNull(aEnumOptional, "aEnumOptional is null");
        this.aCustomEnumOptional = requireNonNull(aCustomEnumOptional, "aCustomEnumOptional is null");

        this.aOptionalDouble = requireNonNull(aOptionalDouble, "aOptionalDouble is null");
        this.aOptionalInt = requireNonNull(aOptionalInt, "aOptionalInt is null");
        this.aOptionalLong = requireNonNull(aOptionalLong, "aOptionalLong is null");

        this.aListBooleanOptional = requireNonNull(aListBooleanOptional, "aListBooleanOptional is null");
        this.aListByteOptional = requireNonNull(aListByteOptional, "aListByteOptional is null");
        this.aListShortOptional = requireNonNull(aListShortOptional, "aListShortOptional is null");
        this.aListIntegerOptional = requireNonNull(aListIntegerOptional, "aListIntegerOptional is null");
        this.aListLongOptional = requireNonNull(aListLongOptional, "aListLongOptional is null");
        this.aListDoubleOptional = requireNonNull(aListDoubleOptional, "aListDoubleOptional is null");
        this.aListStringOptional = requireNonNull(aListStringOptional, "aListStringOptional is null");
        this.aListStructOptional = requireNonNull(aListStructOptional, "aListStructOptional is null");
        this.aListEnumOptional = requireNonNull(aListEnumOptional, "aListEnumOptional is null");
        this.aListCustomEnumOptional = requireNonNull(aListCustomEnumOptional, "aListCustomEnumOptional is null");
    }

    @ThriftField(10)
    public Optional<Boolean> getABooleanOptional()
    {
        return aBooleanOptional;
    }

    @ThriftField(11)
    public Optional<Byte> getAByteOptional()
    {
        return aByteOptional;
    }

    @ThriftField(12)
    public Optional<Short> getAShortOptional()
    {
        return aShortOptional;
    }

    @ThriftField(13)
    public Optional<Integer> getAIntegerOptional()
    {
        return aIntegerOptional;
    }

    @ThriftField(14)
    public Optional<Long> getALongOptional()
    {
        return aLongOptional;
    }

    @ThriftField(15)
    public Optional<Double> getADoubleOptional()
    {
        return aDoubleOptional;
    }

    @ThriftField(16)
    public Optional<String> getAStringOptional()
    {
        return aStringOptional;
    }

    @ThriftField(17)
    public Optional<BonkField> getAStructOptional()
    {
        return aStructOptional;
    }

    @ThriftField(18)
    public Optional<Fruit> getAEnumOptional()
    {
        return aEnumOptional;
    }

    @ThriftField(19)
    public Optional<Letter> getACustomEnumOptional()
    {
        return aCustomEnumOptional;
    }

    @ThriftField(20)
    public OptionalDouble getaOptionalDouble()
    {
        return aOptionalDouble;
    }

    @ThriftField(21)
    public OptionalInt getaOptionalInt()
    {
        return aOptionalInt;
    }

    @ThriftField(22)
    public OptionalLong getaOptionalLong()
    {
        return aOptionalLong;
    }

    @ThriftField(110)
    public Optional<List<Boolean>> getAListBooleanOptional()
    {
        return aListBooleanOptional;
    }

    @ThriftField(111)
    public Optional<List<Byte>> getAListByteOptional()
    {
        return aListByteOptional;
    }

    @ThriftField(112)
    public Optional<List<Short>> getAListShortOptional()
    {
        return aListShortOptional;
    }

    @ThriftField(113)
    public Optional<List<Integer>> getAListIntegerOptional()
    {
        return aListIntegerOptional;
    }

    @ThriftField(114)
    public Optional<List<Long>> getAListLongOptional()
    {
        return aListLongOptional;
    }

    @ThriftField(115)
    public Optional<List<Double>> getAListDoubleOptional()
    {
        return aListDoubleOptional;
    }

    @ThriftField(116)
    public Optional<List<String>> getAListStringOptional()
    {
        return aListStringOptional;
    }

    @ThriftField(117)
    public Optional<List<BonkField>> getAListStructOptional()
    {
        return aListStructOptional;
    }

    @ThriftField(118)
    public Optional<List<Fruit>> getAListEnumOptional()
    {
        return aListEnumOptional;
    }

    @ThriftField(119)
    public Optional<List<Letter>> getAListCustomEnumOptional()
    {
        return aListCustomEnumOptional;
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
        OptionalStruct that = (OptionalStruct) o;
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
