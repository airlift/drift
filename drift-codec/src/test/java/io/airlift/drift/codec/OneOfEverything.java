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
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;

@ThriftStruct
public final class OneOfEverything
{
    @ThriftField(1)
    public boolean aBoolean;
    @ThriftField(2)
    public byte aByte;
    @ThriftField(3)
    public short aShort;
    @ThriftField(4)
    public int aInt;
    @ThriftField(5)
    public long aLong;
    @ThriftField(6)
    public double aDouble;
    @ThriftField(7)
    public String aString;
    @ThriftField(8)
    public BonkField aStruct;
    @ThriftField(9)
    public Fruit aEnum;
    @ThriftField(10)
    public Letter aCustomEnum;

    @ThriftField(11)
    public Set<Boolean> aBooleanSet;
    @ThriftField(12)
    public Set<Byte> aByteSet;
    @ThriftField(13)
    public Set<Short> aShortSet;
    @ThriftField(14)
    public Set<Integer> aIntegerSet;
    @ThriftField(15)
    public Set<Long> aLongSet;
    @ThriftField(16)
    public Set<Double> aDoubleSet;
    @ThriftField(17)
    public Set<String> aStringSet;
    @ThriftField(18)
    public Set<BonkField> aStructSet;
    @ThriftField(19)
    public Set<Fruit> aEnumSet;
    @ThriftField(20)
    public Set<Letter> aCustomEnumSet;

    @ThriftField(21)
    public List<Boolean> aBooleanList;
    @ThriftField(22)
    public List<Byte> aByteList;
    @ThriftField(23)
    public List<Short> aShortList;
    @ThriftField(24)
    public List<Integer> aIntegerList;
    @ThriftField(25)
    public List<Long> aLongList;
    @ThriftField(26)
    public List<Double> aDoubleList;
    @ThriftField(27)
    public List<String> aStringList;
    @ThriftField(28)
    public List<BonkField> aStructList;
    @ThriftField(29)
    public List<Fruit> aEnumList;
    @ThriftField(30)
    public List<Letter> aCustomEnumList;

    @ThriftField(31)
    public Map<String, Boolean> aBooleanValueMap;
    @ThriftField(32)
    public Map<String, Byte> aByteValueMap;
    @ThriftField(33)
    public Map<String, Short> aShortValueMap;
    @ThriftField(34)
    public Map<String, Integer> aIntegerValueMap;
    @ThriftField(35)
    public Map<String, Long> aLongValueMap;
    @ThriftField(36)
    public Map<String, Double> aDoubleValueMap;
    @ThriftField(37)
    public Map<String, String> aStringValueMap;
    @ThriftField(38)
    public Map<String, BonkField> aStructValueMap;
    @ThriftField(39)
    public Map<String, Fruit> aEnumValueMap;
    @ThriftField(40)
    public Map<String, Letter> aCustomEnumValueMap;

    @ThriftField(41)
    public Map<Boolean, String> aBooleanKeyMap;
    @ThriftField(42)
    public Map<Byte, String> aByteKeyMap;
    @ThriftField(43)
    public Map<Short, String> aShortKeyMap;
    @ThriftField(44)
    public Map<Integer, String> aIntegerKeyMap;
    @ThriftField(45)
    public Map<Long, String> aLongKeyMap;
    @ThriftField(46)
    public Map<Double, String> aDoubleKeyMap;
    @ThriftField(47)
    public Map<String, String> aStringKeyMap;
    @ThriftField(48)
    public Map<BonkField, String> aStructKeyMap;
    @ThriftField(49)
    public Map<Fruit, String> aEnumKeyMap;
    @ThriftField(50)
    public Map<Letter, String> aCustomEnumKeyMap;

    @ThriftField(51)
    public Optional<Boolean> aBooleanOptional = Optional.empty();
    @ThriftField(52)
    public Optional<Byte> aByteOptional = Optional.empty();
    @ThriftField(53)
    public Optional<Short> aShortOptional = Optional.empty();
    @ThriftField(54)
    public Optional<Integer> aIntegerOptional = Optional.empty();
    @ThriftField(55)
    public Optional<Long> aLongOptional = Optional.empty();
    @ThriftField(56)
    public Optional<Double> aDoubleOptional = Optional.empty();
    @ThriftField(57)
    public Optional<String> aStringOptional = Optional.empty();
    @ThriftField(58)
    public Optional<BonkField> aStructOptional = Optional.empty();
    @ThriftField(59)
    public Optional<Fruit> aEnumOptional = Optional.empty();
    @ThriftField(60)
    public Optional<Letter> aCustomEnumOptional = Optional.empty();

    @ThriftField(70)
    public UnionField aUnion;
    @ThriftField(71)
    public Set<UnionField> aUnionSet;
    @ThriftField(72)
    public List<UnionField> aUnionList;
    @ThriftField(73)
    public Map<UnionField, String> aUnionKeyMap;
    @ThriftField(74)
    public Map<String, UnionField> aUnionValueMap;

    @ThriftField(100)
    public Set<List<Map<String, BonkField>>> aSetOfListsOfMaps;
    @ThriftField(101)
    public Map<List<String>, Set<BonkField>> aMapOfListToSet;

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OneOfEverything that = (OneOfEverything) o;
        return aBoolean == that.aBoolean &&
                aByte == that.aByte &&
                aShort == that.aShort &&
                aInt == that.aInt &&
                aLong == that.aLong &&
                Double.compare(that.aDouble, aDouble) == 0 &&
                Objects.equals(aString, that.aString) &&
                Objects.equals(aStruct, that.aStruct) &&
                aEnum == that.aEnum &&
                aCustomEnum == that.aCustomEnum &&
                Objects.equals(aBooleanSet, that.aBooleanSet) &&
                Objects.equals(aByteSet, that.aByteSet) &&
                Objects.equals(aShortSet, that.aShortSet) &&
                Objects.equals(aIntegerSet, that.aIntegerSet) &&
                Objects.equals(aLongSet, that.aLongSet) &&
                Objects.equals(aDoubleSet, that.aDoubleSet) &&
                Objects.equals(aStringSet, that.aStringSet) &&
                Objects.equals(aStructSet, that.aStructSet) &&
                Objects.equals(aEnumSet, that.aEnumSet) &&
                Objects.equals(aCustomEnumSet, that.aCustomEnumSet) &&
                Objects.equals(aBooleanList, that.aBooleanList) &&
                Objects.equals(aByteList, that.aByteList) &&
                Objects.equals(aShortList, that.aShortList) &&
                Objects.equals(aIntegerList, that.aIntegerList) &&
                Objects.equals(aLongList, that.aLongList) &&
                Objects.equals(aDoubleList, that.aDoubleList) &&
                Objects.equals(aStringList, that.aStringList) &&
                Objects.equals(aStructList, that.aStructList) &&
                Objects.equals(aEnumList, that.aEnumList) &&
                Objects.equals(aCustomEnumList, that.aCustomEnumList) &&
                Objects.equals(aBooleanValueMap, that.aBooleanValueMap) &&
                Objects.equals(aByteValueMap, that.aByteValueMap) &&
                Objects.equals(aShortValueMap, that.aShortValueMap) &&
                Objects.equals(aIntegerValueMap, that.aIntegerValueMap) &&
                Objects.equals(aLongValueMap, that.aLongValueMap) &&
                Objects.equals(aDoubleValueMap, that.aDoubleValueMap) &&
                Objects.equals(aStringValueMap, that.aStringValueMap) &&
                Objects.equals(aStructValueMap, that.aStructValueMap) &&
                Objects.equals(aEnumValueMap, that.aEnumValueMap) &&
                Objects.equals(aCustomEnumValueMap, that.aCustomEnumValueMap) &&
                Objects.equals(aBooleanKeyMap, that.aBooleanKeyMap) &&
                Objects.equals(aByteKeyMap, that.aByteKeyMap) &&
                Objects.equals(aShortKeyMap, that.aShortKeyMap) &&
                Objects.equals(aIntegerKeyMap, that.aIntegerKeyMap) &&
                Objects.equals(aLongKeyMap, that.aLongKeyMap) &&
                Objects.equals(aDoubleKeyMap, that.aDoubleKeyMap) &&
                Objects.equals(aStringKeyMap, that.aStringKeyMap) &&
                Objects.equals(aStructKeyMap, that.aStructKeyMap) &&
                Objects.equals(aEnumKeyMap, that.aEnumKeyMap) &&
                Objects.equals(aCustomEnumKeyMap, that.aCustomEnumKeyMap) &&
                Objects.equals(aBooleanOptional, that.aBooleanOptional) &&
                Objects.equals(aByteOptional, that.aByteOptional) &&
                Objects.equals(aShortOptional, that.aShortOptional) &&
                Objects.equals(aIntegerOptional, that.aIntegerOptional) &&
                Objects.equals(aLongOptional, that.aLongOptional) &&
                Objects.equals(aDoubleOptional, that.aDoubleOptional) &&
                Objects.equals(aStringOptional, that.aStringOptional) &&
                Objects.equals(aStructOptional, that.aStructOptional) &&
                Objects.equals(aEnumOptional, that.aEnumOptional) &&
                Objects.equals(aCustomEnumOptional, that.aCustomEnumOptional) &&
                Objects.equals(aUnion, that.aUnion) &&
                Objects.equals(aUnionSet, that.aUnionSet) &&
                Objects.equals(aUnionList, that.aUnionList) &&
                Objects.equals(aUnionKeyMap, that.aUnionKeyMap) &&
                Objects.equals(aUnionValueMap, that.aUnionValueMap) &&
                Objects.equals(aSetOfListsOfMaps, that.aSetOfListsOfMaps) &&
                Objects.equals(aMapOfListToSet, that.aMapOfListToSet);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                aBoolean,
                aByte,
                aShort,
                aInt,
                aLong,
                aDouble,
                aString,
                aStruct,
                aEnum,
                aCustomEnum,
                aBooleanSet,
                aByteSet,
                aShortSet,
                aIntegerSet,
                aLongSet,
                aDoubleSet,
                aStringSet,
                aStructSet,
                aEnumSet,
                aCustomEnumSet,
                aBooleanList,
                aByteList,
                aShortList,
                aIntegerList,
                aLongList,
                aDoubleList,
                aStringList,
                aStructList,
                aEnumList,
                aCustomEnumList,
                aBooleanValueMap,
                aByteValueMap,
                aShortValueMap,
                aIntegerValueMap,
                aLongValueMap,
                aDoubleValueMap,
                aStringValueMap,
                aStructValueMap,
                aEnumValueMap,
                aCustomEnumValueMap,
                aBooleanKeyMap,
                aByteKeyMap,
                aShortKeyMap,
                aIntegerKeyMap,
                aLongKeyMap,
                aDoubleKeyMap,
                aStringKeyMap,
                aStructKeyMap,
                aEnumKeyMap,
                aCustomEnumKeyMap,
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
                aUnion,
                aUnionSet,
                aUnionList,
                aUnionKeyMap,
                aUnionValueMap,
                aSetOfListsOfMaps,
                aMapOfListToSet);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("aBoolean", aBoolean)
                .add("aByte", aByte)
                .add("aShort", aShort)
                .add("aInt", aInt)
                .add("aLong", aLong)
                .add("aDouble", aDouble)
                .add("aString", aString)
                .add("aStruct", aStruct)
                .add("aEnum", aEnum)
                .add("aCustomEnum", aCustomEnum)
                .add("aBooleanSet", aBooleanSet)
                .add("aByteSet", aByteSet)
                .add("aShortSet", aShortSet)
                .add("aIntegerSet", aIntegerSet)
                .add("aLongSet", aLongSet)
                .add("aDoubleSet", aDoubleSet)
                .add("aStringSet", aStringSet)
                .add("aStructSet", aStructSet)
                .add("aEnumSet", aEnumSet)
                .add("aCustomEnumSet", aCustomEnumSet)
                .add("aBooleanList", aBooleanList)
                .add("aByteList", aByteList)
                .add("aShortList", aShortList)
                .add("aIntegerList", aIntegerList)
                .add("aLongList", aLongList)
                .add("aDoubleList", aDoubleList)
                .add("aStringList", aStringList)
                .add("aStructList", aStructList)
                .add("aEnumList", aEnumList)
                .add("aCustomEnumList", aCustomEnumList)
                .add("aBooleanValueMap", aBooleanValueMap)
                .add("aByteValueMap", aByteValueMap)
                .add("aShortValueMap", aShortValueMap)
                .add("aIntegerValueMap", aIntegerValueMap)
                .add("aLongValueMap", aLongValueMap)
                .add("aDoubleValueMap", aDoubleValueMap)
                .add("aStringValueMap", aStringValueMap)
                .add("aStructValueMap", aStructValueMap)
                .add("aEnumValueMap", aEnumValueMap)
                .add("aCustomEnumValueMap", aCustomEnumValueMap)
                .add("aBooleanKeyMap", aBooleanKeyMap)
                .add("aByteKeyMap", aByteKeyMap)
                .add("aShortKeyMap", aShortKeyMap)
                .add("aIntegerKeyMap", aIntegerKeyMap)
                .add("aLongKeyMap", aLongKeyMap)
                .add("aDoubleKeyMap", aDoubleKeyMap)
                .add("aStringKeyMap", aStringKeyMap)
                .add("aStructKeyMap", aStructKeyMap)
                .add("aEnumKeyMap", aEnumKeyMap)
                .add("aCustomEnumKeyMap", aCustomEnumKeyMap)
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
                .add("aUnion", aUnion)
                .add("aUnionSet", aUnionSet)
                .add("aUnionList", aUnionList)
                .add("aUnionKeyMap", aUnionKeyMap)
                .add("aUnionValueMap", aUnionValueMap)
                .add("aSetOfListsOfMaps", aSetOfListsOfMaps)
                .add("aMapOfListToSet", aMapOfListToSet)
                .toString();
    }
}
