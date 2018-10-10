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
package io.airlift.drift.idl.generator;

import io.airlift.drift.annotations.ThriftField;
import io.airlift.drift.annotations.ThriftStruct;

import java.util.List;
import java.util.Map;
import java.util.Set;

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
    @ThriftField(80)
    public float aFloat;
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
    @ThriftField(81)
    public Set<Float> aFloatSet;
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
    @ThriftField(82)
    public List<Double> aFloatList;
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
    @ThriftField(83)
    public Map<String, Float> aFloatValueMap;
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
    @ThriftField(84)
    public Map<Float, String> aFloatKeyMap;
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

    @ThriftField(60)
    public UnionField aUnion;
    @ThriftField(61)
    public Set<UnionField> aUnionSet;
    @ThriftField(62)
    public List<UnionField> aUnionList;
    @ThriftField(63)
    public Map<UnionField, String> aUnionKeyMap;
    @ThriftField(64)
    public Map<String, UnionField> aUnionValueMap;

    @ThriftField(70)
    public byte[] aByteArray;

    @ThriftField(100)
    public Set<List<Map<String, BonkField>>> aSetOfListsOfMaps;
    @ThriftField(101)
    public Map<List<String>, Set<BonkField>> aMapOfListToSet;
}
