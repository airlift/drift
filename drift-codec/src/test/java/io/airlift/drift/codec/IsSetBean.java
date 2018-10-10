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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.drift.annotations.ThriftField;
import io.airlift.drift.annotations.ThriftStruct;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.nio.charset.StandardCharsets.UTF_8;

@ThriftStruct
public final class IsSetBean
{
    public static IsSetBean createEmpty()
    {
        return new IsSetBean();
    }

    public static IsSetBean createFull()
    {
        IsSetBean isSetBean = new IsSetBean();
        isSetBean.aBoolean = false;
        isSetBean.aByte = 0;
        isSetBean.aShort = 0;
        isSetBean.aInteger = 0;
        isSetBean.aLong = 0L;
        isSetBean.aFloat = 0.0f;
        isSetBean.aDouble = 0.0d;
        isSetBean.aString = "";
        isSetBean.aStruct = new BonkField();
        isSetBean.aSet = ImmutableSet.of();
        isSetBean.aList = ImmutableList.of();
        isSetBean.aMap = ImmutableMap.of();

        return isSetBean;
    }

    public Boolean aBoolean;
    public boolean isBooleanSet;

    public Byte aByte;
    public boolean isByteSet;

    public Short aShort;
    public boolean isShortSet;

    public Integer aInteger;
    public boolean isIntegerSet;

    public Long aLong;
    public boolean isLongSet;

    public Float aFloat;
    public boolean isFloatSet;

    public Double aDouble;
    public boolean isDoubleSet;

    public String aString;
    public boolean isStringSet;

    public BonkField aStruct;
    public boolean isStructSet;

    public Set<String> aSet;
    public boolean isSetSet;

    public List<String> aList;
    public boolean isListSet;

    public Map<String, String> aMap;
    public boolean isMapSet;

    @ThriftField(20)
    public ByteBuffer field = ByteBuffer.wrap("empty".getBytes(UTF_8));

    @ThriftField(1)
    public Boolean getABoolean()
    {
        return aBoolean;
    }

    @ThriftField
    public void setABoolean(Boolean aBoolean)
    {
        this.isBooleanSet = true;
        this.aBoolean = aBoolean;
    }

    @ThriftField(2)
    public Byte getAByte()
    {
        return aByte;
    }

    @ThriftField
    public void setAByte(Byte aByte)
    {
        this.isByteSet = true;
        this.aByte = aByte;
    }

    @ThriftField(3)
    public Short getAShort()
    {
        return aShort;
    }

    @ThriftField
    public void setAShort(Short aShort)
    {
        this.isShortSet = true;
        this.aShort = aShort;
    }

    @ThriftField(4)
    public Integer getAInteger()
    {
        return aInteger;
    }

    @ThriftField
    public void setAInteger(Integer aInteger)
    {
        this.isIntegerSet = true;
        this.aInteger = aInteger;
    }

    @ThriftField(5)
    public Long getALong()
    {
        return aLong;
    }

    @ThriftField
    public void setALong(Long aLong)
    {
        this.isLongSet = true;
        this.aLong = aLong;
    }

    @ThriftField(12)
    public Float getAFloat()
    {
        return aFloat;
    }

    @ThriftField
    public void setAFloat(Float aFloat)
    {
        this.isFloatSet = true;
        this.aFloat = aFloat;
    }

    @ThriftField(6)
    public Double getADouble()
    {
        return aDouble;
    }

    @ThriftField
    public void setADouble(Double aDouble)
    {
        this.isDoubleSet = true;
        this.aDouble = aDouble;
    }

    @ThriftField(7)
    public String getAString()
    {
        return aString;
    }

    @ThriftField
    public void setAString(String aString)
    {
        this.isStringSet = true;
        this.aString = aString;
    }

    @ThriftField(8)
    public BonkField getAStruct()
    {
        return aStruct;
    }

    @ThriftField
    public void setAStruct(BonkField aStruct)
    {
        this.isStructSet = true;
        this.aStruct = aStruct;
    }

    @ThriftField(9)
    public Set<String> getASet()
    {
        return aSet;
    }

    @ThriftField
    public void setASet(Set<String> aSet)
    {
        this.isSetSet = true;
        this.aSet = aSet;
    }

    @ThriftField(10)
    public List<String> getAList()
    {
        return aList;
    }

    @ThriftField
    public void setAList(List<String> aList)
    {
        this.isListSet = true;
        this.aList = aList;
    }

    @ThriftField(11)
    public Map<String, String> getAMap()
    {
        return aMap;
    }

    @ThriftField
    public void setAMap(Map<String, String> aMap)
    {
        this.isMapSet = true;
        this.aMap = aMap;
    }

    public boolean isBooleanSet()
    {
        return isBooleanSet;
    }

    public boolean isByteSet()
    {
        return isByteSet;
    }

    public boolean isShortSet()
    {
        return isShortSet;
    }

    public boolean isIntegerSet()
    {
        return isIntegerSet;
    }

    public boolean isLongSet()
    {
        return isLongSet;
    }

    public boolean isFloatSet()
    {
        return isFloatSet;
    }

    public boolean isDoubleSet()
    {
        return isDoubleSet;
    }

    public boolean isStringSet()
    {
        return isStringSet;
    }

    public boolean isStructSet()
    {
        return isStructSet;
    }

    public boolean isSetSet()
    {
        return isSetSet;
    }

    public boolean isListSet()
    {
        return isListSet;
    }

    public boolean isMapSet()
    {
        return isMapSet;
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
        IsSetBean isSetBean = (IsSetBean) o;
        return Objects.equals(aBoolean, isSetBean.aBoolean) &&
                Objects.equals(aByte, isSetBean.aByte) &&
                Objects.equals(aShort, isSetBean.aShort) &&
                Objects.equals(aInteger, isSetBean.aInteger) &&
                Objects.equals(aLong, isSetBean.aLong) &&
                Objects.equals(aFloat, isSetBean.aFloat) &&
                Objects.equals(aDouble, isSetBean.aDouble) &&
                Objects.equals(aString, isSetBean.aString) &&
                Objects.equals(aStruct, isSetBean.aStruct) &&
                Objects.equals(aSet, isSetBean.aSet) &&
                Objects.equals(aList, isSetBean.aList) &&
                Objects.equals(aMap, isSetBean.aMap) &&
                Objects.equals(field, isSetBean.field);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(aBoolean, aByte, aShort, aInteger, aLong, aFloat, aDouble, aString, aStruct, aSet, aList, aMap, field);
    }
}
