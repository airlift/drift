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

import static com.google.common.base.MoreObjects.toStringHelper;

@ThriftStruct
public final class CoercionBean
{
    private Boolean booleanValue;
    private Byte byteValue;
    private Short shortValue;
    private Integer integerValue;
    private Long longValue;
    private Float floatValue;
    private Double doubleValue;

    private float primitiveFloat;
    private List<Float> floatList;

    public CoercionBean()
    {
    }

    public CoercionBean(
            Boolean booleanValue,
            Byte byteValue,
            Short shortValue,
            Integer integerValue,
            Long longValue,
            Float floatValue,
            Double doubleValue,
            float primitiveFloat,
            List<Float> floatList)
    {
        this.booleanValue = booleanValue;
        this.byteValue = byteValue;
        this.shortValue = shortValue;
        this.integerValue = integerValue;
        this.longValue = longValue;
        this.floatValue = floatValue;
        this.doubleValue = doubleValue;
        this.primitiveFloat = primitiveFloat;
        this.floatList = floatList;
    }

    @ThriftField(1)
    public Boolean getBooleanValue()
    {
        return booleanValue;
    }

    @ThriftField
    public void setBooleanValue(Boolean booleanValue)
    {
        this.booleanValue = booleanValue;
    }

    @ThriftField(2)
    public Byte getByteValue()
    {
        return byteValue;
    }

    @ThriftField
    public void setByteValue(Byte byteValue)
    {
        this.byteValue = byteValue;
    }

    @ThriftField(3)
    public Short getShortValue()
    {
        return shortValue;
    }

    @ThriftField
    public void setShortValue(Short shortValue)
    {
        this.shortValue = shortValue;
    }

    @ThriftField(4)
    public Integer getIntegerValue()
    {
        return integerValue;
    }

    @ThriftField
    public void setIntegerValue(Integer integerValue)
    {
        this.integerValue = integerValue;
    }

    @ThriftField(5)
    public Long getLongValue()
    {
        return longValue;
    }

    @ThriftField
    public void setLongValue(Long longValue)
    {
        this.longValue = longValue;
    }

    @ThriftField(6)
    public Float getFloatValue()
    {
        return floatValue;
    }

    @ThriftField
    public void setFloatValue(Float floatValue)
    {
        this.floatValue = floatValue;
    }

    @ThriftField(7)
    public Double getDoubleValue()
    {
        return doubleValue;
    }

    @ThriftField
    public void setDoubleValue(Double doubleValue)
    {
        this.doubleValue = doubleValue;
    }

    @ThriftField(8)
    public float getPrimitiveFloat()
    {
        return primitiveFloat;
    }

    @ThriftField
    public void setPrimitiveFloat(float primitiveFloat)
    {
        this.primitiveFloat = primitiveFloat;
    }

    @ThriftField(9)
    public List<Float> getFloatList()
    {
        return floatList;
    }

    @ThriftField
    public void setFloatList(List<Float> floatList)
    {
        this.floatList = floatList;
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
        CoercionBean that = (CoercionBean) o;
        return Float.compare(that.primitiveFloat, primitiveFloat) == 0 &&
                Objects.equals(booleanValue, that.booleanValue) &&
                Objects.equals(byteValue, that.byteValue) &&
                Objects.equals(shortValue, that.shortValue) &&
                Objects.equals(integerValue, that.integerValue) &&
                Objects.equals(longValue, that.longValue) &&
                Objects.equals(floatValue, that.floatValue) &&
                Objects.equals(doubleValue, that.doubleValue) &&
                Objects.equals(floatList, that.floatList);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(booleanValue, byteValue, shortValue, integerValue, longValue, floatValue, doubleValue, primitiveFloat, floatList);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("booleanValue", booleanValue)
                .add("byteValue", byteValue)
                .add("shortValue", shortValue)
                .add("integerValue", integerValue)
                .add("longValue", longValue)
                .add("floatValue", floatValue)
                .add("doubleValue", doubleValue)
                .add("primitiveFloat", primitiveFloat)
                .add("floatList", floatList)
                .toString();
    }
}
