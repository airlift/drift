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
package io.airlift.drift.codec.internal;

import io.airlift.drift.TException;
import io.airlift.drift.codec.ThriftCodec;
import io.airlift.drift.protocol.TField;
import io.airlift.drift.protocol.TList;
import io.airlift.drift.protocol.TMap;
import io.airlift.drift.protocol.TProtocolReader;
import io.airlift.drift.protocol.TProtocolUtil;
import io.airlift.drift.protocol.TSet;
import io.airlift.drift.protocol.TType;

import javax.annotation.concurrent.NotThreadSafe;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;

@NotThreadSafe
public class ProtocolReader
{
    private final TProtocolReader protocol;
    private TField currentField;

    public ProtocolReader(TProtocolReader protocol)
    {
        this.protocol = protocol;
    }

    public void readStructBegin()
            throws TException
    {
        protocol.readStructBegin();
        currentField = null;
    }

    public void readStructEnd()
            throws TException
    {
        if (currentField == null || currentField.getId() != TType.STOP) {
            throw new IllegalStateException("Some fields have not been consumed");
        }

        currentField = null;
        protocol.readStructEnd();
    }

    public boolean nextField()
            throws TException
    {
        // if the current field is a stop record, the caller must call readStructEnd.
        if (currentField != null && currentField.getId() == TType.STOP) {
            throw new NoSuchElementException();
        }
        checkState(currentField == null, "Current field was not read");

        // advance to the next field
        currentField = protocol.readFieldBegin();

        return currentField.getType() != TType.STOP;
    }

    public short getFieldId()
    {
        checkState(currentField != null, "No current field");
        return currentField.getId();
    }

    public byte getFieldType()
    {
        checkState(currentField != null, "No current field");
        return currentField.getType();
    }

    public void skipFieldData()
            throws TException
    {
        TProtocolUtil.skip(protocol, currentField.getType());
        protocol.readFieldEnd();
        currentField = null;
    }

    public Object readField(ThriftCodec<?> codec)
            throws Exception
    {
        if (!checkReadState(codec.getType().getProtocolType().getType())) {
            return null;
        }
        currentField = null;
        Object fieldValue = codec.read(protocol);
        protocol.readFieldEnd();
        return fieldValue;
    }

    public ByteBuffer readBinaryField()
            throws TException
    {
        if (!checkReadState(TType.STRING)) {
            return null;
        }
        currentField = null;
        ByteBuffer fieldValue = protocol.readBinary();
        protocol.readFieldEnd();
        return fieldValue;
    }

    public boolean readBoolField()
            throws TException
    {
        if (!checkReadState(TType.BOOL)) {
            return false;
        }
        currentField = null;
        boolean fieldValue = protocol.readBool();
        protocol.readFieldEnd();
        return fieldValue;
    }

    public byte readByteField()
            throws TException
    {
        if (!checkReadState(TType.BYTE)) {
            return 0;
        }
        currentField = null;
        byte fieldValue = protocol.readByte();
        protocol.readFieldEnd();
        return fieldValue;
    }

    public double readDoubleField()
            throws TException
    {
        if (!checkReadState(TType.DOUBLE)) {
            return 0;
        }
        currentField = null;
        double fieldValue = protocol.readDouble();
        protocol.readFieldEnd();
        return fieldValue;
    }

    public short readI16Field()
            throws TException
    {
        if (!checkReadState(TType.I16)) {
            return 0;
        }
        currentField = null;
        short fieldValue = protocol.readI16();
        protocol.readFieldEnd();
        return fieldValue;
    }

    public int readI32Field()
            throws TException
    {
        if (!checkReadState(TType.I32)) {
            return 0;
        }
        currentField = null;
        int fieldValue = protocol.readI32();
        protocol.readFieldEnd();
        return fieldValue;
    }

    public long readI64Field()
            throws TException
    {
        if (!checkReadState(TType.I64)) {
            return 0;
        }
        currentField = null;
        long fieldValue = protocol.readI64();
        protocol.readFieldEnd();
        return fieldValue;
    }

    public String readStringField()
            throws TException
    {
        if (!checkReadState(TType.STRING)) {
            return null;
        }
        currentField = null;
        String fieldValue = protocol.readString();
        protocol.readFieldEnd();
        return fieldValue;
    }

    public <T> T readStructField(ThriftCodec<T> codec)
            throws Exception
    {
        if (!checkReadState(TType.STRUCT)) {
            return null;
        }
        currentField = null;
        T fieldValue = codec.read(protocol);
        protocol.readFieldEnd();
        return fieldValue;
    }

    public boolean[] readBoolArrayField()
            throws TException
    {
        if (!checkReadState(TType.LIST)) {
            return null;
        }
        currentField = null;
        boolean[] fieldValue = readBoolArray();
        protocol.readFieldEnd();
        return fieldValue;
    }

    public short[] readI16ArrayField()
            throws TException
    {
        if (!checkReadState(TType.LIST)) {
            return null;
        }
        currentField = null;
        short[] fieldValue = readI16Array();
        protocol.readFieldEnd();
        return fieldValue;
    }

    public int[] readI32ArrayField()
            throws TException
    {
        if (!checkReadState(TType.LIST)) {
            return null;
        }
        currentField = null;
        int[] fieldValue = readI32Array();
        protocol.readFieldEnd();
        return fieldValue;
    }

    public long[] readI64ArrayField()
            throws TException
    {
        if (!checkReadState(TType.LIST)) {
            return null;
        }
        currentField = null;
        long[] fieldValue = readI64Array();
        protocol.readFieldEnd();
        return fieldValue;
    }

    public double[] readDoubleArrayField()
            throws TException
    {
        if (!checkReadState(TType.LIST)) {
            return null;
        }
        currentField = null;
        double[] fieldValue = readDoubleArray();
        protocol.readFieldEnd();
        return fieldValue;
    }

    public <E> Set<E> readSetField(ThriftCodec<Set<E>> setCodec)
            throws Exception
    {
        if (!checkReadState(TType.SET)) {
            return null;
        }
        currentField = null;
        Set<E> fieldValue = setCodec.read(protocol);
        protocol.readFieldEnd();
        return fieldValue;
    }

    public <E> List<E> readListField(ThriftCodec<List<E>> listCodec)
            throws Exception
    {
        if (!checkReadState(TType.LIST)) {
            return null;
        }
        currentField = null;
        List<E> read = listCodec.read(protocol);
        protocol.readFieldEnd();
        return read;
    }

    public <K, V> Map<K, V> readMapField(ThriftCodec<Map<K, V>> mapCodec)
            throws Exception
    {
        if (!checkReadState(TType.MAP)) {
            return null;
        }
        currentField = null;
        Map<K, V> fieldValue = mapCodec.read(protocol);
        protocol.readFieldEnd();
        return fieldValue;
    }

    public <T extends Enum<T>> T readEnumField(ThriftCodec<T> enumCodec)
            throws Exception
    {
        if (!checkReadState(TType.I32)) {
            return null;
        }
        currentField = null;
        T fieldValue = null;
        try {
            fieldValue = enumCodec.read(protocol);
        }
        catch (UnknownEnumValueException e) {
            // return null
        }
        protocol.readFieldEnd();
        return fieldValue;
    }

    public ByteBuffer readBinary()
            throws TException
    {
        return protocol.readBinary();
    }

    public boolean readBool()
            throws TException
    {
        return protocol.readBool();
    }

    public byte readByte()
            throws TException
    {
        return protocol.readByte();
    }

    public short readI16()
            throws TException
    {
        return protocol.readI16();
    }

    public int readI32()
            throws TException
    {
        return protocol.readI32();
    }

    public long readI64()
            throws TException
    {
        return protocol.readI64();
    }

    public double readDouble()
            throws TException
    {
        return protocol.readDouble();
    }

    public String readString()
            throws TException
    {
        return protocol.readString();
    }

    public boolean[] readBoolArray()
            throws TException
    {
        TList list = protocol.readListBegin();
        boolean[] array = new boolean[list.getSize()];
        for (int i = 0; i < list.getSize(); i++) {
            array[i] = readBool();
        }
        protocol.readListEnd();
        return array;
    }

    public short[] readI16Array()
            throws TException
    {
        TList list = protocol.readListBegin();
        short[] array = new short[list.getSize()];
        for (int i = 0; i < list.getSize(); i++) {
            array[i] = readI16();
        }
        protocol.readListEnd();
        return array;
    }

    public int[] readI32Array()
            throws TException
    {
        TList list = protocol.readListBegin();
        int[] array = new int[list.getSize()];
        for (int i = 0; i < list.getSize(); i++) {
            array[i] = readI32();
        }
        protocol.readListEnd();
        return array;
    }

    public long[] readI64Array()
            throws TException
    {
        TList list = protocol.readListBegin();
        long[] array = new long[list.getSize()];
        for (int i = 0; i < list.getSize(); i++) {
            array[i] = readI64();
        }
        protocol.readListEnd();
        return array;
    }

    public double[] readDoubleArray()
            throws TException
    {
        TList list = protocol.readListBegin();
        double[] array = new double[list.getSize()];
        for (int i = 0; i < list.getSize(); i++) {
            array[i] = readDouble();
        }
        protocol.readListEnd();
        return array;
    }

    public <E> Set<E> readSet(ThriftCodec<E> elementCodec)
            throws Exception
    {
        TSet tSet = protocol.readSetBegin();
        Set<E> set = new HashSet<>();
        for (int i = 0; i < tSet.getSize(); i++) {
            try {
                E element = elementCodec.read(protocol);
                set.add(element);
            }
            catch (UnknownEnumValueException e) {
                // continue
            }
        }
        protocol.readSetEnd();
        return set;
    }

    public <E> List<E> readList(ThriftCodec<E> elementCodec)
            throws Exception
    {
        TList tList = protocol.readListBegin();
        List<E> list = new ArrayList<>();
        for (int i = 0; i < tList.getSize(); i++) {
            try {
                E element = elementCodec.read(protocol);
                list.add(element);
            }
            catch (UnknownEnumValueException e) {
                // continue
            }
        }
        protocol.readListEnd();
        return list;
    }

    public <K, V> Map<K, V> readMap(ThriftCodec<K> keyCodec, ThriftCodec<V> valueCodec)
            throws Exception
    {
        TMap tMap = protocol.readMapBegin();
        Map<K, V> map = new HashMap<>();
        for (int i = 0; i < tMap.getSize(); i++) {
            try {
                K key = keyCodec.read(protocol);
                V value = valueCodec.read(protocol);
                map.put(key, value);
            }
            catch (UnknownEnumValueException e) {
                // continue
            }
        }
        protocol.readMapEnd();
        return map;
    }

    private boolean checkReadState(byte expectedType)
            throws TException
    {
        checkState(currentField != null, "No current field");

        if (currentField.getType() != expectedType) {
            TProtocolUtil.skip(protocol, currentField.getType());
            protocol.readFieldEnd();
            currentField = null;
            return false;
        }

        return true;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("currentField", currentField)
                .toString();
    }
}
