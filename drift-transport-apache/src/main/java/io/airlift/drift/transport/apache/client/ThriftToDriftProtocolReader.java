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
package io.airlift.drift.transport.apache.client;

import io.airlift.drift.TException;
import io.airlift.drift.protocol.TField;
import io.airlift.drift.protocol.TList;
import io.airlift.drift.protocol.TMap;
import io.airlift.drift.protocol.TMessage;
import io.airlift.drift.protocol.TProtocolReader;
import io.airlift.drift.protocol.TSet;
import io.airlift.drift.protocol.TStruct;
import org.apache.thrift.protocol.TProtocol;

import java.nio.ByteBuffer;

import static java.util.Objects.requireNonNull;

public class ThriftToDriftProtocolReader
        implements TProtocolReader
{
    private final TProtocol protocol;

    public ThriftToDriftProtocolReader(TProtocol protocol)
    {
        this.protocol = requireNonNull(protocol, "protocol is null");
    }

    @Override
    public TMessage readMessageBegin()
            throws TException
    {
        try {
            org.apache.thrift.protocol.TMessage message = protocol.readMessageBegin();
            return new TMessage(message.name, message.type, message.seqid);
        }
        catch (org.apache.thrift.TException e) {
            throw new TException(e);
        }
    }

    @Override
    public void readMessageEnd()
            throws TException
    {
        try {
            protocol.readMessageEnd();
        }
        catch (org.apache.thrift.TException e) {
            throw new TException(e);
        }
    }

    @Override
    public TStruct readStructBegin()
            throws TException
    {
        try {
            org.apache.thrift.protocol.TStruct struct = protocol.readStructBegin();
            return new TStruct(struct.name);
        }
        catch (org.apache.thrift.TException e) {
            throw new TException(e);
        }
    }

    @Override
    public void readStructEnd()
            throws TException
    {
        try {
            protocol.readStructEnd();
        }
        catch (org.apache.thrift.TException e) {
            throw new TException(e);
        }
    }

    @Override
    public TField readFieldBegin()
            throws TException
    {
        try {
            org.apache.thrift.protocol.TField field = protocol.readFieldBegin();
            return new TField(field.name, field.type, field.id);
        }
        catch (org.apache.thrift.TException e) {
            throw new TException(e);
        }
    }

    @Override
    public void readFieldEnd()
            throws TException
    {
        try {
            protocol.readFieldEnd();
        }
        catch (org.apache.thrift.TException e) {
            throw new TException(e);
        }
    }

    @Override
    public TMap readMapBegin()
            throws TException
    {
        try {
            org.apache.thrift.protocol.TMap map = protocol.readMapBegin();
            return new TMap(map.keyType, map.valueType, map.size);
        }
        catch (org.apache.thrift.TException e) {
            throw new TException(e);
        }
    }

    @Override
    public void readMapEnd()
            throws TException
    {
        try {
            protocol.readMapEnd();
        }
        catch (org.apache.thrift.TException e) {
            throw new TException(e);
        }
    }

    @Override
    public TList readListBegin()
            throws TException
    {
        try {
            org.apache.thrift.protocol.TList list = protocol.readListBegin();
            return new TList(list.elemType, list.size);
        }
        catch (org.apache.thrift.TException e) {
            throw new TException(e);
        }
    }

    @Override
    public void readListEnd()
            throws TException
    {
        try {
            protocol.readListEnd();
        }
        catch (org.apache.thrift.TException e) {
            throw new TException(e);
        }
    }

    @Override
    public TSet readSetBegin()
            throws TException
    {
        try {
            org.apache.thrift.protocol.TSet set = protocol.readSetBegin();
            return new TSet(set.elemType, set.size);
        }
        catch (org.apache.thrift.TException e) {
            throw new TException(e);
        }
    }

    @Override
    public void readSetEnd()
            throws TException
    {
        try {
            protocol.readSetEnd();
        }
        catch (org.apache.thrift.TException e) {
            throw new TException(e);
        }
    }

    @Override
    public boolean readBool()
            throws TException
    {
        try {
            return protocol.readBool();
        }
        catch (org.apache.thrift.TException e) {
            throw new TException(e);
        }
    }

    @Override
    public byte readByte()
            throws TException
    {
        try {
            return protocol.readByte();
        }
        catch (org.apache.thrift.TException e) {
            throw new TException(e);
        }
    }

    @Override
    public short readI16()
            throws TException
    {
        try {
            return protocol.readI16();
        }
        catch (org.apache.thrift.TException e) {
            throw new TException(e);
        }
    }

    @Override
    public int readI32()
            throws TException
    {
        try {
            return protocol.readI32();
        }
        catch (org.apache.thrift.TException e) {
            throw new TException(e);
        }
    }

    @Override
    public long readI64()
            throws TException
    {
        try {
            return protocol.readI64();
        }
        catch (org.apache.thrift.TException e) {
            throw new TException(e);
        }
    }

    @Override
    public double readDouble()
            throws TException
    {
        try {
            return protocol.readDouble();
        }
        catch (org.apache.thrift.TException e) {
            throw new TException(e);
        }
    }

    @Override
    public String readString()
            throws TException
    {
        try {
            return protocol.readString();
        }
        catch (org.apache.thrift.TException e) {
            throw new TException(e);
        }
    }

    @Override
    public ByteBuffer readBinary()
            throws TException
    {
        try {
            return protocol.readBinary();
        }
        catch (org.apache.thrift.TException e) {
            throw new TException(e);
        }
    }
}
