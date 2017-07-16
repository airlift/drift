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
package io.airlift.drift.transport.apache;

import io.airlift.drift.TException;
import io.airlift.drift.protocol.TField;
import io.airlift.drift.protocol.TList;
import io.airlift.drift.protocol.TMap;
import io.airlift.drift.protocol.TMessage;
import io.airlift.drift.protocol.TProtocolWriter;
import io.airlift.drift.protocol.TSet;
import io.airlift.drift.protocol.TStruct;
import org.apache.thrift.protocol.TProtocol;

import java.nio.ByteBuffer;

import static java.util.Objects.requireNonNull;

public class ThriftToDriftProtocolWriter
        implements TProtocolWriter
{
    private final TProtocol protocol;

    public ThriftToDriftProtocolWriter(TProtocol protocol)
    {
        this.protocol = requireNonNull(protocol, "protocol is null");
    }

    @Override
    public void writeMessageBegin(TMessage message)
            throws TException
    {
        try {
            protocol.writeMessageBegin(new org.apache.thrift.protocol.TMessage(message.getName(), message.getType(), message.getSequenceId()));
        }
        catch (org.apache.thrift.TException e) {
            throw new TException(e);
        }
    }

    @Override
    public void writeMessageEnd()
            throws TException
    {
        try {
            protocol.writeMessageEnd();
        }
        catch (org.apache.thrift.TException e) {
            throw new TException(e);
        }
    }

    @Override
    public void writeStructBegin(TStruct struct)
            throws TException
    {
        try {
            protocol.writeStructBegin(new org.apache.thrift.protocol.TStruct(struct.getName()));
        }
        catch (org.apache.thrift.TException e) {
            throw new TException(e);
        }
    }

    @Override
    public void writeStructEnd()
            throws TException
    {
        try {
            protocol.writeStructEnd();
        }
        catch (org.apache.thrift.TException e) {
            throw new TException(e);
        }
    }

    @Override
    public void writeFieldBegin(TField field)
            throws TException
    {
        try {
            protocol.writeFieldBegin(new org.apache.thrift.protocol.TField(field.getName(), field.getType(), field.getId()));
        }
        catch (org.apache.thrift.TException e) {
            throw new TException(e);
        }
    }

    @Override
    public void writeFieldEnd()
            throws TException
    {
        try {
            protocol.writeFieldEnd();
        }
        catch (org.apache.thrift.TException e) {
            throw new TException(e);
        }
    }

    @Override
    public void writeFieldStop()
            throws TException
    {
        try {
            protocol.writeFieldStop();
        }
        catch (org.apache.thrift.TException e) {
            throw new TException(e);
        }
    }

    @Override
    public void writeMapBegin(TMap map)
            throws TException
    {
        try {
            protocol.writeMapBegin(new org.apache.thrift.protocol.TMap(map.getKeyType(), map.getValueType(), map.getSize()));
        }
        catch (org.apache.thrift.TException e) {
            throw new TException(e);
        }
    }

    @Override
    public void writeMapEnd()
            throws TException
    {
        try {
            protocol.writeMapEnd();
        }
        catch (org.apache.thrift.TException e) {
            throw new TException(e);
        }
    }

    @Override
    public void writeListBegin(TList list)
            throws TException
    {
        try {
            protocol.writeListBegin(new org.apache.thrift.protocol.TList(list.getType(), list.getSize()));
        }
        catch (org.apache.thrift.TException e) {
            throw new TException(e);
        }
    }

    @Override
    public void writeListEnd()
            throws TException
    {
        try {
            protocol.writeListEnd();
        }
        catch (org.apache.thrift.TException e) {
            throw new TException(e);
        }
    }

    @Override
    public void writeSetBegin(TSet set)
            throws TException
    {
        try {
            protocol.writeSetBegin(new org.apache.thrift.protocol.TSet(set.getType(), set.getSize()));
        }
        catch (org.apache.thrift.TException e) {
            throw new TException(e);
        }
    }

    @Override
    public void writeSetEnd()
            throws TException
    {
        try {
            protocol.writeSetEnd();
        }
        catch (org.apache.thrift.TException e) {
            throw new TException(e);
        }
    }

    @Override
    public void writeBool(boolean value)
            throws TException
    {
        try {
            protocol.writeBool(value);
        }
        catch (org.apache.thrift.TException e) {
            throw new TException(e);
        }
    }

    @Override
    public void writeByte(byte value)
            throws TException
    {
        try {
            protocol.writeByte(value);
        }
        catch (org.apache.thrift.TException e) {
            throw new TException(e);
        }
    }

    @Override
    public void writeI16(short value)
            throws TException
    {
        try {
            protocol.writeI16(value);
        }
        catch (org.apache.thrift.TException e) {
            throw new TException(e);
        }
    }

    @Override
    public void writeI32(int value)
            throws TException
    {
        try {
            protocol.writeI32(value);
        }
        catch (org.apache.thrift.TException e) {
            throw new TException(e);
        }
    }

    @Override
    public void writeI64(long value)
            throws TException
    {
        try {
            protocol.writeI64(value);
        }
        catch (org.apache.thrift.TException e) {
            throw new TException(e);
        }
    }

    @Override
    public void writeDouble(double value)
            throws TException
    {
        try {
            protocol.writeDouble(value);
        }
        catch (org.apache.thrift.TException e) {
            throw new TException(e);
        }
    }

    @Override
    public void writeString(String value)
            throws TException
    {
        try {
            protocol.writeString(value);
        }
        catch (org.apache.thrift.TException e) {
            throw new TException(e);
        }
    }

    @Override
    public void writeBinary(ByteBuffer value)
            throws TException
    {
        try {
            protocol.writeBinary(value);
        }
        catch (org.apache.thrift.TException e) {
            throw new TException(e);
        }
    }
}
