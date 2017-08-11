/*
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
package io.airlift.drift.protocol;

import io.airlift.drift.TException;

import java.nio.ByteBuffer;

public interface TProtocolWriter
{
    void writeMessageBegin(TMessage message)
            throws TException;

    void writeMessageEnd()
            throws TException;

    void writeStructBegin(TStruct struct)
            throws TException;

    void writeStructEnd()
            throws TException;

    void writeFieldBegin(TField field)
            throws TException;

    void writeFieldEnd()
            throws TException;

    void writeFieldStop()
            throws TException;

    void writeMapBegin(TMap map)
            throws TException;

    void writeMapEnd()
            throws TException;

    void writeListBegin(TList list)
            throws TException;

    void writeListEnd()
            throws TException;

    void writeSetBegin(TSet set)
            throws TException;

    void writeSetEnd()
            throws TException;

    void writeBool(boolean value)
            throws TException;

    void writeByte(byte value)
            throws TException;

    void writeI16(short value)
            throws TException;

    void writeI32(int value)
            throws TException;

    void writeI64(long value)
            throws TException;

    void writeDouble(double value)
            throws TException;

    void writeString(String value)
            throws TException;

    void writeBinary(ByteBuffer value)
            throws TException;
}
