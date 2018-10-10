/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * limitations under the License.
 */
package io.airlift.drift.protocol;

import io.airlift.drift.TException;

import java.nio.ByteBuffer;

import static java.lang.Double.doubleToLongBits;
import static java.lang.Double.longBitsToDouble;
import static java.lang.Float.floatToIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * Binary protocol implementation for thrift.
 */
public class TBinaryProtocol
        implements TProtocol
{
    private static final TStruct ANONYMOUS_STRUCT = new TStruct("");

    protected static final int VERSION_MASK = 0xffff0000;
    protected static final int VERSION_1 = 0x80010000;

    private final TTransport transport;

    /**
     * Constructor
     */
    public TBinaryProtocol(TTransport transport)
    {
        this.transport = requireNonNull(transport, "transport is null");
    }

    @Override
    public void writeMessageBegin(TMessage message)
            throws TException
    {
        writeI32(VERSION_1 | (message.getType() & 0xFF));
        writeString(message.getName());
        writeI32(message.getSequenceId());
    }

    @Override
    public void writeMessageEnd() {}

    @Override
    public void writeStructBegin(TStruct struct) {}

    @Override
    public void writeStructEnd() {}

    @Override
    public void writeFieldBegin(TField field)
            throws TException
    {
        writeByte(field.getType());
        writeI16(field.getId());
    }

    @Override
    public void writeFieldEnd() {}

    @Override
    public void writeFieldStop()
            throws TException
    {
        writeByte(TType.STOP);
    }

    @Override
    public void writeMapBegin(TMap map)
            throws TException
    {
        writeByte(map.getKeyType());
        writeByte(map.getValueType());
        writeI32(map.getSize());
    }

    @Override
    public void writeMapEnd() {}

    @Override
    public void writeListBegin(TList list)
            throws TException
    {
        writeByte(list.getType());
        writeI32(list.getSize());
    }

    @Override
    public void writeListEnd() {}

    @Override
    public void writeSetBegin(TSet set)
            throws TException
    {
        writeByte(set.getType());
        writeI32(set.getSize());
    }

    @Override
    public void writeSetEnd() {}

    @Override
    public void writeBool(boolean value)
            throws TException
    {
        writeByte((byte) (value ? 1 : 0));
    }

    private final byte[] bout = new byte[1];

    @Override
    public void writeByte(byte value)
            throws TException
    {
        bout[0] = value;
        transport.write(bout, 0, 1);
    }

    private final byte[] i16out = new byte[2];

    @Override
    public void writeI16(short value)
            throws TException
    {
        i16out[0] = (byte) (0xff & (value >> 8));
        i16out[1] = (byte) (0xff & (value));
        transport.write(i16out, 0, 2);
    }

    private final byte[] i32out = new byte[4];

    @Override
    public void writeI32(int value)
            throws TException
    {
        i32out[0] = (byte) (0xff & (value >> 24));
        i32out[1] = (byte) (0xff & (value >> 16));
        i32out[2] = (byte) (0xff & (value >> 8));
        i32out[3] = (byte) (0xff & (value));
        transport.write(i32out, 0, 4);
    }

    private final byte[] i64out = new byte[8];

    @Override
    public void writeI64(long value)
            throws TException
    {
        i64out[0] = (byte) (0xff & (value >> 56));
        i64out[1] = (byte) (0xff & (value >> 48));
        i64out[2] = (byte) (0xff & (value >> 40));
        i64out[3] = (byte) (0xff & (value >> 32));
        i64out[4] = (byte) (0xff & (value >> 24));
        i64out[5] = (byte) (0xff & (value >> 16));
        i64out[6] = (byte) (0xff & (value >> 8));
        i64out[7] = (byte) (0xff & (value));
        transport.write(i64out, 0, 8);
    }

    @Override
    public void writeFloat(float value)
            throws TException
    {
        writeI32(floatToIntBits(value));
    }

    @Override
    public void writeDouble(double value)
            throws TException
    {
        writeI64(doubleToLongBits(value));
    }

    @Override
    public void writeString(String value)
            throws TException
    {
        byte[] dat = value.getBytes(UTF_8);
        writeI32(dat.length);
        transport.write(dat);
    }

    @Override
    public void writeBinary(ByteBuffer value)
            throws TException
    {
        int length = value.limit() - value.position();
        writeI32(length);
        transport.write(value.array(), value.position() + value.arrayOffset(), length);
    }

    /**
     * Reading methods.
     */

    @Override
    public TMessage readMessageBegin()
            throws TException
    {
        int size = readI32();
        if (size < 0) {
            int version = size & VERSION_MASK;
            if (version != VERSION_1) {
                throw new TProtocolException("Bad version in readMessageBegin: " + version);
            }
            return new TMessage(readString(), (byte) (size & 0x000000ff), readI32());
        }

        // throw new TProtocolException("Missing version in readMessageBegin (old client?)");
        return new TMessage(readStringBody(size), readByte(), readI32());
    }

    @Override
    public void readMessageEnd() {}

    @Override
    public TStruct readStructBegin()
    {
        return ANONYMOUS_STRUCT;
    }

    @Override
    public void readStructEnd() {}

    @Override
    public TField readFieldBegin()
            throws TException
    {
        byte type = readByte();
        short id = type == TType.STOP ? 0 : readI16();
        return new TField("", type, id);
    }

    @Override
    public void readFieldEnd() {}

    @Override
    public TMap readMapBegin()
            throws TException
    {
        TMap map = new TMap(readByte(), readByte(), readI32());
        checkSize(map.getSize());
        return map;
    }

    @Override
    public void readMapEnd() {}

    @Override
    public TList readListBegin()
            throws TException
    {
        TList list = new TList(readByte(), readI32());
        checkSize(list.getSize());
        return list;
    }

    @Override
    public void readListEnd() {}

    @Override
    public TSet readSetBegin()
            throws TException
    {
        TSet set = new TSet(readByte(), readI32());
        checkSize(set.getSize());
        return set;
    }

    @Override
    public void readSetEnd() {}

    @Override
    public boolean readBool()
            throws TException
    {
        return (readByte() == 1);
    }

    private final byte[] bin = new byte[1];

    @Override
    public byte readByte()
            throws TException
    {
        readAll(bin, 1);
        return bin[0];
    }

    private final byte[] i16rd = new byte[2];

    @Override
    public short readI16()
            throws TException
    {
        byte[] buf = i16rd;
        int off = 0;
        readAll(i16rd, 2);
        return (short) (((buf[off] & 0xff) << 8) | ((buf[off + 1] & 0xff)));
    }

    private final byte[] i32rd = new byte[4];

    @Override
    public int readI32()
            throws TException
    {
        byte[] buf = i32rd;
        int off = 0;
        readAll(i32rd, 4);
        return ((buf[off] & 0xff) << 24) |
                ((buf[off + 1] & 0xff) << 16) |
                ((buf[off + 2] & 0xff) << 8) |
                ((buf[off + 3] & 0xff));
    }

    private final byte[] i64rd = new byte[8];

    @Override
    public long readI64()
            throws TException
    {
        byte[] buf = i64rd;
        int off = 0;
        readAll(i64rd, 8);
        return ((long) (buf[off] & 0xff) << 56) |
                ((long) (buf[off + 1] & 0xff) << 48) |
                ((long) (buf[off + 2] & 0xff) << 40) |
                ((long) (buf[off + 3] & 0xff) << 32) |
                ((long) (buf[off + 4] & 0xff) << 24) |
                ((long) (buf[off + 5] & 0xff) << 16) |
                ((long) (buf[off + 6] & 0xff) << 8) |
                ((long) (buf[off + 7] & 0xff));
    }

    @Override
    public float readFloat()
            throws TException
    {
        return intBitsToFloat(readI32());
    }

    @Override
    public double readDouble()
            throws TException
    {
        return longBitsToDouble(readI64());
    }

    @Override
    public String readString()
            throws TException
    {
        int size = checkSize(readI32());
        return readStringBody(size);
    }

    public String readStringBody(int size)
            throws TException
    {
        byte[] buf = new byte[size];
        transport.read(buf, 0, size);
        return new String(buf, UTF_8);
    }

    @Override
    public ByteBuffer readBinary()
            throws TException
    {
        int size = checkSize(readI32());
        byte[] buf = new byte[size];
        transport.read(buf, 0, size);
        return ByteBuffer.wrap(buf);
    }

    private static int checkSize(int length)
            throws TProtocolException
    {
        if (length < 0) {
            throw new TProtocolException("Negative length: " + length);
        }
        return length;
    }

    private void readAll(byte[] buf, int len)
            throws TException
    {
        transport.read(buf, 0, len);
    }
}
