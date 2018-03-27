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
 * under the License.
 */
package io.airlift.drift.protocol;

import io.airlift.drift.TException;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;

import static java.lang.Double.doubleToLongBits;
import static java.lang.Double.longBitsToDouble;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * TCompactProtocol2 is the Java implementation of the compact protocol specified
 * in THRIFT-110. The fundamental approach to reducing the overhead of
 * structures is a) use variable-length integers all over the place and b) make
 * use of unused bits wherever possible. Your savings will obviously vary
 * based on the specific makeup of your structs, but in general, the more
 * fields, nested structures, short strings and collections, and low-value i32
 * and i64 fields you have, the more benefit you'll see.
 */
public class TCompactProtocol
        implements TProtocol
{
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    private static final TStruct ANONYMOUS_STRUCT = new TStruct("");
    private static final TField TSTOP = new TField("", TType.STOP, (short) 0);

    private static final byte[] TTYPE_TO_COMPACT_TYPE = new byte[16];

    static {
        TTYPE_TO_COMPACT_TYPE[TType.STOP] = TType.STOP;
        TTYPE_TO_COMPACT_TYPE[TType.BOOL] = Types.BOOLEAN_TRUE;
        TTYPE_TO_COMPACT_TYPE[TType.BYTE] = Types.BYTE;
        TTYPE_TO_COMPACT_TYPE[TType.I16] = Types.I16;
        TTYPE_TO_COMPACT_TYPE[TType.I32] = Types.I32;
        TTYPE_TO_COMPACT_TYPE[TType.I64] = Types.I64;
        TTYPE_TO_COMPACT_TYPE[TType.DOUBLE] = Types.DOUBLE;
        TTYPE_TO_COMPACT_TYPE[TType.STRING] = Types.BINARY;
        TTYPE_TO_COMPACT_TYPE[TType.LIST] = Types.LIST;
        TTYPE_TO_COMPACT_TYPE[TType.SET] = Types.SET;
        TTYPE_TO_COMPACT_TYPE[TType.MAP] = Types.MAP;
        TTYPE_TO_COMPACT_TYPE[TType.STRUCT] = Types.STRUCT;
    }

    private static final byte PROTOCOL_ID = (byte) 0x82;
    private static final byte VERSION = 1;
    private static final byte VERSION_MASK = 0x1f; // 0001 1111
    private static final byte TYPE_MASK = (byte) 0xE0; // 1110 0000
    private static final byte TYPE_BITS = 0x07; // 0000 0111
    private static final int TYPE_SHIFT_AMOUNT = 5;

    /**
     * All of the on-wire type codes.
     */
    private static final class Types
    {
        public static final byte BOOLEAN_TRUE = 0x01;
        public static final byte BOOLEAN_FALSE = 0x02;
        public static final byte BYTE = 0x03;
        public static final byte I16 = 0x04;
        public static final byte I32 = 0x05;
        public static final byte I64 = 0x06;
        public static final byte DOUBLE = 0x07;
        public static final byte BINARY = 0x08;
        public static final byte LIST = 0x09;
        public static final byte SET = 0x0A;
        public static final byte MAP = 0x0B;
        public static final byte STRUCT = 0x0C;
    }

    /**
     * Used to keep track of the last field for the current and previous structs,
     * so we can do the delta stuff.
     */
    private final Deque<Short> lastField = new ArrayDeque<>();

    private short lastFieldId;

    /**
     * If we encounter a boolean field begin, save the TField here so it can
     * have the value incorporated.
     */
    private TField booleanField;

    /**
     * If we read a field header, and it's a boolean field, save the boolean
     * value here so that readBool can use it.
     */
    private Boolean booleanValue;

    /**
     * The transport for reading from or writing to.
     */
    private final TTransport transport;

    /**
     * Create a TCompactProtocol.
     *
     * @param transport the TTransport object to read from or write to.
     */
    public TCompactProtocol(TTransport transport)
    {
        this.transport = requireNonNull(transport, "transport is null");
    }

    //
    // Public Writing methods.
    //

    /**
     * Write a message header to the wire. Compact Protocol messages contain the
     * protocol version so we can migrate forwards in the future if need be.
     */
    @Override
    public void writeMessageBegin(TMessage message)
            throws TException
    {
        writeByteDirect(PROTOCOL_ID);
        writeByteDirect((VERSION & VERSION_MASK) | ((message.getType() << TYPE_SHIFT_AMOUNT) & TYPE_MASK));
        writeVarint32(message.getSequenceId());
        writeString(message.getName());
    }

    /**
     * Write a struct begin. This doesn't actually put anything on the wire. We
     * use it as an opportunity to put special placeholder markers on the field
     * stack so we can get the field id deltas correct.
     */
    @Override
    public void writeStructBegin(TStruct struct)
            throws TException
    {
        lastField.push(lastFieldId);
        lastFieldId = 0;
    }

    /**
     * Write a struct end. This doesn't actually put anything on the wire. We use
     * this as an opportunity to pop the last field from the current struct off
     * of the field stack.
     */
    @Override
    public void writeStructEnd()
            throws TException
    {
        lastFieldId = lastField.pop();
    }

    /**
     * Write a field header containing the field id and field type. If the
     * difference between the current field id and the last one is small (&lt; 15),
     * then the field id will be encoded in the 4 MSB as a delta. Otherwise, the
     * field id will follow the type header as a zigzag varint.
     */
    @Override
    public void writeFieldBegin(TField field)
            throws TException
    {
        if (field.getType() == TType.BOOL) {
            // we want to possibly include the value, so we'll wait.
            booleanField = field;
        }
        else {
            writeFieldBeginInternal(field, (byte) -1);
        }
    }

    /**
     * The workhorse of writeFieldBegin. It has the option of doing a
     * 'type override' of the type header. This is used specifically in the
     * boolean field case.
     */
    private void writeFieldBeginInternal(TField field, byte typeOverride)
            throws TException
    {
        // short lastField = lastField_.pop();

        // if there's a type override, use that.
        byte typeToWrite = typeOverride == -1 ? getCompactType(field.getType()) : typeOverride;

        // check if we can use delta encoding for the field id
        if (field.getId() > lastFieldId && field.getId() - lastFieldId <= 15) {
            // write them together
            writeByteDirect((field.getId() - lastFieldId) << 4 | typeToWrite);
        }
        else {
            // write them separate
            writeByteDirect(typeToWrite);
            writeI16(field.getId());
        }

        lastFieldId = field.getId();
    }

    /**
     * Write the STOP symbol so we know there are no more fields in this struct.
     */
    @Override
    public void writeFieldStop()
            throws TException
    {
        writeByteDirect(TType.STOP);
    }

    /**
     * Write a map header. If the map is empty, omit the key and value type
     * headers, as we don't need any additional information to skip it.
     */
    @Override
    public void writeMapBegin(TMap map)
            throws TException
    {
        if (map.getSize() == 0) {
            writeByteDirect(0);
        }
        else {
            writeVarint32(map.getSize());
            writeByteDirect(getCompactType(map.getKeyType()) << 4 | getCompactType(map.getValueType()));
        }
    }

    /**
     * Write a list header.
     */
    @Override
    public void writeListBegin(TList list)
            throws TException
    {
        writeCollectionBegin(list.getType(), list.getSize());
    }

    /**
     * Write a set header.
     */
    @Override
    public void writeSetBegin(TSet set)
            throws TException
    {
        writeCollectionBegin(set.getType(), set.getSize());
    }

    /**
     * Write a boolean value. Potentially, this could be a boolean field, in
     * which case the field header info isn't written yet. If so, decide what the
     * right type header is for the value and then write the field header.
     * Otherwise, write a single byte.
     */
    @Override
    public void writeBool(boolean value)
            throws TException
    {
        if (booleanField != null) {
            // we haven't written the field header yet
            writeFieldBeginInternal(booleanField, value ? Types.BOOLEAN_TRUE : Types.BOOLEAN_FALSE);
            booleanField = null;
        }
        else {
            // we're not part of a field, so just write the value.
            writeByteDirect(value ? Types.BOOLEAN_TRUE : Types.BOOLEAN_FALSE);
        }
    }

    /**
     * Write a byte. Nothing to see here!
     */
    @Override
    public void writeByte(byte value)
            throws TException
    {
        writeByteDirect(value);
    }

    /**
     * Write an I16 as a zigzag varint.
     */
    @Override
    public void writeI16(short value)
            throws TException
    {
        writeVarint32(intToZigZag(value));
    }

    /**
     * Write an i32 as a zigzag varint.
     */
    @Override
    public void writeI32(int value)
            throws TException
    {
        writeVarint32(intToZigZag(value));
    }

    /**
     * Write an i64 as a zigzag varint.
     */
    @Override
    public void writeI64(long value)
            throws TException
    {
        writeVarint64(longToZigzag(value));
    }

    /**
     * Write a double to the wire as 8 bytes.
     */
    @Override
    public void writeDouble(double value)
            throws TException
    {
        byte[] data = {0, 0, 0, 0, 0, 0, 0, 0};
        fixedLongToBytes(doubleToLongBits(value), data);
        transport.write(data);
    }

    /**
     * Write a string to the wire with a varint size preceding.
     */
    @Override
    public void writeString(String value)
            throws TException
    {
        byte[] bytes = value.getBytes(UTF_8);
        writeBinary(bytes, 0, bytes.length);
    }

    /**
     * Write a byte array, using a varint for the size.
     */
    @Override
    public void writeBinary(ByteBuffer value)
            throws TException
    {
        int length = value.limit() - value.position();
        writeBinary(value.array(), value.position() + value.arrayOffset(), length);
    }

    private void writeBinary(byte[] buf, int offset, int length)
            throws TException
    {
        writeVarint32(length);
        transport.write(buf, offset, length);
    }

    //
    // These methods are called by structs, but don't actually have any wire
    // output or purpose.
    //

    @Override
    public void writeMessageEnd() {}

    @Override
    public void writeMapEnd() {}

    @Override
    public void writeListEnd() {}

    @Override
    public void writeSetEnd() {}

    @Override
    public void writeFieldEnd() {}

    //
    // Internal writing methods
    //

    /**
     * Abstract method for writing the start of lists and sets. List and sets on
     * the wire differ only by the type indicator.
     */
    private void writeCollectionBegin(byte elemType, int size)
            throws TException
    {
        if (size <= 14) {
            writeByteDirect(size << 4 | getCompactType(elemType));
        }
        else {
            writeByteDirect(0xf0 | getCompactType(elemType));
            writeVarint32(size);
        }
    }

    private final byte[] i32buf = new byte[5];

    /**
     * Write an i32 as a varint. Results in 1-5 bytes on the wire.
     */
    @SuppressWarnings("ValueOfIncrementOrDecrementUsed")
    private void writeVarint32(int n)
            throws TException
    {
        int idx = 0;
        while (true) {
            if ((n & ~0x7F) == 0) {
                i32buf[idx++] = (byte) n;
                break;
            }
            i32buf[idx++] = (byte) ((n & 0x7F) | 0x80);
            n >>>= 7;
        }
        transport.write(i32buf, 0, idx);
    }

    private final byte[] varint64out = new byte[10];

    /**
     * Write an i64 as a varint. Results in 1-10 bytes on the wire.
     */
    @SuppressWarnings("ValueOfIncrementOrDecrementUsed")
    private void writeVarint64(long n)
            throws TException
    {
        int idx = 0;
        while (true) {
            if ((n & ~0x7FL) == 0) {
                varint64out[idx++] = (byte) n;
                break;
            }
            varint64out[idx++] = ((byte) ((n & 0x7F) | 0x80));
            n >>>= 7;
        }
        transport.write(varint64out, 0, idx);
    }

    /**
     * Convert l into a zigzag long. This allows negative numbers to be
     * represented compactly as a varint.
     */
    private static long longToZigzag(long l)
    {
        return (l << 1) ^ (l >> 63);
    }

    /**
     * Convert n into a zigzag int. This allows negative numbers to be
     * represented compactly as a varint.
     */
    private static int intToZigZag(int n)
    {
        return (n << 1) ^ (n >> 31);
    }

    /**
     * Convert a long into little-endian bytes in buf
     */
    private static void fixedLongToBytes(long n, byte[] buf)
    {
        buf[0] = (byte) (n & 0xff);
        buf[1] = (byte) ((n >> 8) & 0xff);
        buf[2] = (byte) ((n >> 16) & 0xff);
        buf[3] = (byte) ((n >> 24) & 0xff);
        buf[4] = (byte) ((n >> 32) & 0xff);
        buf[5] = (byte) ((n >> 40) & 0xff);
        buf[6] = (byte) ((n >> 48) & 0xff);
        buf[7] = (byte) ((n >> 56) & 0xff);
    }

    private final byte[] byteDirectBuffer = new byte[1];

    /**
     * Writes a byte without any possibility of all that field header nonsense.
     * Used internally by other writing methods that know they need to write a byte.
     */
    private void writeByteDirect(byte b)
            throws TException
    {
        byteDirectBuffer[0] = b;
        transport.write(byteDirectBuffer);
    }

    /**
     * Writes a byte without any possibility of all that field header nonsense.
     */
    private void writeByteDirect(int n)
            throws TException
    {
        writeByteDirect((byte) n);
    }

    //
    // Reading methods.
    //

    /**
     * Read a message header.
     */
    @Override
    public TMessage readMessageBegin()
            throws TException
    {
        byte protocolId = readByte();
        if (protocolId != PROTOCOL_ID) {
            throw new TProtocolException("Expected protocol id " + Integer.toHexString(PROTOCOL_ID) + " but got " + Integer.toHexString(protocolId));
        }
        byte versionAndType = readByte();
        byte version = (byte) (versionAndType & VERSION_MASK);
        if (version != VERSION) {
            throw new TProtocolException("Expected version " + VERSION + " but got " + version);
        }
        byte type = (byte) ((versionAndType >> TYPE_SHIFT_AMOUNT) & TYPE_BITS);
        int seqid = readVarint32();
        String messageName = readString();
        return new TMessage(messageName, type, seqid);
    }

    /**
     * Read a struct begin. There's nothing on the wire for this, but it is our
     * opportunity to push a new struct begin marker onto the field stack.
     */
    @Override
    public TStruct readStructBegin()
            throws TException
    {
        lastField.push(lastFieldId);
        lastFieldId = 0;
        return ANONYMOUS_STRUCT;
    }

    /**
     * Doesn't actually consume any wire data, just removes the last field for
     * this struct from the field stack.
     */
    @Override
    public void readStructEnd()
            throws TException
    {
        // consume the last field we read off the wire.
        lastFieldId = lastField.pop();
    }

    /**
     * Read a field header off the wire.
     */
    @Override
    public TField readFieldBegin()
            throws TException
    {
        byte type = readByte();

        // if it's a stop, then we can return immediately, as the struct is over.
        if (type == TType.STOP) {
            return TSTOP;
        }

        short fieldId;

        // mask off the 4 MSB of the type header. it could contain a field id delta.
        short modifier = (short) ((type & 0xf0) >> 4);
        if (modifier == 0) {
            // not a delta. look ahead for the zigzag varint field id.
            fieldId = readI16();
        }
        else {
            // has a delta. add the delta to the last read field id.
            fieldId = (short) (lastFieldId + modifier);
        }

        TField field = new TField("", getTType((byte) (type & 0x0f)), fieldId);

        // if this happens to be a boolean field, the value is encoded in the type
        if (isBoolType(type)) {
            // save the boolean value in a special instance variable.
            booleanValue = (byte) (type & 0x0f) == Types.BOOLEAN_TRUE ? Boolean.TRUE : Boolean.FALSE;
        }

        // push the new field onto the field stack so we can keep the deltas going.
        lastFieldId = field.getId();
        return field;
    }

    /**
     * Read a map header off the wire. If the size is zero, skip reading the key
     * and value type. This means that 0-length maps will yield TMaps without the
     * "correct" types.
     */
    @Override
    public TMap readMapBegin()
            throws TException
    {
        int size = checkSize(readVarint32());
        byte keyAndValueType = size == 0 ? 0 : readByte();
        return new TMap(getTType((byte) (keyAndValueType >> 4)), getTType((byte) (keyAndValueType & 0xf)), size);
    }

    /**
     * Read a list header off the wire. If the list size is 0-14, the size will
     * be packed into the element type header. If it's a longer list, the 4 MSB
     * of the element type header will be 0xF, and a varint will follow with the
     * true size.
     */
    @Override
    public TList readListBegin()
            throws TException
    {
        byte sizeAndType = readByte();
        int size = (sizeAndType >> 4) & 0x0f;
        if (size == 15) {
            size = readVarint32();
        }
        checkSize(size);
        byte type = getTType(sizeAndType);
        return new TList(type, size);
    }

    /**
     * Read a set header off the wire. If the set size is 0-14, the size will
     * be packed into the element type header. If it's a longer set, the 4 MSB
     * of the element type header will be 0xF, and a varint will follow with the
     * true size.
     */
    @Override
    public TSet readSetBegin()
            throws TException
    {
        return new TSet(readListBegin());
    }

    /**
     * Read a boolean off the wire. If this is a boolean field, the value should
     * already have been read during readFieldBegin, so we'll just consume the
     * pre-stored value. Otherwise, read a byte.
     */
    @Override
    public boolean readBool()
            throws TException
    {
        if (booleanValue != null) {
            boolean result = booleanValue;
            booleanValue = null;
            return result;
        }
        return readByte() == Types.BOOLEAN_TRUE;
    }

    private final byte[] byteRawBuf = new byte[1];

    /**
     * Read a single byte off the wire. Nothing interesting here.
     */
    @Override
    public byte readByte()
            throws TException
    {
        transport.read(byteRawBuf, 0, 1);
        return byteRawBuf[0];
    }

    /**
     * Read an i16 from the wire as a zigzag varint.
     */
    @Override
    public short readI16()
            throws TException
    {
        return (short) zigzagToInt(readVarint32());
    }

    /**
     * Read an i32 from the wire as a zigzag varint.
     */
    @Override
    public int readI32()
            throws TException
    {
        return zigzagToInt(readVarint32());
    }

    /**
     * Read an i64 from the wire as a zigzag varint.
     */
    @Override
    public long readI64()
            throws TException
    {
        return zigzagToLong(readVarint64());
    }

    private final byte[] doubleBuf = new byte[8];

    /**
     * No magic here - just read a double off the wire.
     */
    @Override
    public double readDouble()
            throws TException
    {
        transport.read(doubleBuf, 0, 8);
        return longBitsToDouble(bytesToLong(doubleBuf));
    }

    /**
     * Reads a byte[] (via readBinary), and then UTF-8 decodes it.
     */
    @Override
    public String readString()
            throws TException
    {
        int length = checkSize(readVarint32());
        if (length == 0) {
            return "";
        }
        return new String(readBinary(length), UTF_8);
    }

    /**
     * Read a byte[] from the wire.
     */
    @Override
    public ByteBuffer readBinary()
            throws TException
    {
        int length = checkSize(readVarint32());
        if (length == 0) {
            return ByteBuffer.wrap(EMPTY_BYTE_ARRAY);
        }

        byte[] buf = new byte[length];
        transport.read(buf, 0, length);
        return ByteBuffer.wrap(buf);
    }

    /**
     * Read a byte[] of a known length from the wire.
     */
    private byte[] readBinary(int length)
            throws TException
    {
        if (length == 0) {
            return EMPTY_BYTE_ARRAY;
        }

        byte[] buf = new byte[length];
        transport.read(buf, 0, length);
        return buf;
    }

    private static int checkSize(int length)
            throws TProtocolException
    {
        if (length < 0) {
            throw new TProtocolException("Negative length: " + length);
        }
        return length;
    }

    //
    // These methods are here for the struct to call, but don't have any wire
    // encoding.
    //
    @Override
    public void readMessageEnd() {}

    @Override
    public void readFieldEnd() {}

    @Override
    public void readMapEnd() {}

    @Override
    public void readListEnd() {}

    @Override
    public void readSetEnd() {}

    //
    // Internal reading methods
    //

    /**
     * Read an i32 from the wire as a varint. The MSB of each byte is set
     * if there is another byte to follow. This can read up to 5 bytes.
     */
    private int readVarint32()
            throws TException
    {
        int result = 0;
        int shift = 0;
        while (true) {
            byte b = readByte();
            result |= (b & 0x7f) << shift;
            if ((b & 0x80) != 0x80) {
                break;
            }
            shift += 7;
        }
        return result;
    }

    /**
     * Read an i64 from the wire as a proper varint. The MSB of each byte is set
     * if there is another byte to follow. This can read up to 10 bytes.
     */
    private long readVarint64()
            throws TException
    {
        int shift = 0;
        long result = 0;
        while (true) {
            byte b = readByte();
            result |= (long) (b & 0x7f) << shift;
            if ((b & 0x80) != 0x80) {
                break;
            }
            shift += 7;
        }
        return result;
    }

    //
    // encoding helpers
    //

    /**
     * Convert from zigzag int to int.
     */
    private static int zigzagToInt(int n)
    {
        return (n >>> 1) ^ -(n & 1);
    }

    /**
     * Convert from zigzag long to long.
     */
    private static long zigzagToLong(long n)
    {
        return (n >>> 1) ^ -(n & 1);
    }

    /**
     * Note that it's important that the mask bytes are long literals,
     * otherwise they'll default to ints, and when you shift an int left 56 bits,
     * you just get a messed up int.
     */
    private static long bytesToLong(byte[] bytes)
    {
        return ((bytes[7] & 0xffL) << 56) |
                ((bytes[6] & 0xffL) << 48) |
                ((bytes[5] & 0xffL) << 40) |
                ((bytes[4] & 0xffL) << 32) |
                ((bytes[3] & 0xffL) << 24) |
                ((bytes[2] & 0xffL) << 16) |
                ((bytes[1] & 0xffL) << 8) |
                ((bytes[0] & 0xffL));
    }

    //
    // type testing and converting
    //

    private static boolean isBoolType(byte b)
    {
        int lowerNibble = b & 0x0f;
        return lowerNibble == Types.BOOLEAN_TRUE || lowerNibble == Types.BOOLEAN_FALSE;
    }

    /**
     * Given a TCompactProtocol.Types constant, convert it to its corresponding
     * TType value.
     */
    private static byte getTType(byte type)
            throws TProtocolException
    {
        switch ((byte) (type & 0x0f)) {
            case TType.STOP:
                return TType.STOP;
            case Types.BOOLEAN_FALSE:
            case Types.BOOLEAN_TRUE:
                return TType.BOOL;
            case Types.BYTE:
                return TType.BYTE;
            case Types.I16:
                return TType.I16;
            case Types.I32:
                return TType.I32;
            case Types.I64:
                return TType.I64;
            case Types.DOUBLE:
                return TType.DOUBLE;
            case Types.BINARY:
                return TType.STRING;
            case Types.LIST:
                return TType.LIST;
            case Types.SET:
                return TType.SET;
            case Types.MAP:
                return TType.MAP;
            case Types.STRUCT:
                return TType.STRUCT;
            default:
                throw new TProtocolException("don't know what type: " + (byte) (type & 0x0f));
        }
    }

    /**
     * Given a TType value, find the appropriate TCompactProtocol.Types constant.
     */
    private static byte getCompactType(byte ttype)
    {
        return TTYPE_TO_COMPACT_TYPE[ttype];
    }
}
