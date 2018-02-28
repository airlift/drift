/*
 * Copyright (C) 2013 Facebook, Inc.
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
package io.airlift.drift.transport.netty;

import io.airlift.drift.TApplicationException;
import io.airlift.drift.codec.ThriftCodec;
import io.airlift.drift.codec.internal.ProtocolReader;
import io.airlift.drift.codec.internal.ProtocolWriter;
import io.airlift.drift.codec.metadata.ThriftType;
import io.airlift.drift.protocol.TMessage;
import io.airlift.drift.protocol.TProtocolFactory;
import io.airlift.drift.protocol.TProtocolReader;
import io.airlift.drift.protocol.TProtocolWriter;
import io.airlift.drift.transport.DriftApplicationException;
import io.airlift.drift.transport.MethodMetadata;
import io.airlift.drift.transport.ParameterMetadata;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

import static io.airlift.drift.TApplicationException.Type.BAD_SEQUENCE_ID;
import static io.airlift.drift.TApplicationException.Type.INVALID_MESSAGE_TYPE;
import static io.airlift.drift.TApplicationException.Type.MISSING_RESULT;
import static io.airlift.drift.TApplicationException.Type.WRONG_METHOD_NAME;
import static io.airlift.drift.protocol.TMessageType.CALL;
import static io.airlift.drift.protocol.TMessageType.EXCEPTION;
import static io.airlift.drift.protocol.TMessageType.ONEWAY;
import static io.airlift.drift.protocol.TMessageType.REPLY;
import static io.airlift.drift.transport.netty.ExceptionReader.readTApplicationException;
import static java.lang.String.format;

interface MessageEncoding
{
    ByteBuf writeRequest(ByteBufAllocator allocator, int sequenceId, MethodMetadata method, List<Object> parameters, Map<String, String> headers)
            throws Exception;

    OptionalInt extractResponseSequenceId(ByteBuf buffer);

    Object readResponse(ByteBuf buffer, int sequenceId, MethodMetadata method)
            throws Exception;

    static ByteBuf encodeRequest(TProtocolFactory protocolFactory, ByteBufAllocator allocator, int sequenceId, MethodMetadata method, List<Object> parameters)
            throws Exception
    {
        TChannelBufferOutputTransport transport = new TChannelBufferOutputTransport(allocator);
        try {
            TProtocolWriter protocol = protocolFactory.getProtocol(transport);

            // Note that though setting message type to ONEWAY can be helpful when looking at packet
            // captures, some clients always send CALL and so servers are forced to rely on the "oneway"
            // attribute on thrift method in the interface definition, rather than checking the message
            // type.
            protocol.writeMessageBegin(new TMessage(method.getName(), method.isOneway() ? ONEWAY : CALL, sequenceId));

            // write the parameters
            ProtocolWriter writer = new ProtocolWriter(protocol);
            writer.writeStructBegin(method.getName() + "_args");
            for (int i = 0; i < parameters.size(); i++) {
                Object value = parameters.get(i);
                ParameterMetadata parameter = method.getParameters().get(i);
                writer.writeField(parameter.getName(), parameter.getFieldId(), parameter.getCodec(), value);
            }
            writer.writeStructEnd();

            protocol.writeMessageEnd();
            return transport.getBuffer();
        }
        finally {
            transport.release();
        }
    }

    static Object decodeResponse(TProtocolFactory protocolFactory, ByteBuf responseMessage, int sequenceId, MethodMetadata method)
            throws Exception
    {
        TChannelBufferInputTransport transport = new TChannelBufferInputTransport(responseMessage);
        try {
            TProtocolReader protocol = protocolFactory.getProtocol(transport);

            // validate response header
            TMessage message = protocol.readMessageBegin();
            if (message.getType() == EXCEPTION) {
                TApplicationException exception = readTApplicationException(protocol);
                protocol.readMessageEnd();
                throw exception;
            }
            if (message.getType() != REPLY) {
                throw new TApplicationException(INVALID_MESSAGE_TYPE, format("Received invalid message type %s from server", message.getType()));
            }
            if (!message.getName().equals(method.getName())) {
                throw new TApplicationException(WRONG_METHOD_NAME, format("Wrong method name in reply: expected %s but received %s", method.getName(), message.getName()));
            }
            if (message.getSequenceId() != sequenceId) {
                throw new TApplicationException(BAD_SEQUENCE_ID, format("%s failed: out of sequence response", method.getName()));
            }

            // read response struct
            ProtocolReader reader = new ProtocolReader(protocol);
            reader.readStructBegin();

            Object results = null;
            Exception exception = null;
            while (reader.nextField()) {
                if (reader.getFieldId() == 0) {
                    results = reader.readField(method.getResultCodec());
                }
                else {
                    ThriftCodec<Object> exceptionCodec = method.getExceptionCodecs().get(reader.getFieldId());
                    if (exceptionCodec != null) {
                        exception = (Exception) reader.readField(exceptionCodec);
                    }
                    else {
                        reader.skipFieldData();
                    }
                }
            }
            reader.readStructEnd();
            protocol.readMessageEnd();

            if (exception != null) {
                throw new DriftApplicationException(exception);
            }

            if (method.getResultCodec().getType() == ThriftType.VOID) {
                return null;
            }

            if (results == null) {
                throw new TApplicationException(MISSING_RESULT, format("%s failed: unknown result", method.getName()));
            }
            return results;
        }
        finally {
            transport.release();
        }
    }
}
