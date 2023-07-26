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
package io.airlift.drift.integration;

import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import io.airlift.drift.client.MethodInvocationFilter;
import io.airlift.drift.integration.scribe.apache.LogEntry;
import io.airlift.drift.integration.scribe.apache.ResultCode;
import io.airlift.drift.integration.scribe.apache.scribe;
import io.airlift.drift.transport.netty.codec.Protocol;
import io.airlift.drift.transport.netty.codec.Transport;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.apache.thrift.transport.layered.TFramedTransport;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;

import java.util.List;
import java.util.function.ToIntFunction;

import static io.airlift.drift.integration.ClientTestUtils.MESSAGES;
import static org.testng.Assert.assertEquals;

final class LegacyApacheThriftTesterUtil
{
    private LegacyApacheThriftTesterUtil() {}

    public static List<ToIntFunction<HostAndPort>> legacyApacheThriftTestClients(List<MethodInvocationFilter> filters, Transport transport, Protocol protocol, boolean secure)
    {
        return ImmutableList.of(
                address -> logThrift(address, MESSAGES, filters, transport, protocol, secure));
    }

    private static int logThrift(HostAndPort address, List<LogEntry> messages, List<MethodInvocationFilter> filters, Transport transportType, Protocol protocolType, boolean secure)
    {
        if (!filters.isEmpty()) {
            return 0;
        }

        TTransportFactory transportFactory;
        switch (transportType) {
            case UNFRAMED:
                transportFactory = new TTransportFactory();
                break;
            case FRAMED:
                transportFactory = new TFramedTransport.Factory();
                break;
            case HEADER:
                return 0;
            default:
                throw new IllegalArgumentException("Unsupported transport " + transportType);
        }

        try (TSocket socket = createClientSocket(secure, address)) {
            if (!socket.isOpen()) {
                socket.open();
            }
            TTransport transport = transportFactory.getTransport(socket);
            TProtocol protocol;
            switch (protocolType) {
                case BINARY:
                    protocol = new TBinaryProtocol(transport);
                    break;
                case COMPACT:
                    protocol = new TCompactProtocol(transport);
                    break;
                case FB_COMPACT:
                    return 0;
                default:
                    throw new IllegalArgumentException("Unsupported protocol " + protocolType);
            }

            assertEquals(new scribe.Client(protocol).Log(messages), ResultCode.OK);
        }
        catch (TException e) {
            throw new RuntimeException(e);
        }
        return 1;
    }

    private static TSocket createClientSocket(boolean secure, HostAndPort address)
            throws TTransportException
    {
        if (!secure) {
            return new TSocket(address.getHost(), address.getPort());
        }

        try {
            SSLContext serverSslContext = ClientTestUtils.getClientSslContext();
            SSLSocket clientSocket = (SSLSocket) serverSslContext.getSocketFactory().createSocket(address.getHost(), address.getPort());
            //            clientSocket.setSoTimeout(timeout);
            return new TSocket(clientSocket);
        }
        catch (Exception e) {
            throw new TTransportException("Error initializing secure socket", e);
        }
    }
}
