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
import io.airlift.drift.integration.scribe.apache.ScribeService;
import io.airlift.drift.integration.scribe.apache.scribe;
import io.airlift.drift.transport.netty.codec.Protocol;
import io.airlift.drift.transport.netty.codec.Transport;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.testng.annotations.Test;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;

import java.util.List;
import java.util.function.ToIntFunction;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Lists.newArrayList;
import static io.airlift.drift.integration.ApacheThriftTesterUtil.apacheThriftTestClients;
import static io.airlift.drift.integration.ClientTestUtils.MESSAGES;
import static io.airlift.drift.integration.DriftNettyTesterUtil.driftNettyTestClients;
import static io.airlift.drift.integration.LegacyApacheThriftTesterUtil.legacyApacheThriftTestClients;
import static io.airlift.drift.transport.netty.codec.Protocol.FB_COMPACT;
import static io.airlift.drift.transport.netty.codec.Transport.HEADER;
import static java.util.Collections.nCopies;
import static org.testng.Assert.assertEquals;

public class TestClientsWithApacheServer
{
    @Test
    public static void testApacheServer()
            throws Exception
    {
        testApacheServer(ImmutableList.of());
    }

    @Test
    public void testHandlersWithApacheServer()
            throws Exception
    {
        TestFilter firstFilter = new TestFilter();
        TestFilter secondFilter = new TestFilter();
        List<MethodInvocationFilter> filters = ImmutableList.of(firstFilter, secondFilter);

        int invocationCount = testApacheServer(filters);

        firstFilter.assertCounts(invocationCount);
        secondFilter.assertCounts(invocationCount);
    }

    private static int testApacheServer(List<MethodInvocationFilter> filters)
            throws Exception
    {
        ScribeService scribeService = new ScribeService();
        TProcessor processor = new scribe.Processor<>(scribeService);

        int invocationCount = 0;
        for (boolean secure : ImmutableList.of(true, false)) {
            for (Transport transport : Transport.values()) {
                for (Protocol protocol : Protocol.values()) {
                    invocationCount += testApacheServer(secure, transport, protocol, processor, ImmutableList.<ToIntFunction<HostAndPort>>builder()
                            .addAll(legacyApacheThriftTestClients(filters, transport, protocol, secure))
                            .addAll(driftNettyTestClients(filters, transport, protocol, secure))
                            .addAll(apacheThriftTestClients(filters, transport, protocol, secure))
                            .build());
                }
            }
        }

        assertEquals(scribeService.getMessages(), newArrayList(concat(nCopies(invocationCount, MESSAGES))));

        return invocationCount;
    }

    private static int testApacheServer(boolean secure, Transport transport, Protocol protocol, TProcessor processor, List<ToIntFunction<HostAndPort>> clients)
            throws Exception
    {
        // Apache server does not support header transport
        if (transport == HEADER || protocol == FB_COMPACT) {
            return 0;
        }

        TTransportFactory transportFactory;
        switch (transport) {
            case UNFRAMED:
                transportFactory = new TTransportFactory();
                break;
            case FRAMED:
                transportFactory = new TFramedTransport.Factory();
                break;
            default:
                throw new IllegalArgumentException("Unsupported transport " + transport);
        }
        TProtocolFactory protocolFactory;
        switch (protocol) {
            case BINARY:
                protocolFactory = new TBinaryProtocol.Factory();
                break;
            case COMPACT:
                protocolFactory = new TCompactProtocol.Factory();
                break;
            default:
                throw new IllegalArgumentException("Unsupported protocol " + protocol);
        }

        try (TServerSocket serverSocket = createServerTransport(secure)) {
            TServer server = new TSimpleServer(new Args(serverSocket)
                    .protocolFactory(protocolFactory)
                    .transportFactory(transportFactory)
                    .processor(processor));

            Thread serverThread = new Thread(server::serve);
            try {
                serverThread.start();

                int localPort = serverSocket.getServerSocket().getLocalPort();
                HostAndPort address = HostAndPort.fromParts("localhost", localPort);

                int sum = 0;
                for (ToIntFunction<HostAndPort> client : clients) {
                    sum += client.applyAsInt(address);
                }
                return sum;
            }
            finally {
                server.stop();
                serverThread.interrupt();
            }
        }
    }

    private static TServerSocket createServerTransport(boolean secure)
            throws TTransportException
    {
        if (!secure) {
            return new TServerSocket(0);
        }

        try {
            SSLContext serverSslContext = ClientTestUtils.getServerSslContext();
            SSLServerSocket serverSocket = (SSLServerSocket) serverSslContext.getServerSocketFactory().createServerSocket(0);
            return new TServerSocket(serverSocket);
        }
        catch (Exception e) {
            throw new TTransportException("Error initializing secure socket", e);
        }
    }
}
