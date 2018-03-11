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
package io.airlift.drift.integration;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import com.google.common.net.HostAndPort;
import io.airlift.drift.client.MethodInvocationFilter;
import io.airlift.drift.codec.ThriftCodecManager;
import io.airlift.drift.integration.scribe.drift.DriftScribeService;
import io.airlift.drift.server.DriftServer;
import io.airlift.drift.server.DriftService;
import io.airlift.drift.server.stats.NullMethodInvocationStatsFactory;
import io.airlift.drift.transport.netty.DriftNettyClientConfig.Transport;
import io.airlift.drift.transport.netty.Protocol;
import io.airlift.drift.transport.netty.server.DriftNettyServerConfig;
import io.airlift.drift.transport.netty.server.DriftNettyServerTransport;
import io.airlift.drift.transport.netty.server.DriftNettyServerTransportFactory;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Lists.newArrayList;
import static io.airlift.drift.integration.ApacheThriftTesterUtil.apacheThriftTestClients;
import static io.airlift.drift.integration.ClientTestUtils.DRIFT_MESSAGES;
import static io.airlift.drift.integration.ClientTestUtils.HEADER_VALUE;
import static io.airlift.drift.integration.DriftNettyTesterUtil.driftNettyTestClients;
import static io.airlift.drift.integration.LegacyApacheThriftTesterUtil.legacyApacheThriftTestClients;
import static io.airlift.drift.transport.netty.DriftNettyClientConfig.Transport.HEADER;
import static java.util.Collections.nCopies;
import static org.testng.Assert.assertEquals;

public class TestClientsWithDriftNettyServer
{
    private static final ThriftCodecManager CODEC_MANAGER = new ThriftCodecManager();

    @Test
    public void testDriftServer()
            throws Exception
    {
        testDriftServer(ImmutableList.of());
    }

    @Test
    public void testHandlersWithDriftServer()
            throws Exception
    {
        TestFilter firstFilter = new TestFilter();
        TestFilter secondFilter = new TestFilter();
        List<MethodInvocationFilter> filters = ImmutableList.of(firstFilter, secondFilter);

        int invocationCount = testDriftServer(filters);

        firstFilter.assertCounts(invocationCount);
        secondFilter.assertCounts(invocationCount);
    }

    private static int testDriftServer(List<MethodInvocationFilter> filters)
            throws Exception
    {
        DriftScribeService scribeService = new DriftScribeService();
        AtomicInteger invocationCount = new AtomicInteger();
        AtomicInteger headerInvocationCount = new AtomicInteger();
        testDriftServer(new DriftService(scribeService), address -> {
            for (boolean secure : ImmutableList.of(true, false)) {
                for (Transport transport : ImmutableList.of(HEADER)) {
                    for (Protocol protocol : Protocol.values()) {
                        int count = Streams.concat(
                                legacyApacheThriftTestClients(filters, transport, protocol, secure).stream(),
                                driftNettyTestClients(filters, transport, protocol, secure).stream(),
                                apacheThriftTestClients(filters, transport, protocol, secure).stream())
                                .mapToInt(client -> client.applyAsInt(address))
                                .sum();
                        invocationCount.addAndGet(count);
                        if (transport == HEADER) {
                            headerInvocationCount.addAndGet(count);
                        }
                    }
                }
            }
        });

        assertEquals(scribeService.getMessages(), newArrayList(concat(nCopies(invocationCount.get(), DRIFT_MESSAGES))));
        assertEquals(scribeService.getHeaders(), newArrayList(nCopies(headerInvocationCount.get(), HEADER_VALUE)));

        return invocationCount.get();
    }

    private static void testDriftServer(DriftService service, Consumer<HostAndPort> task)
            throws Exception
    {
        DriftNettyServerConfig config = new DriftNettyServerConfig()
                .setSslEnabled(true)
                .setTrustCertificate(ClientTestUtils.getCertificateChainFile())
                .setKey(ClientTestUtils.getPrivateKeyFile());
        DriftServer driftServer = new DriftServer(
                new DriftNettyServerTransportFactory(config),
                CODEC_MANAGER,
                new NullMethodInvocationStatsFactory(),
                ImmutableSet.of(service),
                ImmutableSet.of());
        try {
            driftServer.start();

            HostAndPort address = HostAndPort.fromParts("localhost", ((DriftNettyServerTransport) driftServer.getServerTransport()).getPort());

            task.accept(address);
        }
        finally {
            driftServer.shutdownGracefully();
        }
    }
}
