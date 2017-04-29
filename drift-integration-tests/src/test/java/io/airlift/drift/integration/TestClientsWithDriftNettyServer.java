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
import com.google.common.net.HostAndPort;
import io.airlift.drift.client.MethodInvocationFilter;
import io.airlift.drift.codec.ThriftCodecManager;
import io.airlift.drift.integration.scribe.drift.DriftScribeService;
import io.airlift.drift.server.DriftServer;
import io.airlift.drift.server.DriftService;
import io.airlift.drift.server.stats.NullMethodInvocationStatsFactory;
import io.airlift.drift.transport.netty.DriftNettyClientConfig.Protocol;
import io.airlift.drift.transport.netty.DriftNettyClientConfig.Transport;
import io.airlift.drift.transport.netty.server.DriftNettyServerConfig;
import io.airlift.drift.transport.netty.server.DriftNettyServerTransport;
import io.airlift.drift.transport.netty.server.DriftNettyServerTransportFactory;
import org.testng.annotations.Test;

import java.util.List;
import java.util.function.ToIntFunction;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Lists.newArrayList;
import static io.airlift.drift.integration.ApacheThriftTesterUtil.apacheThriftTestClients;
import static io.airlift.drift.integration.ClientTestUtils.DRIFT_MESSAGES;
import static io.airlift.drift.integration.DriftNettyTesterUtil.driftNettyTestClients;
import static io.airlift.drift.integration.LegacyApacheThriftTesterUtil.legacyApacheThriftTestClients;
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
        ImmutableList.Builder<ToIntFunction<HostAndPort>> clients = ImmutableList.builder();
        for (boolean secure : ImmutableList.of(true, false)) {
            for (Transport transport : Transport.values()) {
                for (Protocol protocol : Protocol.values()) {
                    clients.addAll(legacyApacheThriftTestClients(filters, transport, protocol, secure))
                            .addAll(driftNettyTestClients(filters, transport, protocol, secure))
                            .addAll(apacheThriftTestClients(filters, transport, protocol, secure));
                }
            }
        }

        DriftScribeService scribeService = new DriftScribeService();
        int invocationCount = testDriftServer(new DriftService(scribeService), clients.build());

        assertEquals(scribeService.getMessages(), newArrayList(concat(nCopies(invocationCount, DRIFT_MESSAGES))));

        return invocationCount;
    }

    private static int testDriftServer(DriftService service, List<ToIntFunction<HostAndPort>> clients)
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

            int sum = 0;
            for (ToIntFunction<HostAndPort> client : clients) {
                sum += client.applyAsInt(address);
            }
            return sum;
        }
        finally {
            driftServer.shutdownGracefully();
        }
    }
}
