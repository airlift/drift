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
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.drift.client.MethodInvocationFilter;
import io.airlift.drift.codec.ThriftCodec;
import io.airlift.drift.codec.ThriftCodecManager;
import io.airlift.drift.integration.scribe.apache.LogEntry;
import io.airlift.drift.integration.scribe.drift.DriftLogEntry;
import io.airlift.drift.integration.scribe.drift.DriftResultCode;
import io.airlift.drift.transport.MethodMetadata;
import io.airlift.drift.transport.ParameterMetadata;
import io.airlift.drift.transport.netty.codec.Protocol;
import io.airlift.drift.transport.netty.codec.Transport;
import io.airlift.drift.transport.netty.server.DriftNettyServerConfig;
import io.airlift.drift.transport.netty.server.DriftNettyServerTransport;
import io.airlift.drift.transport.netty.server.DriftNettyServerTransportFactory;
import io.airlift.drift.transport.server.ServerInvokeRequest;
import io.airlift.drift.transport.server.ServerMethodInvoker;
import io.airlift.drift.transport.server.ServerTransport;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.ToIntFunction;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Lists.newArrayList;
import static io.airlift.drift.codec.metadata.ThriftType.list;
import static io.airlift.drift.integration.ApacheThriftTesterUtil.apacheThriftTestClients;
import static io.airlift.drift.integration.ClientTestUtils.DRIFT_OK;
import static io.airlift.drift.integration.ClientTestUtils.MESSAGES;
import static io.airlift.drift.integration.DriftNettyTesterUtil.driftNettyTestClients;
import static io.airlift.drift.integration.LegacyApacheThriftTesterUtil.legacyApacheThriftTestClients;
import static java.util.Collections.nCopies;
import static org.testng.Assert.assertEquals;

public class TestClientsWithDriftNettyServerTransport
{
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
        TestServerMethodInvoker methodInvoker = new TestServerMethodInvoker();

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
        int invocationCount = testDriftServer(methodInvoker, clients.build());

        assertEquals(methodInvoker.getMessages(), newArrayList(concat(nCopies(invocationCount, MESSAGES))));

        return invocationCount;
    }

    private static int testDriftServer(ServerMethodInvoker methodInvoker, List<ToIntFunction<HostAndPort>> clients)
            throws Exception
    {
        DriftNettyServerConfig config = new DriftNettyServerConfig()
                .setSslEnabled(true)
                .setTrustCertificate(ClientTestUtils.getCertificateChainFile())
                .setKey(ClientTestUtils.getPrivateKeyFile());
        ServerTransport serverTransport = new DriftNettyServerTransportFactory(config).createServerTransport(methodInvoker);
        try {
            serverTransport.start();

            HostAndPort address = HostAndPort.fromParts("localhost", ((DriftNettyServerTransport) serverTransport).getPort());

            int sum = 0;
            for (ToIntFunction<HostAndPort> client : clients) {
                sum += client.applyAsInt(address);
            }
            return sum;
        }
        finally {
            serverTransport.shutdown();
        }
    }

    private static final ThriftCodecManager CODEC_MANAGER = new ThriftCodecManager();
    private static final MethodMetadata LOG_METHOD_METADATA = new MethodMetadata(
            "Log",
            ImmutableList.of(new ParameterMetadata(
                    (short) 1,
                    "messages",
                    (ThriftCodec<Object>) CODEC_MANAGER.getCodec(list(CODEC_MANAGER.getCodec(DriftLogEntry.class).getType())))),
            (ThriftCodec<Object>) (Object) CODEC_MANAGER.getCodec(DriftResultCode.class),
            ImmutableMap.of(),
            false);

    private static class TestServerMethodInvoker
            implements ServerMethodInvoker
    {
        private final List<LogEntry> messages = new CopyOnWriteArrayList<>();

        private List<LogEntry> getMessages()
        {
            return messages;
        }

        @Override
        public Optional<MethodMetadata> getMethodMetadata(String name)
        {
            if (LOG_METHOD_METADATA.getName().equals(name)) {
                return Optional.of(LOG_METHOD_METADATA);
            }
            return Optional.empty();
        }

        @Override
        public ListenableFuture<Object> invoke(ServerInvokeRequest request)
        {
            MethodMetadata method = request.getMethod();
            if (!LOG_METHOD_METADATA.getName().equals(method.getName())) {
                return Futures.immediateFailedFuture(new IllegalArgumentException("unknown method " + method));
            }

            Map<Short, Object> parameters = request.getParameters();
            if (parameters.size() != 1 || !parameters.containsKey((short) 1) || !(getOnlyElement(parameters.values()) instanceof List)) {
                return Futures.immediateFailedFuture(new IllegalArgumentException("invalid parameters"));
            }
            for (DriftLogEntry driftLogEntry : (List<DriftLogEntry>) getOnlyElement(parameters.values())) {
                messages.add(new LogEntry(driftLogEntry.getCategory(), driftLogEntry.getMessage()));
            }

            return Futures.immediateFuture(DRIFT_OK);
        }

        @Override
        public void recordResult(String methodName, long startTime, ListenableFuture<Object> result)
        {
            // TODO implement
        }
    }
}
