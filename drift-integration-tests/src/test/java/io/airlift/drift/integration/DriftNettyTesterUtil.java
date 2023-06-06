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
import io.airlift.drift.client.DriftClientFactory;
import io.airlift.drift.client.DriftClientFactoryManager;
import io.airlift.drift.client.MethodInvocationFilter;
import io.airlift.drift.client.address.AddressSelector;
import io.airlift.drift.integration.scribe.drift.DriftAsyncScribe;
import io.airlift.drift.integration.scribe.drift.DriftLogEntry;
import io.airlift.drift.integration.scribe.drift.DriftScribe;
import io.airlift.drift.transport.client.DriftClientConfig;
import io.airlift.drift.transport.netty.buffer.TestingPooledByteBufAllocator;
import io.airlift.drift.transport.netty.client.DriftNettyClientConfig;
import io.airlift.drift.transport.netty.client.DriftNettyClientModule;
import io.airlift.drift.transport.netty.client.DriftNettyConnectionFactoryConfig;
import io.airlift.drift.transport.netty.client.DriftNettyMethodInvokerFactory;
import io.airlift.drift.transport.netty.codec.Protocol;
import io.airlift.drift.transport.netty.codec.Transport;

import java.util.List;
import java.util.Optional;
import java.util.function.ToIntFunction;

import static io.airlift.drift.client.ExceptionClassifier.NORMAL_RESULT;
import static io.airlift.drift.integration.ClientTestUtils.CODEC_MANAGER;
import static io.airlift.drift.integration.ClientTestUtils.DRIFT_MESSAGES;
import static io.airlift.drift.integration.ClientTestUtils.DRIFT_OK;
import static io.airlift.drift.integration.ClientTestUtils.HEADER_VALUE;
import static io.airlift.drift.integration.ClientTestUtils.logDriftClientBinder;
import static io.airlift.drift.transport.netty.client.DriftNettyMethodInvokerFactory.createStaticDriftNettyMethodInvokerFactory;
import static io.airlift.drift.transport.netty.codec.Protocol.COMPACT;
import static io.airlift.drift.transport.netty.codec.Transport.HEADER;
import static org.testng.Assert.assertEquals;

final class DriftNettyTesterUtil
{
    private DriftNettyTesterUtil() {}

    public static List<ToIntFunction<HostAndPort>> driftNettyTestClients(List<MethodInvocationFilter> filters, Transport transport, Protocol protocol, boolean secure)
    {
        return ImmutableList.of(
                address -> logNettyDriftClient(address, HEADER_VALUE, DRIFT_MESSAGES, filters, transport, protocol, secure),
                address -> logNettyStaticDriftClient(address, HEADER_VALUE, DRIFT_MESSAGES, filters, transport, protocol, secure),
                address -> logNettyDriftClientAsync(address, HEADER_VALUE, DRIFT_MESSAGES, filters, transport, protocol, secure),
                address -> logNettyClientBinder(address, HEADER_VALUE, DRIFT_MESSAGES, filters, transport, protocol, secure));
    }

    private static int logNettyDriftClient(
            HostAndPort address,
            String headerValue,
            List<DriftLogEntry> entries,
            List<MethodInvocationFilter> filters,
            Transport transport,
            Protocol protocol,
            boolean secure)
    {
        if (!isValidConfiguration(transport, protocol)) {
            return 0;
        }

        AddressSelector<?> addressSelector = (context, attempted) -> Optional.of(() -> address);
        DriftNettyClientConfig config = new DriftNettyClientConfig()
                .setTransport(transport)
                .setProtocol(protocol)
                .setTrustCertificate(ClientTestUtils.getCertificateChainFile())
                .setSslEnabled(secure);

        try (TestingPooledByteBufAllocator testingAllocator = new TestingPooledByteBufAllocator();
                DriftNettyMethodInvokerFactory<String> methodInvokerFactory = new DriftNettyMethodInvokerFactory<>(
                        new DriftNettyConnectionFactoryConfig(),
                        clientIdentity -> config,
                        testingAllocator)) {
            DriftClientFactoryManager<String> clientFactoryManager = new DriftClientFactoryManager<>(CODEC_MANAGER, methodInvokerFactory);
            DriftClientFactory proxyFactory = clientFactoryManager.createDriftClientFactory("clientIdentity", addressSelector, NORMAL_RESULT);

            DriftScribe scribe = proxyFactory.createDriftClient(DriftScribe.class, Optional.empty(), filters, new DriftClientConfig()).get();

            assertEquals(scribe.log(headerValue, entries), DRIFT_OK);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        return 1;
    }

    private static int logNettyStaticDriftClient(
            HostAndPort address,
            String headerValue,
            List<DriftLogEntry> entries,
            List<MethodInvocationFilter> filters,
            Transport transport,
            Protocol protocol,
            boolean secure)
    {
        if (!isValidConfiguration(transport, protocol)) {
            return 0;
        }

        AddressSelector<?> addressSelector = (context, attempted) -> Optional.of(() -> address);
        DriftNettyClientConfig config = new DriftNettyClientConfig()
                .setTransport(transport)
                .setProtocol(protocol)
                .setTrustCertificate(ClientTestUtils.getCertificateChainFile())
                .setSslEnabled(secure);

        try (TestingPooledByteBufAllocator testingAllocator = new TestingPooledByteBufAllocator();
                DriftNettyMethodInvokerFactory<?> methodInvokerFactory = createStaticDriftNettyMethodInvokerFactory(config, testingAllocator)) {
            DriftClientFactory proxyFactory = new DriftClientFactory(CODEC_MANAGER, methodInvokerFactory, addressSelector);

            DriftScribe scribe = proxyFactory.createDriftClient(DriftScribe.class, Optional.empty(), filters, new DriftClientConfig()).get();

            assertEquals(scribe.log(headerValue, entries), DRIFT_OK);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        return 1;
    }

    private static int logNettyDriftClientAsync(
            HostAndPort address,
            String headerValue,
            List<DriftLogEntry> entries,
            List<MethodInvocationFilter> filters,
            Transport transport,
            Protocol protocol,
            boolean secure)
    {
        if (!isValidConfiguration(transport, protocol)) {
            return 0;
        }

        AddressSelector<?> addressSelector = (context, attempted) -> Optional.of(() -> address);
        DriftNettyClientConfig config = new DriftNettyClientConfig()
                .setTransport(transport)
                .setProtocol(protocol)
                .setTrustCertificate(ClientTestUtils.getCertificateChainFile())
                .setSslEnabled(secure);

        try (TestingPooledByteBufAllocator testingAllocator = new TestingPooledByteBufAllocator();
                DriftNettyMethodInvokerFactory<String> methodInvokerFactory = new DriftNettyMethodInvokerFactory<>(
                        new DriftNettyConnectionFactoryConfig(),
                        clientIdentity -> config,
                        testingAllocator)) {
            DriftClientFactoryManager<String> proxyFactoryManager = new DriftClientFactoryManager<>(CODEC_MANAGER, methodInvokerFactory);
            DriftClientFactory proxyFactory = proxyFactoryManager.createDriftClientFactory("myFactory", addressSelector, NORMAL_RESULT);

            DriftAsyncScribe scribe = proxyFactory.createDriftClient(DriftAsyncScribe.class, Optional.empty(), filters, new DriftClientConfig()).get();

            assertEquals(scribe.log(headerValue, entries).get(), DRIFT_OK);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        return 1;
    }

    private static int logNettyClientBinder(
            HostAndPort address,
            String headerValue,
            List<DriftLogEntry> entries,
            List<MethodInvocationFilter> filters,
            Transport transport,
            Protocol protocol,
            boolean secure)
    {
        if (!isValidConfiguration(transport, protocol)) {
            return 0;
        }

        try (TestingPooledByteBufAllocator testingAllocator = new TestingPooledByteBufAllocator()) {
            return logDriftClientBinder(address, headerValue, entries, new DriftNettyClientModule(testingAllocator), filters, transport, protocol, secure);
        }
    }

    private static boolean isValidConfiguration(Transport transport, Protocol protocol)
    {
        // HEADER transport cannot be used with COMPACT protocol
        return transport != HEADER || protocol != COMPACT;
    }
}
