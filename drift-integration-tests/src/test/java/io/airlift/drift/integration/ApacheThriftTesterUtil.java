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
import io.airlift.drift.transport.apache.client.ApacheThriftClientConfig;
import io.airlift.drift.transport.apache.client.ApacheThriftClientModule;
import io.airlift.drift.transport.apache.client.ApacheThriftConnectionFactoryConfig;
import io.airlift.drift.transport.apache.client.ApacheThriftMethodInvokerFactory;
import io.airlift.drift.transport.client.DriftClientConfig;
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
import static io.airlift.drift.transport.apache.client.ApacheThriftMethodInvokerFactory.createStaticApacheThriftMethodInvokerFactory;
import static io.airlift.drift.transport.netty.codec.Protocol.FB_COMPACT;
import static io.airlift.drift.transport.netty.codec.Transport.HEADER;
import static org.testng.Assert.assertEquals;

final class ApacheThriftTesterUtil
{
    private ApacheThriftTesterUtil() {}

    public static List<ToIntFunction<HostAndPort>> apacheThriftTestClients(List<MethodInvocationFilter> filters, Transport transport, Protocol protocol, boolean secure)
    {
        return ImmutableList.of(
                address -> logApacheThriftDriftClient(address, HEADER_VALUE, DRIFT_MESSAGES, filters, transport, protocol, secure),
                address -> logApacheThriftStaticDriftClient(address, HEADER_VALUE, DRIFT_MESSAGES, filters, transport, protocol, secure),
                address -> logApacheThriftDriftClientAsync(address, HEADER_VALUE, DRIFT_MESSAGES, filters, transport, protocol, secure),
                address -> logApacheThriftClientBinder(address, HEADER_VALUE, DRIFT_MESSAGES, filters, transport, protocol, secure));
    }

    private static int logApacheThriftDriftClient(
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

        AddressSelector<?> addressSelector = context -> Optional.of(() -> address);
        ApacheThriftClientConfig config = new ApacheThriftClientConfig()
                .setTransport(toApacheThriftTransport(transport))
                .setProtocol(toApacheThriftProtocol(protocol))
                .setTrustCertificate(ClientTestUtils.getCertificateChainFile())
                .setSslEnabled(secure);
        ApacheThriftConnectionFactoryConfig factoryConfig = new ApacheThriftConnectionFactoryConfig();
        try (ApacheThriftMethodInvokerFactory<String> methodInvokerFactory = new ApacheThriftMethodInvokerFactory<>(factoryConfig, clientIdentity -> config)) {
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

    private static int logApacheThriftStaticDriftClient(
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

        AddressSelector<?> addressSelector = context -> Optional.of(() -> address);
        ApacheThriftClientConfig config = new ApacheThriftClientConfig()
                .setTransport(toApacheThriftTransport(transport))
                .setProtocol(toApacheThriftProtocol(protocol))
                .setTrustCertificate(ClientTestUtils.getCertificateChainFile())
                .setSslEnabled(secure);

        try (ApacheThriftMethodInvokerFactory<?> methodInvokerFactory = createStaticApacheThriftMethodInvokerFactory(config)) {
            DriftClientFactory proxyFactory = new DriftClientFactory(CODEC_MANAGER, methodInvokerFactory, addressSelector);

            DriftScribe scribe = proxyFactory.createDriftClient(DriftScribe.class, Optional.empty(), filters, new DriftClientConfig()).get();

            assertEquals(scribe.log(headerValue, entries), DRIFT_OK);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        return 1;
    }

    private static int logApacheThriftDriftClientAsync(
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

        AddressSelector<?> addressSelector = context -> Optional.of(() -> address);
        ApacheThriftClientConfig config = new ApacheThriftClientConfig()
                .setTransport(toApacheThriftTransport(transport))
                .setProtocol(toApacheThriftProtocol(protocol))
                .setTrustCertificate(ClientTestUtils.getCertificateChainFile())
                .setSslEnabled(secure);
        ApacheThriftConnectionFactoryConfig factoryConfig = new ApacheThriftConnectionFactoryConfig();
        try (ApacheThriftMethodInvokerFactory<String> methodInvokerFactory = new ApacheThriftMethodInvokerFactory<>(factoryConfig, clientIdentity -> config)) {
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

    private static int logApacheThriftClientBinder(
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

        return logDriftClientBinder(address, headerValue, entries, new ApacheThriftClientModule(), filters, transport, protocol, secure);
    }

    private static boolean isValidConfiguration(Transport transport, Protocol protocol)
    {
        // Apache thrift client does not support header protocol
        return transport != HEADER && protocol != FB_COMPACT;
    }

    private static ApacheThriftClientConfig.Transport toApacheThriftTransport(Transport transport)
    {
        switch (transport) {
            case UNFRAMED:
                return ApacheThriftClientConfig.Transport.UNFRAMED;
            case FRAMED:
                return ApacheThriftClientConfig.Transport.FRAMED;
            default:
                throw new IllegalArgumentException("Unsupported transport " + transport);
        }
    }

    private static ApacheThriftClientConfig.Protocol toApacheThriftProtocol(Protocol protocol)
    {
        switch (protocol) {
            case BINARY:
                return ApacheThriftClientConfig.Protocol.BINARY;
            case COMPACT:
                return ApacheThriftClientConfig.Protocol.COMPACT;
            default:
                throw new IllegalArgumentException("Unsupported protocol " + protocol);
        }
    }
}
