/*
 * Copyright (C) 2017 Facebook, Inc.
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
package io.airlift.drift.client.address;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import com.google.inject.Injector;
import com.google.inject.Key;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.drift.annotations.ThriftMethod;
import io.airlift.drift.annotations.ThriftService;
import io.airlift.drift.client.guice.AddressSelectorBinder;
import io.airlift.drift.client.guice.DefaultClient;
import org.testng.annotations.Test;

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Map;

import static io.airlift.drift.client.address.SimpleAddressSelectorBinder.simpleAddressSelector;
import static io.airlift.drift.client.guice.DriftClientAnnotationFactory.getDriftClientAnnotation;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static org.testng.Assert.assertEquals;

public class TestSimpleAddressSelectorBinder
{
    private static final Annotation THRIFT_SERVICE_ANNOTATION = getDriftClientAnnotation(Client.class, DefaultClient.class);

    @Test
    public void testNoDefaults()
            throws Exception
    {
        List<HostAndPort> addresses = ImmutableList.of(HostAndPort.fromParts("example.com", 1), HostAndPort.fromParts("example.com", 2));
        Map<String, String> properties = ImmutableMap.of("testService" + ".thrift.client.addresses", "example.com:1,example.com:2");
        testAddressSelector(simpleAddressSelector(), properties, addresses);
    }

    @Test
    public void testSingleDefault()
            throws Exception
    {
        HostAndPort address = HostAndPort.fromParts("example.com", 1);
        testAddressSelector(simpleAddressSelector(address), ImmutableMap.of(), ImmutableList.of(address));

        testAddressSelector(
                simpleAddressSelector(address),
                ImmutableMap.of("testService" + ".thrift.client.addresses", "example.com:11,example.com:22"),
                ImmutableList.of(HostAndPort.fromParts("example.com", 11), HostAndPort.fromParts("example.com", 22)));
    }

    @Test
    public void testMultipleDefaults()
            throws Exception
    {
        List<HostAndPort> addresses = ImmutableList.of(HostAndPort.fromParts("example.com", 1), HostAndPort.fromParts("example.com", 2));
        testAddressSelector(simpleAddressSelector(addresses), ImmutableMap.of(), addresses);

        testAddressSelector(
                simpleAddressSelector(addresses),
                ImmutableMap.of("testService" + ".thrift.client.addresses", "example.com:11,example.com:22"),
                ImmutableList.of(HostAndPort.fromParts("example.com", 11), HostAndPort.fromParts("example.com", 22)));
    }

    private static void testAddressSelector(
            AddressSelectorBinder addressSelectorBinder,
            Map<String, String> configurationProperties,
            List<HostAndPort> expected)
            throws Exception
    {
        Bootstrap app = new Bootstrap(
                binder -> addressSelectorBinder.bind(binder, THRIFT_SERVICE_ANNOTATION, "testService"));

        LifeCycleManager lifeCycleManager = null;
        try {
            Injector injector = app
                    .setRequiredConfigurationProperties(configurationProperties)
                    .doNotInitializeLogging()
                    .initialize();
            lifeCycleManager = injector.getInstance(LifeCycleManager.class);

            AddressSelector<?> addressSelector = injector.getInstance(Key.get(AddressSelector.class, THRIFT_SERVICE_ANNOTATION));
            assertInstanceOf(addressSelector, SimpleAddressSelector.class);
            SimpleAddressSelector simpleAddressSelector = (SimpleAddressSelector) addressSelector;
            assertEquals(simpleAddressSelector.getAddresses(), expected);
        }
        finally {
            if (lifeCycleManager != null) {
                try {
                    lifeCycleManager.stop();
                }
                catch (Exception ignored) {
                }
            }
        }
    }

    @ThriftService("testService")
    public interface Client
    {
        @ThriftMethod
        String test();
    }
}
