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
package io.airlift.drift.transport.netty.client;

import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.MINUTES;

public class TestDriftNettyConnectionFactoryConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(DriftNettyConnectionFactoryConfig.class)
                .setThreadCount(Runtime.getRuntime().availableProcessors() * 2)
                .setConnectionPoolEnabled(false)
                .setConnectionPoolMaxSize(1000)
                .setConnectionPoolIdleTimeout(new Duration(1, MINUTES))
                .setSslContextRefreshTime(new Duration(1, MINUTES))
                .setSocksProxy(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("thrift.client.thread-count", "99")
                .put("thrift.client.connection-pool.enabled", "true")
                .put("thrift.client.connection-pool.max-size", "555")
                .put("thrift.client.connection-pool.idle-timeout", "7m")
                .put("thrift.client.ssl-context.refresh-time", "33m")
                .put("thrift.client.socks-proxy", "example.com:9876")
                .build();

        DriftNettyConnectionFactoryConfig expected = new DriftNettyConnectionFactoryConfig()
                .setThreadCount(99)
                .setConnectionPoolEnabled(true)
                .setConnectionPoolMaxSize(555)
                .setConnectionPoolIdleTimeout(new Duration(7, MINUTES))
                .setSslContextRefreshTime(new Duration(33, MINUTES))
                .setSocksProxy(HostAndPort.fromParts("example.com", 9876));

        assertFullMapping(properties, expected);
    }
}
