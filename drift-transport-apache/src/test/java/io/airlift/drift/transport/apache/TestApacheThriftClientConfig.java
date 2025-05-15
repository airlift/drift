/*
 * Copyright (C) 2013 Facebook, Inc.
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
package io.airlift.drift.transport.apache;

import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import io.airlift.drift.transport.apache.client.ApacheThriftClientConfig;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.nio.file.Paths;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.drift.transport.apache.client.ApacheThriftClientConfig.Protocol.BINARY;
import static io.airlift.drift.transport.apache.client.ApacheThriftClientConfig.Protocol.COMPACT;
import static io.airlift.drift.transport.apache.client.ApacheThriftClientConfig.Transport.FRAMED;
import static io.airlift.drift.transport.apache.client.ApacheThriftClientConfig.Transport.HEADER;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class TestApacheThriftClientConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(ApacheThriftClientConfig.class)
                .setTransport(FRAMED)
                .setProtocol(BINARY)
                .setConnectTimeout(new Duration(500, MILLISECONDS))
                .setRequestTimeout(new Duration(1, MINUTES))
                .setSocksProxy(null)
                .setMaxFrameSize(new DataSize(16, MEGABYTE))
                .setSslEnabled(false)
                .setTrustCertificate(null)
                .setKey(null)
                .setKeyPassword(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("thrift.client.transport", "HEADER")
                .put("thrift.client.protocol", "COMPACT")
                .put("thrift.client.connect-timeout", "99ms")
                .put("thrift.client.request-timeout", "33m")
                .put("thrift.client.socks-proxy", "localhost:11")
                .put("thrift.client.max-frame-size", "55MB")
                .put("thrift.client.ssl.enabled", "true")
                .put("thrift.client.ssl.trust-certificate", "trust")
                .put("thrift.client.ssl.key", "key")
                .put("thrift.client.ssl.key-password", "key_password")
                .build();

        ApacheThriftClientConfig expected = new ApacheThriftClientConfig()
                .setTransport(HEADER)
                .setProtocol(COMPACT)
                .setConnectTimeout(new Duration(99, MILLISECONDS))
                .setRequestTimeout(new Duration(33, MINUTES))
                .setSocksProxy(HostAndPort.fromParts("localhost", 11))
                .setMaxFrameSize(new DataSize(55, MEGABYTE))
                .setSslEnabled(true)
                .setTrustCertificate(Paths.get("trust").toFile())
                .setKey(Paths.get("key").toFile())
                .setKeyPassword("key_password");

        assertFullMapping(properties, expected);
    }
}
