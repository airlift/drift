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
package io.airlift.drift.transport.netty.server;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class TestDriftNettyServerConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(DriftNettyServerConfig.class)
                .setPort(0)
                .setAcceptBacklog(1024)
                .setIoThreadCount(3)
                .setWorkerThreadCount(Runtime.getRuntime().availableProcessors() * 2)
                .setRequestTimeout(new Duration(1, MINUTES))
                .setMaxFrameSize(new DataSize(16, MEGABYTE))
                .setSslContextRefreshTime(new Duration(1, MINUTES))
                .setAllowPlaintext(true)
                .setSslEnabled(false)
                .setTrustCertificate(null)
                .setKey(null)
                .setKeyPassword(null)
                .setSessionCacheSize(10_000)
                .setSessionTimeout(new Duration(1, DAYS))
                .setCiphers("")
                .setAssumeClientsSupportOutOfOrderResponses(true));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("thrift.server.port", "99")
                .put("thrift.server.accept-backlog", "101")
                .put("thrift.server.io-thread-count", "202")
                .put("thrift.server.worker-thread-count", "303")
                .put("thrift.server.request-timeout", "33m")
                .put("thrift.server.max-frame-size", "55MB")
                .put("thrift.server.ssl-context.refresh-time", "33m")
                .put("thrift.server.allow-plaintext", "false")
                .put("thrift.server.ssl.enabled", "true")
                .put("thrift.server.ssl.trust-certificate", "trust")
                .put("thrift.server.ssl.key", "key")
                .put("thrift.server.ssl.key-password", "key_password")
                .put("thrift.server.ssl.session-cache-size", "678")
                .put("thrift.server.ssl.session-timeout", "78h")
                .put("thrift.server.ssl.ciphers", "some_cipher")
                .put("thrift.server.assume-clients-support-out-of-order-responses", "false")
                .build();

        DriftNettyServerConfig expected = new DriftNettyServerConfig()
                .setPort(99)
                .setAcceptBacklog(101)
                .setIoThreadCount(202)
                .setWorkerThreadCount(303)
                .setRequestTimeout(new Duration(33, MINUTES))
                .setMaxFrameSize(new DataSize(55, MEGABYTE))
                .setSslContextRefreshTime(new Duration(33, MINUTES))
                .setAllowPlaintext(false)
                .setSslEnabled(true)
                .setTrustCertificate(new File("trust"))
                .setKey(new File("key"))
                .setKeyPassword("key_password")
                .setSessionCacheSize(678)
                .setSessionTimeout(new Duration(78, HOURS))
                .setCiphers("some_cipher")
                .setAssumeClientsSupportOutOfOrderResponses(false);

        assertFullMapping(properties, expected);
    }
}
