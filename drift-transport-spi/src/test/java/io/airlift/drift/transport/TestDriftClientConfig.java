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
package io.airlift.drift.transport;

import com.google.common.collect.ImmutableMap;
import io.airlift.drift.transport.client.DriftClientConfig;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestDriftClientConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(DriftClientConfig.class)
                .setMaxRetries(5)
                .setMinBackoffDelay(new Duration(100, MILLISECONDS))
                .setMaxBackoffDelay(new Duration(30, SECONDS))
                .setBackoffScaleFactor(2.0)
                .setMaxRetryTime(new Duration(1, MINUTES))
                .setStatsEnabled(true));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("thrift.client.max-retries", "99")
                .put("thrift.client.min-backoff-delay", "11ms")
                .put("thrift.client.max-backoff-delay", "22m")
                .put("thrift.client.backoff-scale-factor", "2.2")
                .put("thrift.client.max-retry-time", "33m")
                .put("thrift.client.stats.enabled", "false")
                .build();

        DriftClientConfig expected = new DriftClientConfig()
                .setMaxRetries(99)
                .setMinBackoffDelay(new Duration(11, MILLISECONDS))
                .setMaxBackoffDelay(new Duration(22, MINUTES))
                .setBackoffScaleFactor(2.2)
                .setMaxRetryTime(new Duration(33, MINUTES))
                .setStatsEnabled(false);

        assertFullMapping(properties, expected);
    }
}
