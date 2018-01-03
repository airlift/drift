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
package io.airlift.drift.transport.client;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class DriftClientConfig
{
    private int maxRetries = 5;
    private Duration minBackoffDelay = new Duration(100, MILLISECONDS);
    private Duration maxBackoffDelay = new Duration(30, SECONDS);
    private double backoffScaleFactor = 2.0;
    private Duration maxRetryTime = new Duration(1, MINUTES);

    private boolean statsEnabled = true;

    @Min(0L)
    public int getMaxRetries()
    {
        return maxRetries;
    }

    @Config("thrift.client.max-retries")
    @ConfigDescription("Minimum number of retry attempts")
    public DriftClientConfig setMaxRetries(int maxRetries)
    {
        this.maxRetries = maxRetries;
        return this;
    }

    @MinDuration("1ms")
    @NotNull
    public Duration getMinBackoffDelay()
    {
        return minBackoffDelay;
    }

    @Config("thrift.client.min-backoff-delay")
    @ConfigDescription("Minimum delay between request retries")
    public DriftClientConfig setMinBackoffDelay(Duration minBackoffDelay)
    {
        this.minBackoffDelay = minBackoffDelay;
        return this;
    }

    @MinDuration("1s")
    @NotNull
    public Duration getMaxBackoffDelay()
    {
        return maxBackoffDelay;
    }

    @Config("thrift.client.max-backoff-delay")
    @ConfigDescription("Maximum delay between request retries")
    public DriftClientConfig setMaxBackoffDelay(Duration maxBackoffDelay)
    {
        this.maxBackoffDelay = maxBackoffDelay;
        return this;
    }

    public double getBackoffScaleFactor()
    {
        return backoffScaleFactor;
    }

    @Config("thrift.client.backoff-scale-factor")
    @ConfigDescription("Scale factor for request retry delay")
    public DriftClientConfig setBackoffScaleFactor(double backoffScaleFactor)
    {
        this.backoffScaleFactor = backoffScaleFactor;
        return this;
    }

    @MinDuration("1ms")
    @NotNull
    public Duration getMaxRetryTime()
    {
        return maxRetryTime;
    }

    @Config("thrift.client.max-retry-time")
    @ConfigDescription("Total time limit for a request to be retried")
    public DriftClientConfig setMaxRetryTime(Duration maxRetryTime)
    {
        this.maxRetryTime = maxRetryTime;
        return this;
    }

    public boolean isStatsEnabled()
    {
        return statsEnabled;
    }

    @Config("thrift.client.stats.enabled")
    @ConfigDescription("Enable per-method JMX stats")
    public DriftClientConfig setStatsEnabled(boolean statsEnabled)
    {
        this.statsEnabled = statsEnabled;
        return this;
    }
}
