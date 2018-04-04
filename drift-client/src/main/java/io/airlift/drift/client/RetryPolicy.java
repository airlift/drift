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
package io.airlift.drift.client;

import io.airlift.drift.transport.client.ConnectionFailedException;
import io.airlift.drift.transport.client.DriftClientConfig;
import io.airlift.drift.transport.client.RequestTimeoutException;
import io.airlift.units.Duration;

import java.io.InterruptedIOException;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.drift.client.ExceptionClassification.HostStatus.DOWN;
import static io.airlift.drift.client.ExceptionClassification.HostStatus.NORMAL;
import static io.airlift.drift.client.ExceptionClassifier.NORMAL_RESULT;
import static java.lang.Boolean.TRUE;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class RetryPolicy
{
    public static final RetryPolicy NO_RETRY_POLICY = new RetryPolicy(
            0,
            new Duration(0, MILLISECONDS),
            new Duration(0, MILLISECONDS),
            1.0,
            new Duration(0, MILLISECONDS),
            NORMAL_RESULT);

    private final int maxRetries;
    private final Duration minBackoffDelay;
    private final Duration maxBackoffDelay;
    private final double backoffScaleFactor;
    private final Duration maxRetryTime;
    private final ExceptionClassifier exceptionClassifier;

    public RetryPolicy(DriftClientConfig config, ExceptionClassifier exceptionClassifier)
    {
        this(
                config.getMaxRetries(),
                config.getMinBackoffDelay(),
                config.getMaxBackoffDelay(),
                config.getBackoffScaleFactor(),
                config.getMaxRetryTime(),
                exceptionClassifier);
    }

    public RetryPolicy(int maxRetries,
            Duration minBackoffDelay,
            Duration maxBackoffDelay,
            double backoffScaleFactor,
            Duration maxRetryTime,
            ExceptionClassifier exceptionClassifier)
    {
        checkArgument(maxRetries >= 0, "maxRetries must be positive");
        this.maxRetries = maxRetries;
        this.minBackoffDelay = requireNonNull(minBackoffDelay, "minBackoffDelay is null");
        this.maxBackoffDelay = requireNonNull(maxBackoffDelay, "maxBackoffDelay is null");
        checkArgument(backoffScaleFactor >= 1.0, "backoffScaleFactor must be at least 1");
        this.backoffScaleFactor = backoffScaleFactor;
        this.maxRetryTime = requireNonNull(maxRetryTime, "maxRetryTime is null");
        this.exceptionClassifier = requireNonNull(exceptionClassifier, "exceptionClassifier is null");
    }

    public int getMaxRetries()
    {
        return maxRetries;
    }

    public Duration getBackoffDelay(int invocationAttempts)
    {
        long delayInMs = (long) (minBackoffDelay.toMillis() * Math.pow(backoffScaleFactor, invocationAttempts - 1));
        return new Duration(min(delayInMs, maxBackoffDelay.toMillis()), MILLISECONDS);
    }

    public Duration getMaxRetryTime()
    {
        return maxRetryTime;
    }

    public ExceptionClassification classifyException(Throwable throwable, boolean idempotent)
    {
        if (throwable instanceof ConnectionFailedException) {
            return new ExceptionClassification(Optional.of(TRUE), DOWN);
        }

        if (idempotent && throwable instanceof RequestTimeoutException) {
            // We don't know if the server is overloaded, or if this specific
            // request just takes to long, so just mark the server as normal.
            return new ExceptionClassification(Optional.of(TRUE), NORMAL);
        }

        // interrupted exceptions are always an immediate failure
        if (throwable instanceof InterruptedException || throwable instanceof InterruptedIOException) {
            return new ExceptionClassification(Optional.of(false), NORMAL);
        }

        return exceptionClassifier.classifyException(throwable);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("maxAttempts", maxRetries)
                .add("minSleepTime", minBackoffDelay)
                .add("maxSleepTime", maxBackoffDelay)
                .add("scaleFactor", backoffScaleFactor)
                .add("maxRetryTime", maxRetryTime)
                .add("exceptionClassifier", exceptionClassifier)
                .toString();
    }
}
