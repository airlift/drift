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
package io.airlift.drift.client;

import io.airlift.units.Duration;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class RetriesFailedException
        extends Exception
{
    private final int invocationAttempts;
    private final int connectionAttempts;
    private final Duration retryTime;
    private final int overloadedRejects;

    public RetriesFailedException(int invocationAttempts, Duration retryTime, int connectionAttempts, int overloadedRejects)
    {
        super(format(
                "Invocation retries failed (invocationAttempts: %s, duration: %s, connectionAttempts: %s, overloadedRejects: %s)",
                invocationAttempts,
                retryTime,
                connectionAttempts,
                overloadedRejects));
        this.invocationAttempts = invocationAttempts;
        this.connectionAttempts = connectionAttempts;
        this.retryTime = requireNonNull(retryTime, "retryTime is null");
        this.overloadedRejects = overloadedRejects;
    }

    public int getInvocationAttempts()
    {
        return invocationAttempts;
    }

    public int getConnectionAttempts()
    {
        return connectionAttempts;
    }

    public Duration getRetryTime()
    {
        return retryTime;
    }

    public int getOverloadedRejects()
    {
        return overloadedRejects;
    }
}
