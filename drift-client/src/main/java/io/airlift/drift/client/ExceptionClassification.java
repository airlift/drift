/*
 * Copyright (C) 2013 Facebook, Inc.
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
package io.airlift.drift.client;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class ExceptionClassification
{
    public enum HostStatus
    {
        NORMAL, OVERLOADED, DOWN
    }

    private final Optional<Boolean> retry;
    private final HostStatus hostStatus;

    public ExceptionClassification(Optional<Boolean> retry, HostStatus hostStatus)
    {
        this.retry = requireNonNull(retry, "retry is null");
        this.hostStatus = requireNonNull(hostStatus, "hostStatus is null");
    }

    public Optional<Boolean> isRetry()
    {
        return retry;
    }

    public HostStatus getHostStatus()
    {
        return hostStatus;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ExceptionClassification that = (ExceptionClassification) o;
        return Objects.equals(retry, that.retry) &&
                hostStatus == that.hostStatus;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(retry, hostStatus);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("retry", retry)
                .add("hostStatus", hostStatus)
                .toString();
    }
}
