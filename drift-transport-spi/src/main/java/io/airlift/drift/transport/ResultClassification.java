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
package io.airlift.drift.transport;

import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class ResultClassification
{
    private final Optional<Boolean> retry;
    private final boolean hostDown;

    public ResultClassification(Optional<Boolean> retry, boolean hostDown)
    {
        this.retry = requireNonNull(retry, "retry is null");
        this.hostDown = hostDown;
    }

    public Optional<Boolean> isRetry()
    {
        return retry;
    }

    public boolean isHostDown()
    {
        return hostDown;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("retry", retry)
                .add("hostDown", hostDown)
                .toString();
    }
}
