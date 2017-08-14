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
package io.airlift.drift.server;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class DriftService
{
    private final Object service;
    private final Optional<String> qualifier;
    private final boolean statsEnabled;

    public DriftService(Object service)
    {
        this(service, Optional.empty(), false);
    }

    public DriftService(Object service, Optional<String> qualifier, boolean statsEnabled)
    {
        this.service = requireNonNull(service, "service is null");
        this.qualifier = requireNonNull(qualifier, "qualifier is null");
        this.statsEnabled = statsEnabled;
    }

    public Object getService()
    {
        return service;
    }

    public Optional<String> getQualifier()
    {
        return qualifier;
    }

    public boolean isStatsEnabled()
    {
        return statsEnabled;
    }
}
