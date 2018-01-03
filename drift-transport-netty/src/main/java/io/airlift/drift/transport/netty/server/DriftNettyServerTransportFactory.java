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

import io.airlift.drift.transport.server.ServerMethodInvoker;
import io.airlift.drift.transport.server.ServerTransport;
import io.airlift.drift.transport.server.ServerTransportFactory;

import static java.util.Objects.requireNonNull;

public class DriftNettyServerTransportFactory
        implements ServerTransportFactory
{
    private final DriftNettyServerConfig config;

    public DriftNettyServerTransportFactory(DriftNettyServerConfig config)
    {
        this.config = requireNonNull(config, "config is null");
    }

    @Override
    public ServerTransport createServerTransport(ServerMethodInvoker methodInvoker)
    {
        return new DriftNettyServerTransport(methodInvoker, config);
    }
}
