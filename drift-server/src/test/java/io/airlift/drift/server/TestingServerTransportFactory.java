/*
 * Copyright (C) 2017 Facebook, Inc.
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

import io.airlift.drift.transport.server.ServerTransport;
import io.airlift.drift.transport.server.ServerTransportFactory;

import static com.google.common.base.Preconditions.checkState;

public class TestingServerTransportFactory
        implements ServerTransportFactory
{
    private TestingServerTransport serverTransport;

    public synchronized TestingServerTransport getServerTransport()
    {
        return serverTransport;
    }

    @Override
    public synchronized ServerTransport createServerTransport(io.airlift.drift.transport.server.ServerMethodInvoker serverMethodInvoker)
    {
        checkState(this.serverTransport == null);
        this.serverTransport = new TestingServerTransport(serverMethodInvoker);
        return serverTransport;
    }
}
