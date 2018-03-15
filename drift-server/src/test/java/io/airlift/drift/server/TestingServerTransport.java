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

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.drift.transport.MethodMetadata;
import io.airlift.drift.transport.server.ServerInvokeRequest;
import io.airlift.drift.transport.server.ServerTransport;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class TestingServerTransport
        implements ServerTransport
{
    public enum State
    {
        NOT_STARTED, RUNNING, SHUTDOWN
    }

    private final io.airlift.drift.transport.server.ServerMethodInvoker serverMethodInvoker;
    private State state = State.NOT_STARTED;

    public TestingServerTransport(io.airlift.drift.transport.server.ServerMethodInvoker serverMethodInvoker)
    {
        this.serverMethodInvoker = serverMethodInvoker;
    }

    public synchronized State getState()
    {
        return state;
    }

    @Override
    public synchronized void start()
    {
        checkState(state == State.NOT_STARTED);
        state = State.RUNNING;
    }

    public synchronized ListenableFuture<Object> invoke(String methodName, Map<String, String> headers, Map<Short, Object> parameters)
    {
        long startTime = System.nanoTime();
        Optional<MethodMetadata> methodMetadata = serverMethodInvoker.getMethodMetadata(methodName);
        checkArgument(methodMetadata.isPresent(), "Method %s not found", methodName);

        ListenableFuture<Object> result = serverMethodInvoker.invoke(new ServerInvokeRequest(methodMetadata.get(), headers, parameters));

        serverMethodInvoker.recordResult(methodName, startTime, result);
        return result;
    }

    @Override
    public synchronized void shutdown()
    {
        checkState(state == State.RUNNING);
        state = State.SHUTDOWN;
    }
}
