/*
 * Copyright (C) 2018 Facebook, Inc.
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
package io.airlift.drift.integration.guice;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import javax.annotation.concurrent.GuardedBy;

import static java.lang.Math.toIntExact;

public class ThrowingServiceHandler
        implements ThrowingService
{
    @GuardedBy("this")
    private SettableFuture<?> createFuture = SettableFuture.create();

    @GuardedBy("this")
    private SettableFuture<String> awaitFuture;

    @Override
    public void fail(String message, boolean retryable)
            throws ExampleException
    {
        throw new ExampleException(message, retryable);
    }

    @Override
    public byte[] generateTooLargeFrame()
    {
        return new byte[toIntExact(MAX_FRAME_SIZE.toBytes()) + 1];
    }

    @Override
    public String acceptBytes(byte[] bytes)
    {
        return "OK";
    }

    @Override
    public synchronized ListenableFuture<String> await()
    {
        createFuture.set(null);
        if (awaitFuture == null) {
            awaitFuture = SettableFuture.create();
        }
        return awaitFuture;
    }

    @Override
    public synchronized String release()
    {
        createFuture = SettableFuture.create();
        awaitFuture.set("OK");
        awaitFuture = null;
        return "OK";
    }

    public synchronized SettableFuture<?> waitForAwait()
    {
        return createFuture;
    }
}
