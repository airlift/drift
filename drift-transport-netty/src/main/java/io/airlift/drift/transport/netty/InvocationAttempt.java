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
package io.airlift.drift.transport.netty;

import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.drift.TException;
import io.airlift.drift.transport.ConnectionFailedException;
import io.netty.channel.Channel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import javax.annotation.Nullable;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

class InvocationAttempt
{
    private final HostAndPort address;
    private final ConnectionManager connectionManager;
    private final InvocationFunction<Channel> invocationFunction;

    private final InvocationResponseFuture future = new InvocationResponseFuture();

    private final AtomicBoolean started = new AtomicBoolean();

    // current outstanding task for debugging
    private final AtomicReference<java.util.concurrent.Future<?>> currentTask = new AtomicReference<>();

    InvocationAttempt(
            HostAndPort address,
            ConnectionManager connectionManager,
            InvocationFunction<Channel> invocationFunction)
    {
        this.address = address;
        this.connectionManager = connectionManager;
        this.invocationFunction = invocationFunction;
    }

    ListenableFuture<Object> getFuture()
    {
        if (started.compareAndSet(false, true)) {
            try {
                tryConnect();
            }
            catch (Throwable throwable) {
                future.fatalError(throwable);
            }
        }

        return future;
    }

    private void tryConnect()
    {
        // request was already canceled
        if (future.isCancelled()) {
            return;
        }

        Future<Channel> channelFuture = connectionManager.getConnection(address);
        currentTask.set(channelFuture);
        channelFuture.addListener(new SafeFutureCallback<Channel>()
        {
            @Override
            public void safeOnSuccess(Channel channel)
            {
                tryInvocation(channel);
            }

            @Override
            public void safeOnFailure(Throwable t)
            {
                future.fatalError(new ConnectionFailedException(address, t));
            }
        });
    }

    private void tryInvocation(Channel channel)
    {
        try {
            ListenableFuture<Object> invocationFuture = invocationFunction.invokeOn(channel);
            currentTask.set(invocationFuture);
            Futures.addCallback(invocationFuture, new SafeFutureCallback<Object>()
            {
                @Override
                public void safeOnSuccess(Object result)
                {
                    connectionManager.returnConnection(channel);
                    future.success(result);
                }

                @Override
                public void safeOnFailure(Throwable t)
                {
                    connectionManager.returnConnection(channel);
                    future.fatalError(t);
                }
            });
        }
        catch (Throwable e) {
            connectionManager.returnConnection(channel);
            throw e;
        }
    }

    // This is an non-static inner class so it retains a reference to
    // the invocation attempt which make debugging easier
    private static class InvocationResponseFuture
            extends AbstractFuture<Object>
    {
        void success(Object value)
        {
            set(value);
        }

        void fatalError(Throwable throwable)
        {
            if (throwable instanceof InterruptedException) {
                Thread.currentThread().interrupt();
                throwable = new TException(throwable);
            }

            // exception in the future is expected to be a TException
            if (!(throwable instanceof TException)) {
                throwable = new TException(throwable);
            }
            setException(throwable);
        }
    }

    // The invocation attempt works by using repeated callbacks without any active monitoring, so
    // it is critical that callbacks do not throw.  This is a safety system to turn programming
    // errors into a failed future so, the request doesn't hang.
    private abstract class SafeFutureCallback<T>
            implements FutureCallback<T>, GenericFutureListener<Future<T>>
    {
        abstract void safeOnSuccess(@Nullable T result)
                throws Exception;

        abstract void safeOnFailure(Throwable throwable)
                throws Exception;

        @Override
        public void operationComplete(Future<T> future)
                throws Exception
        {
            if (!future.isSuccess()) {
                onFailure(future.cause());
            }
            else {
                onSuccess(future.getNow());
            }
        }

        @Override
        public final void onSuccess(@Nullable T result)
        {
            try {
                safeOnSuccess(result);
            }
            catch (Throwable t) {
                future.fatalError(t);
            }
        }

        @Override
        public final void onFailure(Throwable throwable)
        {
            try {
                safeOnFailure(throwable);
            }
            catch (Throwable t) {
                future.fatalError(t);
            }
        }
    }
}
