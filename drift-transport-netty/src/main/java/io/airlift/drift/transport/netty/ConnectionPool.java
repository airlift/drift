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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.airlift.drift.transport.TTransportException;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.Future;

import javax.annotation.concurrent.GuardedBy;

import java.io.Closeable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

class ConnectionPool
        implements ConnectionManager, Closeable
{
    private final ConnectionManager connectionFactory;
    private final EventLoopGroup group;

    private final LoadingCache<HostAndPort, Future<Channel>> cachedConnections;
    private final ScheduledExecutorService maintenanceThread;

    @GuardedBy("this")
    private boolean closed;

    public ConnectionPool(ConnectionManager connectionFactory, EventLoopGroup group, DriftNettyClientConfig config)
    {
        this.connectionFactory = connectionFactory;
        this.group = requireNonNull(group, "group is null");
        requireNonNull(config, "config is null");

        // todo from config
        cachedConnections = CacheBuilder.newBuilder()
                .maximumSize(100)
                .expireAfterAccess(10, TimeUnit.MINUTES)
                .<HostAndPort, Future<Channel>>removalListener(notification -> closeConnection(notification.getValue()))
                .build(new CacheLoader<HostAndPort, Future<Channel>>()
                {
                    @Override
                    public Future<Channel> load(HostAndPort address)
                            throws Exception
                    {
                        return createConnection(address);
                    }
                });

        maintenanceThread = newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
                .setNameFormat("drift-connection-maintenance-%s")
                .setDaemon(true)
                .build());

        maintenanceThread.scheduleWithFixedDelay(cachedConnections::cleanUp, 1, 1, TimeUnit.SECONDS);
    }

    @Override
    public Future<Channel> getConnection(HostAndPort address)
    {
        Future<Channel> future;
        synchronized (this) {
            if (closed) {
                return group.next().newFailedFuture(new TTransportException("Connection pool is closed"));
            }

            try {
                future = cachedConnections.get(address);
            }
            catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        // check if connection is failed
        if (isFailed(future)) {
            // remove failed connection
            cachedConnections.asMap().remove(address, future);
        }
        return future;
    }

    @Override
    public void returnConnection(Channel connection)
    {
    }

    @Override
    public synchronized void close()
    {
        if (closed) {
            return;
        }
        closed = true;

        try {
            cachedConnections.invalidateAll();
        }
        finally {
            maintenanceThread.shutdownNow();
        }
    }

    private Future<Channel> createConnection(HostAndPort address)
    {
        return connectionFactory.getConnection(address);
    }

    private static void closeConnection(Future<Channel> future)
    {
        future.addListener(ignored -> {
            if (future.isSuccess()) {
                Channel channel = future.getNow();
                channel.close();
            }
        });
    }

    private static boolean isFailed(Future<?> future)
    {
        if (!future.isDone()) {
            return false;
        }
        try {
            future.get();
            return false;
        }
        catch (Exception e) {
            return true;
        }
    }
}
