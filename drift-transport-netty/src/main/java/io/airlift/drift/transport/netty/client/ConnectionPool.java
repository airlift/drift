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
package io.airlift.drift.transport.netty.client;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.net.HostAndPort;
import io.airlift.drift.protocol.TTransportException;
import io.airlift.units.Duration;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.Future;

import javax.annotation.concurrent.GuardedBy;

import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class ConnectionPool
        implements ConnectionManager
{
    private final ConnectionManager connectionFactory;
    private final EventLoopGroup group;

    private final Cache<ConnectionKey, Future<Channel>> cachedConnections;

    private final ScheduledExecutorService maintenanceThread =
            newSingleThreadScheduledExecutor(daemonThreadsNamed("drift-connection-maintenance"));

    @GuardedBy("this")
    private boolean closed;

    public ConnectionPool(ConnectionManager connectionFactory, EventLoopGroup group, int maxSize, Duration idleTimeout)
    {
        this.connectionFactory = requireNonNull(connectionFactory, "connectionFactory is null");
        this.group = requireNonNull(group, "group is null");

        cachedConnections = CacheBuilder.newBuilder()
                .maximumSize(maxSize)
                .expireAfterAccess(idleTimeout.toMillis(), MILLISECONDS)
                .<ConnectionKey, Future<Channel>>removalListener(notification -> closeConnection(notification.getValue()))
                .build();

        maintenanceThread.scheduleWithFixedDelay(cachedConnections::cleanUp, 1, 1, TimeUnit.SECONDS);
    }

    @Override
    public Future<Channel> getConnection(ConnectionParameters connectionParameters, HostAndPort address)
    {
        ConnectionKey key = new ConnectionKey(connectionParameters, address);

        Future<Channel> future;
        synchronized (this) {
            if (closed) {
                return group.next().newFailedFuture(new TTransportException("Connection pool is closed"));
            }

            try {
                future = cachedConnections.get(key, () -> createConnection(key));
            }
            catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        // check if connection is failed
        if (isFailed(future)) {
            // remove failed connection
            cachedConnections.asMap().remove(key, future);
        }
        return future;
    }

    private Future<Channel> createConnection(ConnectionKey key)
    {
        Future<Channel> future = connectionFactory.getConnection(key.getConnectionParameters(), key.getAddress());

        // remove connection from cache when it is closed
        future.addListener(channelFuture -> {
            if (future.isSuccess()) {
                future.getNow().closeFuture().addListener(closeFuture -> cachedConnections.asMap().remove(key, future));
            }
        });

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

    private static class ConnectionKey
    {
        private final ConnectionParameters connectionParameters;
        private final HostAndPort address;

        public ConnectionKey(ConnectionParameters connectionParameters, HostAndPort address)
        {
            this.connectionParameters = connectionParameters;
            this.address = address;
        }

        public ConnectionParameters getConnectionParameters()
        {
            return connectionParameters;
        }

        public HostAndPort getAddress()
        {
            return address;
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
            ConnectionKey that = (ConnectionKey) o;
            return Objects.equals(connectionParameters, that.connectionParameters) &&
                    Objects.equals(address, that.address);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(connectionParameters, address);
        }
    }
}
