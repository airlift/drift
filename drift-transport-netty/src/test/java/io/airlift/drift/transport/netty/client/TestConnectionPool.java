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
package io.airlift.drift.transport.netty.client;

import com.google.common.net.HostAndPort;
import io.airlift.drift.transport.netty.client.ConnectionManager.ConnectionParameters;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.netty.channel.Channel;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ImmediateEventExecutor;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.airlift.drift.transport.netty.codec.Protocol.FB_COMPACT;
import static io.airlift.drift.transport.netty.codec.Transport.HEADER;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

public class TestConnectionPool
{
    private static final ConnectionParameters PARAMETERS = new ConnectionParameters(HEADER, FB_COMPACT, new DataSize(1, MEGABYTE), new Duration(1, MINUTES), new Duration(1, MINUTES), Optional.empty(), Optional.empty());

    @Test
    public void testPooling()
    {
        try (ConnectionPool pool = new ConnectionPool(new TestingConnectionManager(), new DefaultEventLoopGroup(), 10, new Duration(1, MINUTES))) {
            HostAndPort address1 = HostAndPort.fromParts("localhost", 1234);
            HostAndPort address2 = HostAndPort.fromParts("localhost", 4567);

            Channel channel1 = futureGet(pool.getConnection(PARAMETERS, address1));
            Channel channel2 = futureGet(pool.getConnection(PARAMETERS, address1));
            assertSame(channel1, channel2);

            Channel channel3 = futureGet(pool.getConnection(PARAMETERS, address2));
            assertNotSame(channel1, channel3);

            Channel channel4 = futureGet(pool.getConnection(PARAMETERS, address1));
            assertSame(channel1, channel4);
        }
    }

    @Test
    public void testConnectionClosed()
    {
        try (ConnectionPool pool = new ConnectionPool(new TestingConnectionManager(), new DefaultEventLoopGroup(), 10, new Duration(1, MINUTES))) {
            HostAndPort address = HostAndPort.fromParts("localhost", 1234);

            Channel channel1 = futureGet(pool.getConnection(PARAMETERS, address));
            assertTrue(channel1.isOpen());
            channel1.close();
            assertFalse(channel1.isOpen());

            Channel channel2 = futureGet(pool.getConnection(PARAMETERS, address));
            assertTrue(channel2.isOpen());
            assertNotSame(channel1, channel2);
        }
    }

    private static <T> T futureGet(Future<T> future)
    {
        assertTrue(future.isSuccess());
        return future.getNow();
    }

    private static class TestingConnectionManager
            implements ConnectionManager
    {
        @Override
        public Future<Channel> getConnection(ConnectionParameters connectionParameters, HostAndPort address)
        {
            return ImmediateEventExecutor.INSTANCE.newSucceededFuture(new EmbeddedChannel());
        }

        @Override
        public void returnConnection(Channel connection) {}

        @Override
        public void close() {}
    }
}
