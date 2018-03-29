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

import com.google.inject.Injector;
import com.google.inject.Scopes;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.drift.transport.netty.client.DriftNettyClientModule;
import io.airlift.drift.transport.netty.server.DriftNettyServerModule;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.ServerSocket;

import static io.airlift.drift.client.address.SimpleAddressSelectorBinder.simpleAddressSelector;
import static io.airlift.drift.client.guice.DriftClientBinder.driftClientBinder;
import static io.airlift.drift.server.guice.DriftServerBinder.driftServerBinder;
import static org.testng.Assert.assertEquals;

public class TestGuiceIntegration
{
    @Test
    public void test()
            throws Exception
    {
        int port = findUnusedPort();

        Bootstrap bootstrap = new Bootstrap(
                new DriftNettyServerModule(),
                new DriftNettyClientModule(),
                binder -> {
                    binder.bind(PingServiceHandler.class).in(Scopes.SINGLETON);
                    driftServerBinder(binder).bindService(PingServiceHandler.class);
                    driftClientBinder(binder).bindDriftClient(PingService.class)
                            .withAddressSelector(simpleAddressSelector());
                });

        Injector injector = bootstrap
                .strictConfig()
                .setRequiredConfigurationProperty("thrift.server.port", String.valueOf(port))
                .setRequiredConfigurationProperty("ping.thrift.client.addresses", "localhost:" + port)
                .doNotInitializeLogging()
                .initialize();

        LifeCycleManager lifeCycleManager = injector.getInstance(LifeCycleManager.class);
        PingService pingService = injector.getInstance(PingService.class);
        PingServiceHandler pingServiceHandler = injector.getInstance(PingServiceHandler.class);

        try {
            assertEquals(pingServiceHandler.getInvocationCount(), 0);
            pingService.ping();
            assertEquals(pingServiceHandler.getInvocationCount(), 1);
        }
        finally {
            lifeCycleManager.stop();
        }
    }

    private static int findUnusedPort()
            throws IOException
    {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
