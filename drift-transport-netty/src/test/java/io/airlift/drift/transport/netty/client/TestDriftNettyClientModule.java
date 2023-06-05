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

import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.drift.transport.client.DriftClientConfig;
import io.airlift.drift.transport.client.MethodInvokerFactory;
import org.testng.annotations.Test;

import java.lang.annotation.Annotation;

import static com.google.inject.name.Names.named;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static org.testng.Assert.assertNotNull;

public class TestDriftNettyClientModule
{
    @Test
    public void test()
            throws Exception
    {
        Annotation clientAnnotation = named("test");
        Bootstrap bootstrap = new Bootstrap(
                new DriftNettyClientModule(),
                binder -> configBinder(binder).bindConfig(DriftClientConfig.class, clientAnnotation, "prefix"));

        Injector injector = bootstrap
                .doNotInitializeLogging()
                .initialize();

        assertNotNull(injector.getInstance(Key.get(new TypeLiteral<MethodInvokerFactory<Annotation>>() {})));
        assertNotNull(injector.getInstance(Key.get(DriftClientConfig.class, clientAnnotation)));
        assertNotNull(injector.getInstance(Key.get(DriftNettyClientConfig.class, clientAnnotation)));
    }
}
