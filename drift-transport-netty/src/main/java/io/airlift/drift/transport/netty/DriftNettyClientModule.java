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

import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.airlift.drift.transport.DriftClientConfig;
import io.airlift.drift.transport.MethodInvokerFactory;

import java.lang.annotation.Annotation;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class DriftNettyClientModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(DriftNettyConnectionFactoryConfig.class);
        configBinder(binder).bindConfigurationBindingListener((binding, configBinder) -> {
            if (binding.getConfigClass().equals(DriftClientConfig.class)) {
                configBinder.bindConfig(DriftNettyClientConfig.class, binding.getKey().getAnnotation(), binding.getPrefix().orElse(null));
            }
        });
    }

    @Provides
    @Singleton
    private static MethodInvokerFactory<Annotation> getMethodInvokerFactory(DriftNettyConnectionFactoryConfig factoryConfig, Injector injector)
    {
        return new DriftNettyMethodInvokerFactory<>(factoryConfig, annotation -> injector.getInstance(Key.get(DriftNettyClientConfig.class, annotation)));
    }
}
