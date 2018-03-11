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
package io.airlift.drift.transport.apache.client;

import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.airlift.configuration.ConfigBinder;
import io.airlift.configuration.ConfigurationBinding;
import io.airlift.drift.transport.client.DriftClientConfig;
import io.airlift.drift.transport.client.MethodInvokerFactory;

import java.lang.annotation.Annotation;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class ApacheThriftClientModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(ApacheThriftConnectionFactoryConfig.class);
        configBinder(binder).bindConfigurationBindingListener(ApacheThriftClientModule::bindApacheThriftClientConfig);
    }

    private static void bindApacheThriftClientConfig(ConfigurationBinding<?> binding, ConfigBinder configBinder)
    {
        if (binding.getConfigClass().equals(DriftClientConfig.class)) {
            configBinder.bindConfig(ApacheThriftClientConfig.class, binding.getKey().getAnnotation(), binding.getPrefix().orElse(null));
        }
    }

    @Provides
    @Singleton
    private static MethodInvokerFactory<Annotation> getMethodInvokerFactory(ApacheThriftConnectionFactoryConfig factoryConfig, Injector injector)
    {
        return new ApacheThriftMethodInvokerFactory<>(factoryConfig, annotation -> injector.getInstance(Key.get(ApacheThriftClientConfig.class, annotation)));
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        return o != null && getClass() == o.getClass();
    }

    @Override
    public int hashCode()
    {
        return getClass().hashCode();
    }
}
