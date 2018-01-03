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

import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import io.airlift.drift.transport.client.DriftClientConfig;
import io.airlift.drift.transport.client.MethodInvokerFactory;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Provider;

import java.lang.annotation.Annotation;

import static com.google.common.base.Preconditions.checkState;
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

        binder.bind(new TypeLiteral<MethodInvokerFactory<Annotation>>() {})
                .toProvider(MethodInvokerFactoryProvider.class)
                .in(Scopes.SINGLETON);
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

    private static class MethodInvokerFactoryProvider
            implements Provider<MethodInvokerFactory<Annotation>>
    {
        private Injector injector;
        private DriftNettyMethodInvokerFactory<Annotation> factory;

        @Inject
        public void setInjector(Injector injector)
        {
            this.injector = injector;
        }

        @Override
        public MethodInvokerFactory<Annotation> get()
        {
            checkState(factory == null, "factory already created");

            factory = new DriftNettyMethodInvokerFactory<>(
                    injector.getInstance(DriftNettyConnectionFactoryConfig.class),
                    annotation -> injector.getInstance(Key.get(DriftNettyClientConfig.class, annotation)));

            return factory;
        }

        @PreDestroy
        public void destroy()
        {
            factory.close();
        }
    }
}
