/*
 * Copyright (C) 2012 Facebook, Inc.
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
package io.airlift.drift.server.guice;

import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import io.airlift.drift.server.DriftServer;
import io.airlift.drift.server.DriftService;
import io.airlift.drift.server.MethodInvocationFilter;
import io.airlift.drift.server.stats.JmxMethodInvocationStatsFactory;
import io.airlift.drift.server.stats.MethodInvocationStatsFactory;
import io.airlift.drift.server.stats.NullMethodInvocationStatsFactory;
import io.airlift.drift.transport.server.DriftServerConfig;
import org.weakref.jmx.MBeanExporter;

import javax.inject.Inject;
import javax.inject.Provider;

import java.lang.annotation.Annotation;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.drift.codec.metadata.ThriftServiceMetadata.getThriftServiceAnnotation;
import static java.util.Objects.requireNonNull;

public class DriftServerBinder
{
    public static DriftServerBinder driftServerBinder(Binder binder)
    {
        return new DriftServerBinder(binder);
    }

    private final Binder binder;

    private DriftServerBinder(Binder binder)
    {
        this.binder = requireNonNull(binder, "binder is null").skipSources(getClass());
        binder.install(new DriftServerBinderModule());
    }

    public <T> void bindService(Class<T> serverInterface)
    {
        bindService(serverInterface, getServiceName(serverInterface), DefaultService.class);
    }

    public <T> void bindService(Class<T> serverInterface, Class<? extends Annotation> annotationType)
    {
        String configPrefix = getServiceName(serverInterface) + "." + annotationType.getSimpleName();
        bindService(serverInterface, configPrefix, annotationType);
    }

    private <T> void bindService(Class<T> serverInterface, String configPrefix, Class<? extends Annotation> annotation)
    {
        configBinder(binder).bindConfig(DriftServerConfig.class, annotation, configPrefix);

        newSetBinder(binder, DriftService.class).addBinding()
                .toProvider(new DriftServiceProvider<>(serverInterface, annotation));

        if (annotation == DefaultService.class) {
            binder.bind(serverInterface).annotatedWith(DefaultService.class).to(serverInterface);
        }
    }

    public void bindFilter(MethodInvocationFilter filter)
    {
        newSetBinder(binder, MethodInvocationFilter.class).addBinding()
                .toInstance(filter);
    }

    public <T extends MethodInvocationFilter> void bindFilter(Class<T> filterClass)
    {
        newSetBinder(binder, MethodInvocationFilter.class).addBinding()
                .to(filterClass);
    }

    public <T extends MethodInvocationFilter> void bindFilter(Key<T> filterKey)
    {
        newSetBinder(binder, MethodInvocationFilter.class).addBinding()
                .to(filterKey);
    }

    private static String getServiceName(Class<?> serverInterface)
    {
        requireNonNull(serverInterface, "serverInterface is null");
        String serviceName = getThriftServiceAnnotation(serverInterface).value();
        if (!serviceName.isEmpty()) {
            return serviceName;
        }
        return serverInterface.getSimpleName();
    }

    private static class DriftServiceProvider<T>
            implements Provider<DriftService>
    {
        private final Class<T> serverInterface;
        private final Class<? extends Annotation> annotation;
        private Injector injector;

        public DriftServiceProvider(Class<T> serverInterface, Class<? extends Annotation> annotation)
        {
            this.serverInterface = requireNonNull(serverInterface, "serverInterface is null");
            this.annotation = requireNonNull(annotation, "annotation is null");
        }

        @Inject
        public void setInjector(Injector injector)
        {
            this.injector = injector;
        }

        @Override
        public final DriftService get()
        {
            checkState(injector != null, "injector was not set");
            T service = injector.getInstance(Key.get(serverInterface, annotation));
            DriftServerConfig config = injector.getInstance(Key.get(DriftServerConfig.class, annotation));
            Optional<String> qualifier;
            if (annotation == DefaultService.class) {
                qualifier = Optional.empty();
            }
            else {
                qualifier = Optional.of(annotation.getSimpleName());
            }
            return new DriftService(service, qualifier, config.isStatsEnabled());
        }
    }

    private static class DefaultMethodInvocationStatsFactoryProvider
            implements Provider<MethodInvocationStatsFactory>
    {
        private final Optional<MBeanExporter> mbeanExporter;

        @Inject
        public DefaultMethodInvocationStatsFactoryProvider(Optional<MBeanExporter> mbeanExporter)
        {
            this.mbeanExporter = mbeanExporter;
        }

        @Override
        public MethodInvocationStatsFactory get()
        {
            return mbeanExporter
                    .map(JmxMethodInvocationStatsFactory::new)
                    .map(MethodInvocationStatsFactory.class::cast)
                    .orElseGet(NullMethodInvocationStatsFactory::new);
        }
    }

    private static class DriftServerBinderModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            binder.bind(DriftServer.class).in(SINGLETON);
            newSetBinder(binder, DriftService.class);
            newSetBinder(binder, MethodInvocationFilter.class);

            newOptionalBinder(binder, MBeanExporter.class);
            newOptionalBinder(binder, MethodInvocationStatsFactory.class)
                    .setDefault()
                    .toProvider(DefaultMethodInvocationStatsFactoryProvider.class)
                    .in(SINGLETON);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            return !(o == null || getClass() != o.getClass());
        }

        @Override
        public int hashCode()
        {
            return 42;
        }
    }
}
