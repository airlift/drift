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
package io.airlift.drift.client.guice;

import com.google.common.collect.ImmutableList;
import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import io.airlift.configuration.ConfigDefaults;
import io.airlift.drift.client.DriftClient;
import io.airlift.drift.client.DriftClientFactory;
import io.airlift.drift.client.DriftClientFactoryManager;
import io.airlift.drift.client.ExceptionClassifier;
import io.airlift.drift.client.MethodInvocationFilter;
import io.airlift.drift.client.address.AddressSelector;
import io.airlift.drift.client.stats.JmxMethodInvocationStatsFactory;
import io.airlift.drift.client.stats.MethodInvocationStatsFactory;
import io.airlift.drift.client.stats.NullMethodInvocationStatsFactory;
import io.airlift.drift.codec.ThriftCodecManager;
import io.airlift.drift.codec.guice.ThriftCodecModule;
import io.airlift.drift.transport.client.DriftClientConfig;
import io.airlift.drift.transport.client.MethodInvokerFactory;
import org.weakref.jmx.MBeanExporter;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.drift.client.ExceptionClassifier.mergeExceptionClassifiers;
import static io.airlift.drift.client.guice.DriftClientAnnotationFactory.extractDriftClientBindingAnnotation;
import static io.airlift.drift.client.guice.DriftClientAnnotationFactory.getDriftClientAnnotation;
import static io.airlift.drift.codec.metadata.ThriftServiceMetadata.getThriftServiceAnnotation;
import static java.util.Objects.requireNonNull;

public class DriftClientBinder
{
    public static DriftClientBinder driftClientBinder(Binder binder)
    {
        return new DriftClientBinder(binder);
    }

    private final Binder binder;

    private DriftClientBinder(Binder binder)
    {
        this.binder = requireNonNull(binder, "binder is null").skipSources(this.getClass());
        binder.install(new ThriftCodecModule());
        binder.install(new DriftClientBinderModule());
    }

    public <T> DriftClientBindingBuilder bindDriftClient(Class<T> clientInterface)
    {
        String configPrefix = getServiceName(clientInterface);
        return bindDriftClient(clientInterface, configPrefix, DefaultClient.class);
    }

    public <T> DriftClientBindingBuilder bindDriftClient(Class<T> clientInterface, Class<? extends Annotation> annotationType)
    {
        String configPrefix = getServiceName(clientInterface);
        if (annotationType != DefaultClient.class) {
            configPrefix += "." + annotationType.getSimpleName();
        }
        return bindDriftClient(clientInterface, configPrefix, annotationType);
    }

    private <T> DriftClientBindingBuilder bindDriftClient(Class<T> clientInterface, String configPrefix, Class<? extends Annotation> annotation)
    {
        Annotation clientAnnotation = getDriftClientAnnotation(clientInterface, annotation);

        configBinder(binder).bindConfig(DriftClientConfig.class, clientAnnotation, configPrefix);

        TypeLiteral<DriftClient<T>> typeLiteral = driftClientTypeLiteral(clientInterface);

        Provider<T> instanceProvider = new DriftClientInstanceProvider<>(clientAnnotation, Key.get(typeLiteral, annotation));
        Provider<DriftClient<T>> factoryProvider = new DriftClientProvider<>(clientInterface, clientAnnotation);

        binder.bind(Key.get(clientInterface, annotation)).toProvider(instanceProvider).in(Scopes.SINGLETON);
        binder.bind(Key.get(typeLiteral, annotation)).toProvider(factoryProvider).in(Scopes.SINGLETON);

        if (annotation == DefaultClient.class) {
            binder.bind(Key.get(clientInterface)).toProvider(instanceProvider).in(Scopes.SINGLETON);
            binder.bind(Key.get(typeLiteral)).toProvider(factoryProvider).in(Scopes.SINGLETON);
        }

        return new DriftClientBindingBuilder(binder, clientAnnotation, configPrefix);
    }

    public <T> void bindClientConfigDefaults(Class<T> clientInterface, ConfigDefaults<DriftClientConfig> configDefaults)
    {
        bindClientConfigDefaults(clientInterface, DefaultClient.class, configDefaults);
    }

    public <T> void bindClientConfigDefaults(Class<T> clientInterface, Class<? extends Annotation> annotationType, ConfigDefaults<DriftClientConfig> configDefaults)
    {
        bindConfigDefaults(clientInterface, annotationType, DriftClientConfig.class, configDefaults);
    }

    public <T, C> void bindConfigDefaults(Class<T> clientInterface, Class<C> configClass, ConfigDefaults<C> configDefaults)
    {
        bindConfigDefaults(configClass, DefaultClient.class, configClass, configDefaults);
    }

    public <T, C> void bindConfigDefaults(Class<T> clientInterface, Class<? extends Annotation> annotationType, Class<C> configClass, ConfigDefaults<C> configDefaults)
    {
        configBinder(binder).bindConfigDefaults(configClass, getDriftClientAnnotation(clientInterface, annotationType), configDefaults);
    }

    private static String getServiceName(Class<?> clientInterface)
    {
        requireNonNull(clientInterface, "clientInterface is null");
        String serviceName = getThriftServiceAnnotation(clientInterface).value();
        if (!serviceName.isEmpty()) {
            return serviceName;
        }
        return clientInterface.getSimpleName();
    }

    @SuppressWarnings("unchecked")
    private static <T> TypeLiteral<DriftClient<T>> driftClientTypeLiteral(Class<T> clientInterface)
    {
        Type javaType = new TypeToken<DriftClient<T>>() {}
                .where(new TypeParameter<T>() {}, TypeToken.of(clientInterface))
                .getType();
        return (TypeLiteral<DriftClient<T>>) TypeLiteral.get(javaType);
    }

    private static class DriftClientInstanceProvider<T>
            extends AbstractAnnotatedProvider<T>
    {
        private final Key<DriftClient<T>> key;

        public DriftClientInstanceProvider(Annotation annotation, Key<DriftClient<T>> key)
        {
            super(annotation);
            this.key = requireNonNull(key, "key is null");
        }

        @Override
        protected T get(Injector injector, Annotation annotation)
        {
            return injector.getInstance(key).get();
        }
    }

    private static class DriftClientProvider<T>
            extends AbstractAnnotatedProvider<DriftClient<T>>
    {
        private static final TypeLiteral<DriftClientFactoryManager<Annotation>> DRIFT_CLIENT_FACTORY_MANAGER_TYPE = new TypeLiteral<DriftClientFactoryManager<Annotation>>() {};
        private static final TypeLiteral<Set<MethodInvocationFilter>> SET_METHOD_INVOCATION_FILTERS_TYPE = new TypeLiteral<Set<MethodInvocationFilter>>() {};
        private static final TypeLiteral<Set<ExceptionClassifier>> SET_EXCEPTION_CLASSIFIER_TYPE = new TypeLiteral<Set<ExceptionClassifier>>() {};

        private final Class<T> clientInterface;

        public DriftClientProvider(Class<T> clientInterface, Annotation annotation)
        {
            super(annotation);
            this.clientInterface = requireNonNull(clientInterface, "clientInterface is null");
        }

        @Override
        protected DriftClient<T> get(Injector injector, Annotation clientAnnotation)
        {
            DriftClientConfig config = injector.getInstance(Key.get(DriftClientConfig.class, clientAnnotation));
            DriftClientFactoryManager<Annotation> driftClientFactoryManager = injector.getInstance(Key.get(DRIFT_CLIENT_FACTORY_MANAGER_TYPE));

            AddressSelector<?> addressSelector = injector.getInstance(Key.get(AddressSelector.class, clientAnnotation));

            ExceptionClassifier exceptionClassifier = mergeExceptionClassifiers(ImmutableList.<ExceptionClassifier>builder()
                    .addAll(injector.getInstance(Key.get(SET_EXCEPTION_CLASSIFIER_TYPE, clientAnnotation))) // per-client
                    .addAll(injector.getInstance(Key.get(SET_EXCEPTION_CLASSIFIER_TYPE))) // global
                    .build());

            List<MethodInvocationFilter> filters = ImmutableList.copyOf(injector.getInstance(Key.get(SET_METHOD_INVOCATION_FILTERS_TYPE, clientAnnotation)));

            DriftClientFactory driftClientFactory = driftClientFactoryManager.createDriftClientFactory(clientAnnotation, addressSelector, exceptionClassifier);
            return driftClientFactory.createDriftClient(clientInterface, extractDriftClientBindingAnnotation(clientAnnotation), filters, config);
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

    private static class DriftClientBinderModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            newSetBinder(binder, ExceptionClassifier.class);
            newOptionalBinder(binder, MBeanExporter.class);
            newOptionalBinder(binder, MethodInvocationStatsFactory.class)
                    .setDefault()
                    .toProvider(DefaultMethodInvocationStatsFactoryProvider.class)
                    .in(Scopes.SINGLETON);
        }

        @Provides
        @Singleton
        private static DriftClientFactoryManager<Annotation> getDriftClientFactory(
                ThriftCodecManager codecManager,
                MethodInvokerFactory<Annotation> methodInvokerFactory,
                MethodInvocationStatsFactory methodInvocationStatsFactory)
        {
            return new DriftClientFactoryManager<>(codecManager, methodInvokerFactory, methodInvocationStatsFactory);
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
}
