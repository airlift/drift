/*
 * Copyright (C) 2012 Facebook, Inc.
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
package io.airlift.drift.client;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.collect.TreeMultimap;
import io.airlift.drift.annotations.ThriftMethod;
import io.airlift.drift.annotations.ThriftService;
import io.airlift.drift.codec.metadata.ThriftCatalog;

import javax.annotation.concurrent.Immutable;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static io.airlift.drift.codec.metadata.ReflectionHelper.findAnnotatedMethods;
import static io.airlift.drift.codec.metadata.ReflectionHelper.getEffectiveClassAnnotations;
import static java.util.Objects.requireNonNull;

@Immutable
public class ThriftServiceMetadata
{
    private final String name;
    private final Map<String, ThriftMethodMetadata> methods;

    ThriftServiceMetadata(Class<?> serviceClass, ThriftCatalog catalog)
    {
        requireNonNull(serviceClass, "serviceClass is null");
        ThriftService thriftService = getThriftServiceAnnotation(serviceClass);

        if (thriftService.value().isEmpty()) {
            name = serviceClass.getSimpleName();
        }
        else {
            name = thriftService.value();
        }

        ImmutableMap.Builder<String, ThriftMethodMetadata> builder = ImmutableMap.builder();

        // A multimap from order to method name. Sorted by key (order), with nulls (i.e. no order) last.
        // Within each key, values (ThriftMethodMetadata) are sorted by method name.
        TreeMultimap<Integer, ThriftMethodMetadata> declaredMethods = TreeMultimap.create(
                Ordering.natural().nullsLast(),
                Ordering.natural().onResultOf(ThriftMethodMetadata::getName));
        for (Method method : findAnnotatedMethods(serviceClass, ThriftMethod.class)) {
            if (method.isAnnotationPresent(ThriftMethod.class)) {
                ThriftMethodMetadata methodMetadata = new ThriftMethodMetadata(method, catalog);
                builder.put(methodMetadata.getName(), methodMetadata);
                if (method.getDeclaringClass().equals(serviceClass)) {
                    declaredMethods.put(ThriftCatalog.getMethodOrder(method), methodMetadata);
                }
            }
        }
        methods = builder.build();
    }

    public String getName()
    {
        return name;
    }

    public Map<String, ThriftMethodMetadata> getMethods()
    {
        return methods;
    }

    public static ThriftService getThriftServiceAnnotation(Class<?> serviceClass)
    {
        Set<ThriftService> serviceAnnotations = getEffectiveClassAnnotations(serviceClass, ThriftService.class);
        Preconditions.checkArgument(!serviceAnnotations.isEmpty(), "Service class %s is not annotated with @ThriftService", serviceClass.getName());
        Preconditions.checkArgument(
                serviceAnnotations.size() == 1,
                "Service class %s has multiple conflicting @ThriftService annotations: %s",
                serviceClass.getName(),
                serviceAnnotations);

        return Iterables.getOnlyElement(serviceAnnotations);
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
        ThriftServiceMetadata that = (ThriftServiceMetadata) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(methods, that.methods);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, methods);
    }
}
