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
package io.airlift.drift.server.guice;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.multibindings.Multibinder;
import io.airlift.drift.server.MethodInvocationFilter;

import java.lang.annotation.Annotation;
import java.util.List;

@FunctionalInterface
public interface MethodInvocationFilterBinder
{
    static MethodInvocationFilterBinder staticFilterBinder(MethodInvocationFilter... filters)
    {
        return staticFilterBinder(ImmutableList.copyOf(filters));
    }

    static MethodInvocationFilterBinder staticFilterBinder(List<MethodInvocationFilter> filters)
    {
        return (filterMultibinder, binder, annotation, prefix) -> {
            for (MethodInvocationFilter filter : filters) {
                filterMultibinder.addBinding().toInstance(filter);
            }
        };
    }

    void bind(Multibinder<MethodInvocationFilter> filterMultibinder, Binder binder, Class<? extends Annotation> annotation, String prefix);
}
