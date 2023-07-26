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
package io.airlift.drift.codec.metadata;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;

import java.lang.reflect.Constructor;
import java.util.List;

import static java.util.Objects.requireNonNull;

@Immutable
public class ThriftConstructorInjection
{
    private final Constructor<?> constructor;
    private final List<ThriftParameterInjection> parameters;

    public ThriftConstructorInjection(Constructor<?> constructor, ThriftParameterInjection... parameters)
    {
        this(
                requireNonNull(constructor, "constructor is null"),
                ImmutableList.copyOf(requireNonNull(parameters, "parameters is null")));
    }

    public ThriftConstructorInjection(Constructor<?> constructor, List<ThriftParameterInjection> parameters)
    {
        this.constructor = requireNonNull(constructor, "constructor is null");
        this.parameters = ImmutableList.copyOf(requireNonNull(parameters, "parameters is null"));
    }

    public Constructor<?> getConstructor()
    {
        return constructor;
    }

    public List<ThriftParameterInjection> getParameters()
    {
        return parameters;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(constructor.getName());
        sb.append('(');
        Joiner.on(", ").appendTo(sb, parameters);
        sb.append(')');
        return sb.toString();
    }
}
