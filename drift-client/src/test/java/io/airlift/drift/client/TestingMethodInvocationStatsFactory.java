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

import io.airlift.drift.client.stats.MethodInvocationStatsFactory;
import io.airlift.drift.codec.metadata.ThriftServiceMetadata;
import io.airlift.drift.transport.MethodMetadata;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.util.Objects.requireNonNull;

public class TestingMethodInvocationStatsFactory
        implements MethodInvocationStatsFactory
{
    private final ConcurrentMap<Key, TestingMethodInvocationStat> stats = new ConcurrentHashMap<>();

    public TestingMethodInvocationStat getStat(String serviceName, Optional<String> qualifier, String methodName)
    {
        return stats.get(new Key(serviceName, qualifier, methodName));
    }

    @Override
    public TestingMethodInvocationStat getStat(ThriftServiceMetadata serviceMetadata, Optional<String> qualifier, MethodMetadata metadata)
    {
        return stats.computeIfAbsent(new Key(serviceMetadata.getName(), qualifier, metadata.getName()), key -> new TestingMethodInvocationStat());
    }

    private static class Key
    {
        private final String serviceName;
        private final Optional<String> qualifier;
        private final String methodName;

        public Key(String serviceName, Optional<String> qualifier, String methodName)
        {
            this.serviceName = requireNonNull(serviceName, "serviceName is null");
            this.qualifier = requireNonNull(qualifier, "qualifier is null");
            this.methodName = requireNonNull(methodName, "methodName is null");
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
            Key key = (Key) o;
            return Objects.equals(serviceName, key.serviceName) &&
                    Objects.equals(qualifier, key.qualifier) &&
                    Objects.equals(methodName, key.methodName);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(serviceName, qualifier, methodName);
        }

        @Override
        public String toString()
        {
            return serviceName +
                    qualifier.map(name -> "." + name).orElse("") +
                    "." + methodName;
        }
    }
}
