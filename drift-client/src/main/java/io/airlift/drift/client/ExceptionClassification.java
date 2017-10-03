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
package io.airlift.drift.client;

import java.util.EnumSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.Ordering.natural;
import static io.airlift.drift.client.ExceptionClassification.HostStatus.NORMAL;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collector.Characteristics.CONCURRENT;
import static java.util.stream.Collector.Characteristics.UNORDERED;

public final class ExceptionClassification
{
    public static final ExceptionClassification NORMAL_EXCEPTION = new ExceptionClassification(Optional.empty(), NORMAL);

    public enum HostStatus
    {
        NORMAL, OVERLOADED, DOWN
    }

    private final Optional<Boolean> retry;
    private final HostStatus hostStatus;

    public ExceptionClassification(Optional<Boolean> retry, HostStatus hostStatus)
    {
        this.retry = requireNonNull(retry, "retry is null");
        this.hostStatus = requireNonNull(hostStatus, "hostStatus is null");
    }

    public Optional<Boolean> isRetry()
    {
        return retry;
    }

    public HostStatus getHostStatus()
    {
        return hostStatus;
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
        ExceptionClassification that = (ExceptionClassification) o;
        return Objects.equals(retry, that.retry) &&
                hostStatus == that.hostStatus;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(retry, hostStatus);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("retry", retry)
                .add("hostStatus", hostStatus)
                .toString();
    }

    public static Collector<ExceptionClassification, ?, ExceptionClassification> mergeExceptionClassifications()
    {
        return new MergeExceptionClassificationsCollector();
    }

    private static class MergeExceptionClassificationsCollector
            implements Collector<ExceptionClassification, MergeExceptionClassificationsCollector.Accumulator, ExceptionClassification>
    {
        @Override
        public Supplier<Accumulator> supplier()
        {
            return Accumulator::new;
        }

        @Override
        public BiConsumer<Accumulator, ExceptionClassification> accumulator()
        {
            return Accumulator::add;
        }

        @Override
        public BinaryOperator<Accumulator> combiner()
        {
            return Accumulator::add;
        }

        @Override
        public Function<Accumulator, ExceptionClassification> finisher()
        {
            return Accumulator::toExceptionClassification;
        }

        @Override
        public Set<Characteristics> characteristics()
        {
            return EnumSet.of(CONCURRENT, UNORDERED);
        }

        public static class Accumulator
        {
            private Optional<Boolean> retry = Optional.empty();
            private HostStatus hostStatus = NORMAL;

            public void add(ExceptionClassification classification)
            {
                retry = mergeRetry(retry, classification.isRetry());
                hostStatus = natural().max(hostStatus, classification.getHostStatus());
            }

            public Accumulator add(Accumulator accumulation)
            {
                retry = mergeRetry(retry, accumulation.retry);
                hostStatus = natural().max(hostStatus, accumulation.hostStatus);
                return this;
            }

            public ExceptionClassification toExceptionClassification()
            {
                return new ExceptionClassification(retry, hostStatus);
            }

            @SuppressWarnings("OptionalIsPresent")
            public static Optional<Boolean> mergeRetry(Optional<Boolean> oldRetry, Optional<Boolean> newRetry)
            {
                if (!oldRetry.isPresent()) {
                    return newRetry;
                }
                if (!newRetry.isPresent()) {
                    return oldRetry;
                }
                return Optional.of(oldRetry.get() && newRetry.get());
            }
        }
    }
}
