/*
 * Copyright (C) 2018 Facebook, Inc.
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
package io.airlift.drift.transport.netty.buffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

import javax.annotation.concurrent.GuardedBy;

import java.io.Closeable;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class TestingPooledByteBufAllocator
        extends PooledByteBufAllocator
        implements Closeable
{
    public TestingPooledByteBufAllocator()
    {
        super(false);
    }

    @GuardedBy("this")
    private final List<WeakReference<ByteBuf>> trackedBuffers = new ArrayList<>();

    @Override
    protected ByteBuf newHeapBuffer(int initialCapacity, int maxCapacity)
    {
        return track(super.newHeapBuffer(initialCapacity, maxCapacity));
    }

    @Override
    protected ByteBuf newDirectBuffer(int initialCapacity, int maxCapacity)
    {
        return track(super.newDirectBuffer(initialCapacity, maxCapacity));
    }

    @Override
    public CompositeByteBuf compositeHeapBuffer(int maxNumComponents)
    {
        return track(super.compositeHeapBuffer(maxNumComponents));
    }

    @Override
    public CompositeByteBuf compositeDirectBuffer(int maxNumComponents)
    {
        return track(super.compositeDirectBuffer(maxNumComponents));
    }

    public synchronized List<ByteBuf> getReferencedBuffers()
    {
        return trackedBuffers.stream()
                .map(WeakReference::get)
                .filter(Objects::nonNull)
                .filter(byteBuf -> byteBuf.refCnt() > 0)
                .collect(toImmutableList());
    }

    private synchronized CompositeByteBuf track(CompositeByteBuf byteBuf)
    {
        trackedBuffers.add(new WeakReference<>(byteBuf));
        trackedBuffers.removeIf(byteBufWeakReference -> byteBufWeakReference.get() == null);
        return byteBuf;
    }

    private synchronized ByteBuf track(ByteBuf byteBuf)
    {
        trackedBuffers.add(new WeakReference<>(byteBuf));
        trackedBuffers.removeIf(byteBufWeakReference -> byteBufWeakReference.get() == null);
        return byteBuf;
    }

    @Override
    public void close()
    {
        List<ByteBuf> referencedBuffers = getReferencedBuffers();
        if (!referencedBuffers.isEmpty()) {
            throw new AssertionError("LEAK");
        }
    }
}
