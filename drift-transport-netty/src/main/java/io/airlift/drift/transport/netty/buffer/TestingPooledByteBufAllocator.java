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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.ResourceLeakDetector;

import javax.annotation.concurrent.GuardedBy;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * This is a custom ByteBufAllocator that tracks outstanding allocations and
 * throws from the close() method if it detects any leaked buffers.
 *
 * Never use this class in production, it will cause your server to run out
 * of memory! This is because it holds strong references to all allocated
 * buffers and doesn't release them until close() is called at the end of a
 * unit test.
 */
public class TestingPooledByteBufAllocator
        extends PooledByteBufAllocator
        implements Closeable
{
    /**
     * Call this instead of the constructor. It will turn on netty's
     * built-in resource leak tracking before creating the test allocator.
     * When the test allocator's <code>close()</code> method is called,
     * the resource leak tracking setting will be reverted to its
     * original value.
     * @return a new <code>TestingPooledByteBufAllocator</code>.
     */
    public static TestingPooledByteBufAllocator newAllocator()
    {
        ResourceLeakDetector.Level oldLevel = ResourceLeakDetector.getLevel();
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
        return new TestingPooledByteBufAllocator(oldLevel);
    }

    private TestingPooledByteBufAllocator(ResourceLeakDetector.Level oldLevel)
    {
        super(false);
        this.oldLevel = oldLevel;
    }

    @GuardedBy("this")
    private final List<ByteBuf> trackedBuffers = new ArrayList<>();

    private final ResourceLeakDetector.Level oldLevel;

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

    private synchronized CompositeByteBuf track(CompositeByteBuf byteBuf)
    {
        trackedBuffers.add(byteBuf);
        return byteBuf;
    }

    private synchronized ByteBuf track(ByteBuf byteBuf)
    {
        trackedBuffers.add(byteBuf);
        return byteBuf;
    }

    @Override
    @SuppressFBWarnings(value = "DM_GC", justification = "Netty's leak detection only works on GC'ed buffers")
    public void close()
    {
        try {
            long referencedBuffersCount;
            synchronized (this) {
                referencedBuffersCount = trackedBuffers.stream()
                        .filter(Objects::nonNull)
                        .filter(byteBuf -> byteBuf.refCnt() > 0)
                        .count();
                trackedBuffers.clear(); // Make tracked buffers eligible for GC
            }
            // Throw an error if there were any leaked buffers
            if (referencedBuffersCount > 0) {
                // Trigger a GC. This will hopefully (but not necessarily) print
                // details about detected leaks to standard error before the error
                // is thrown.
                System.gc();
                throw new AssertionError("LEAK");
            }
        }
        finally {
            ResourceLeakDetector.setLevel(oldLevel);
        }
    }
}
