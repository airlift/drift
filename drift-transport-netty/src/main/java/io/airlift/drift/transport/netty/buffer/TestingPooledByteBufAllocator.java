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
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.ResourceLeakDetector;

import javax.annotation.concurrent.GuardedBy;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;

import static io.netty.util.ResourceLeakDetector.Level.PARANOID;

/**
 * This is a custom {@link ByteBufAllocator} that tracks outstanding allocations and
 * throws from the {@link #close()} method if it detects any leaked buffers.
 * <p>
 * Never use this class in production, it will cause your server to run out
 * of memory! This is because it holds strong references to all allocated
 * buffers and doesn't release them until {@link #close()} is called at the end of a
 * unit test.
 */
public class TestingPooledByteBufAllocator
        extends PooledByteBufAllocator
        implements Closeable
{
    @GuardedBy("this")
    private final List<ByteBuf> trackedBuffers = new ArrayList<>();

    private final ResourceLeakDetector.Level oldLevel;

    public TestingPooledByteBufAllocator()
    {
        this(getAndSetResourceLeakDetectorLevel(PARANOID));
    }

    private static ResourceLeakDetector.Level getAndSetResourceLeakDetectorLevel(ResourceLeakDetector.Level newLevel)
    {
        ResourceLeakDetector.Level oldLevel = ResourceLeakDetector.getLevel();
        ResourceLeakDetector.setLevel(newLevel);
        return oldLevel;
    }

    private TestingPooledByteBufAllocator(ResourceLeakDetector.Level oldLevel)
    {
        super(false);
        this.oldLevel = oldLevel;
    }

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
    @SuppressFBWarnings(value = "DM_GC", justification = "Netty's leak detection only works if buffer is garbage collected")
    public void close()
    {
        try {
            boolean leaked;
            synchronized (this) {
                leaked = trackedBuffers.stream()
                        .anyMatch(byteBuf -> byteBuf.refCnt() > 0);
                trackedBuffers.clear();
            }
            // Throw an error if there were any leaked buffers
            if (leaked) {
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
