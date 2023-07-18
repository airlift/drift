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
package io.airlift.drift.codec.internal.compiler;

import com.google.errorprone.annotations.Immutable;
import com.google.inject.Inject;
import io.airlift.bytecode.DynamicClassLoader;
import io.airlift.drift.codec.ThriftCodec;
import io.airlift.drift.codec.ThriftCodecManager;
import io.airlift.drift.codec.internal.ForCompiler;
import io.airlift.drift.codec.internal.ThriftCodecFactory;
import io.airlift.drift.codec.metadata.ThriftStructMetadata;

import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * Creates Thrift codecs directly in byte code.
 */
@Immutable
public class CompilerThriftCodecFactory
        implements ThriftCodecFactory
{
    private final boolean debug;
    private final DynamicClassLoader classLoader;

    @Inject
    public CompilerThriftCodecFactory(@ForCompiler ClassLoader parent)
    {
        this(false, parent);
    }

    public CompilerThriftCodecFactory(boolean debug)
    {
        this(debug, getPrivilegedClassLoader(CompilerThriftCodecFactory.class.getClassLoader()));
    }

    public CompilerThriftCodecFactory(boolean debug, ClassLoader parent)
    {
        this.debug = debug;
        this.classLoader = getPrivilegedClassLoader(parent);
    }

    @Override
    public ThriftCodec<?> generateThriftTypeCodec(ThriftCodecManager codecManager, ThriftStructMetadata metadata)
    {
        ThriftCodecByteCodeGenerator<?> generator = new ThriftCodecByteCodeGenerator<>(
                codecManager,
                metadata,
                classLoader,
                debug);
        return generator.getThriftCodec();
    }

    private static DynamicClassLoader getPrivilegedClassLoader(ClassLoader parent)
    {
        return AccessController.doPrivileged((PrivilegedAction<DynamicClassLoader>) () -> new DynamicClassLoader(parent));
    }
}
