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
package io.airlift.drift.codec.guice;

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.internal.MoreTypes.ParameterizedTypeImpl;
import io.airlift.drift.codec.InternalThriftCodec;
import io.airlift.drift.codec.ThriftCodec;
import io.airlift.drift.codec.ThriftCodecManager;

import javax.inject.Provider;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static java.util.Objects.requireNonNull;

public class ThriftCodecBinder
{
    public static ThriftCodecBinder thriftCodecBinder(Binder binder)
    {
        return new ThriftCodecBinder(binder);
    }

    private final Binder binder;

    private ThriftCodecBinder(Binder binder)
    {
        this.binder = binder;
    }

    public void bindCustomThriftCodec(ThriftCodec<?> thriftCodec)
    {
        requireNonNull(thriftCodec, "thriftCodec is null");

        // bind the custom codec instance to the internal thrift codec set
        newSetBinder(binder, new TypeLiteral<ThriftCodec<?>>() {}, InternalThriftCodec.class).addBinding().toInstance(thriftCodec);

        // make the custom codec available to user code for binding
        Type type = thriftCodec.getType().getJavaType();
        binder.bind(getThriftCodecKey(type)).toProvider(new ThriftCodecProvider(type)).in(Scopes.SINGLETON);
    }

    public void bindCustomThriftCodec(Class<? extends ThriftCodec<?>> thriftCodecType)
    {
        requireNonNull(thriftCodecType, "thriftCodecType is null");
        bindCustomThriftCodec(Key.get(thriftCodecType));
    }

    public void bindCustomThriftCodec(TypeLiteral<? extends ThriftCodec<?>> thriftCodecType)
    {
        requireNonNull(thriftCodecType, "thriftCodecType is null");
        bindCustomThriftCodec(Key.get(thriftCodecType));
    }

    public void bindCustomThriftCodec(Key<? extends ThriftCodec<?>> thriftCodecKey)
    {
        requireNonNull(thriftCodecKey, "thriftCodecKey is null");

        // bind the custom codec type to the internal thrift codec set
        newSetBinder(binder, new TypeLiteral<ThriftCodec<?>>() {}, InternalThriftCodec.class).addBinding().to(thriftCodecKey);

        // make the custom codec available to user code for binding
        binder.bind(thriftCodecKey).in(Scopes.SINGLETON);
    }

    public void bindThriftCodec(Class<?> type)
    {
        requireNonNull(type, "type is null");
        bindThriftCodec(TypeLiteral.get(type));
    }

    public void bindThriftCodec(TypeLiteral<?> type)
    {
        requireNonNull(type, "type is null");
        bindThriftCodec(Key.get(type));
    }

    public void bindThriftCodec(Key<?> key)
    {
        requireNonNull(key, "key is null");
        Type type = key.getTypeLiteral().getType();
        binder.bind(getThriftCodecKey(type)).toProvider(new ThriftCodecProvider(type)).in(Scopes.SINGLETON);
    }

    public void bindListThriftCodec(Class<?> type)
    {
        requireNonNull(type, "type is null");

        ParameterizedTypeImpl listType = new ParameterizedTypeImpl(null, List.class, type);
        binder.bind(getThriftCodecKey(listType)).toProvider(new ThriftCodecProvider(listType)).in(Scopes.SINGLETON);
    }

    public void bindMapThriftCodec(Class<?> keyType, Class<?> valueType)
    {
        requireNonNull(keyType, "keyType is null");
        requireNonNull(valueType, "valueType is null");

        ParameterizedTypeImpl mapType = new ParameterizedTypeImpl(null, Map.class, keyType, valueType);
        binder.bind(getThriftCodecKey(mapType)).toProvider(new ThriftCodecProvider(mapType)).in(Scopes.SINGLETON);
    }

    private Key<ThriftCodec<?>> getThriftCodecKey(Type type)
    {
        return (Key<ThriftCodec<?>>) Key.get(new ParameterizedTypeImpl(null, ThriftCodec.class, type));
    }

    static class ThriftCodecProvider
            implements Provider<ThriftCodec<?>>
    {
        private final Type type;
        private ThriftCodecManager thriftCodecManager;

        public ThriftCodecProvider(Type type)
        {
            this.type = type;
        }

        @Inject
        public void setThriftCodecManager(ThriftCodecManager thriftCodecManager)
        {
            this.thriftCodecManager = thriftCodecManager;
        }

        @Override
        public ThriftCodec<?> get()
        {
            return thriftCodecManager.getCodec(type);
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
            ThriftCodecProvider that = (ThriftCodecProvider) o;
            return Objects.equals(type, that.type);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(type);
        }
    }
}
