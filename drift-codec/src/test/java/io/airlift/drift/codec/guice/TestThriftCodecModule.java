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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Stage;
import com.google.inject.TypeLiteral;
import io.airlift.drift.codec.BonkConstructor;
import io.airlift.drift.codec.ThriftCodec;
import io.airlift.drift.codec.metadata.ThriftType;
import io.airlift.drift.protocol.TCompactProtocol;
import io.airlift.drift.protocol.TMemoryBuffer;
import io.airlift.drift.protocol.TProtocolReader;
import io.airlift.drift.protocol.TProtocolWriter;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.drift.codec.guice.ThriftCodecBinder.thriftCodecBinder;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestThriftCodecModule
{
    @Test
    public void testThriftClientAndServerModules()
            throws Exception
    {
        Injector injector = Guice.createInjector(
                Stage.PRODUCTION,
                new ThriftCodecModule(),
                binder -> {
                    thriftCodecBinder(binder).bindThriftCodec(BonkConstructor.class);
                    thriftCodecBinder(binder).bindListThriftCodec(BonkConstructor.class);
                    thriftCodecBinder(binder).bindMapThriftCodec(String.class, BonkConstructor.class);

                    thriftCodecBinder(binder).bindThriftCodec(new TypeLiteral<Map<Integer, List<String>>>() {});

                    thriftCodecBinder(binder).bindCustomThriftCodec(new ThriftCodec<ValueClass>()
                    {
                        @Override
                        public ThriftType getType()
                        {
                            return new ThriftType(ThriftType.STRING, ValueClass.class);
                        }

                        @Override
                        public ValueClass read(TProtocolReader protocol)
                                throws Exception
                        {
                            return new ValueClass(protocol.readString());
                        }

                        @Override
                        public void write(ValueClass value, TProtocolWriter protocol)
                                throws Exception
                        {
                            protocol.writeString(value.getValue());
                        }
                    });
                });

        testRoundTripSerialize(
                injector.getInstance(Key.get(new TypeLiteral<ThriftCodec<BonkConstructor>>() {})),
                new BonkConstructor("message", 42));

        testRoundTripSerialize(
                injector.getInstance(Key.get(new TypeLiteral<ThriftCodec<List<BonkConstructor>>>() {})),
                ImmutableList.of(new BonkConstructor("one", 1), new BonkConstructor("two", 2)));

        testRoundTripSerialize(
                injector.getInstance(Key.get(new TypeLiteral<ThriftCodec<Map<String, BonkConstructor>>>() {})),
                ImmutableMap.of("uno", new BonkConstructor("one", 1), "dos", new BonkConstructor("two", 2)));

        testRoundTripSerialize(
                injector.getInstance(Key.get(new TypeLiteral<ThriftCodec<Map<Integer, List<String>>>>() {})),
                ImmutableMap.of(1, ImmutableList.of("one", "uno"), 2, ImmutableList.of("two", "dos")));

        testRoundTripSerialize(
                injector.getInstance(Key.get(new TypeLiteral<ThriftCodec<ValueClass>>() {})),
                new ValueClass("my value"));
    }

    public static <T> void testRoundTripSerialize(ThriftCodec<T> codec, T value)
            throws Exception
    {
        // write value
        TMemoryBuffer transport = new TMemoryBuffer(10 * 1024);
        TCompactProtocol protocol = new TCompactProtocol(transport);
        codec.write(value, protocol);

        // read value back
        T copy = codec.read(protocol);
        assertNotNull(copy);

        // verify they are the same
        assertEquals(copy, value);
    }

    public static class ValueClass
    {
        private final String value;

        public ValueClass(String value)
        {
            this.value = requireNonNull(value, "value is null");
        }

        public String getValue()
        {
            return value;
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
            ValueClass that = (ValueClass) o;
            return Objects.equals(value, that.value);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(value);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("value", value)
                    .toString();
        }
    }
}
