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
package io.airlift.drift.integration.guice;

import io.airlift.drift.integration.scribe.drift.DriftLogEntry;

import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;

public class EchoServiceHandler
        implements EchoService
{
    @Override
    public void echoVoid()
    {
        // nothing to return
    }

    @Override
    public boolean echoBoolean(boolean value)
    {
        return value;
    }

    @Override
    public byte echoByte(byte value)
    {
        return value;
    }

    @Override
    public short echoShort(short value)
    {
        return value;
    }

    @Override
    public int echoInt(int value)
    {
        return value;
    }

    @Override
    public long echoLong(long value)
    {
        return value;
    }

    @Override
    public double echoDouble(double value)
    {
        return value;
    }

    @Override
    public String echoString(String value)
            throws NullValueException
    {
        return notNull(value);
    }

    @Override
    public byte[] echoBinary(byte[] value)
            throws NullValueException
    {
        return notNull(value);
    }

    @Override
    public DriftLogEntry echoStruct(DriftLogEntry value)
            throws NullValueException
    {
        return notNull(value);
    }

    @Override
    public Integer echoInteger(Integer value)
            throws NullValueException
    {
        return notNull(value);
    }

    @Override
    public List<Integer> echoListInteger(List<Integer> value)
            throws NullValueException
    {
        return notNull(value);
    }

    @Override
    public List<String> echoListString(List<String> value)
            throws NullValueException
    {
        return notNull(value);
    }

    @Override
    public String echoOptionalString(Optional<String> value)
            throws EmptyOptionalException
    {
        return value.orElseThrow(EmptyOptionalException::new);
    }

    @Override
    public int echoOptionalInt(OptionalInt value)
            throws EmptyOptionalException
    {
        return value.orElseThrow(EmptyOptionalException::new);
    }

    @Override
    public long echoOptionalLong(OptionalLong value)
            throws EmptyOptionalException
    {
        return value.orElseThrow(EmptyOptionalException::new);
    }

    @Override
    public double echoOptionalDouble(OptionalDouble value)
            throws EmptyOptionalException
    {
        return value.orElseThrow(EmptyOptionalException::new);
    }

    @Override
    public DriftLogEntry echoOptionalStruct(Optional<DriftLogEntry> value)
            throws EmptyOptionalException
    {
        return value.orElseThrow(EmptyOptionalException::new);
    }

    @Override
    public List<Integer> echoOptionalListInteger(Optional<List<Integer>> value)
            throws EmptyOptionalException
    {
        return value.orElseThrow(EmptyOptionalException::new);
    }

    @Override
    public List<String> echoOptionalListString(Optional<List<String>> value)
            throws EmptyOptionalException
    {
        return value.orElseThrow(EmptyOptionalException::new);
    }

    private static <T> T notNull(T value)
            throws NullValueException
    {
        if (value == null) {
            throw new NullValueException();
        }
        return value;
    }
}
