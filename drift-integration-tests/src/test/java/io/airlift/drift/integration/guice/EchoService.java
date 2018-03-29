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

import io.airlift.drift.annotations.ThriftMethod;
import io.airlift.drift.annotations.ThriftService;
import io.airlift.drift.annotations.ThriftStruct;
import io.airlift.drift.integration.scribe.drift.DriftLogEntry;

import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;

@ThriftService("echo")
public interface EchoService
{
    @ThriftMethod
    void echoVoid();

    @ThriftMethod
    boolean echoBoolean(boolean value);

    @ThriftMethod
    byte echoByte(byte value);

    @ThriftMethod
    short echoShort(short value);

    @ThriftMethod
    int echoInt(int value);

    @ThriftMethod
    long echoLong(long value);

    @ThriftMethod
    double echoDouble(double value);

    @ThriftMethod
    String echoString(String value)
            throws NullValueException;

    @ThriftMethod
    byte[] echoBinary(byte[] value)
            throws NullValueException;

    @ThriftMethod
    DriftLogEntry echoStruct(DriftLogEntry value)
            throws NullValueException;

    @ThriftMethod
    Integer echoInteger(Integer value)
            throws NullValueException;

    @ThriftMethod
    List<Integer> echoListInteger(List<Integer> value)
            throws NullValueException;

    @ThriftMethod
    List<String> echoListString(List<String> value)
            throws NullValueException;

    @ThriftMethod
    int echoOptionalInt(OptionalInt value)
            throws EmptyOptionalException;

    @ThriftMethod
    long echoOptionalLong(OptionalLong value)
            throws EmptyOptionalException;

    @ThriftMethod
    double echoOptionalDouble(OptionalDouble value)
            throws EmptyOptionalException;

    @ThriftMethod
    String echoOptionalString(Optional<String> value)
            throws EmptyOptionalException;

    @ThriftMethod
    DriftLogEntry echoOptionalStruct(Optional<DriftLogEntry> value)
            throws EmptyOptionalException;

    @ThriftMethod
    List<Integer> echoOptionalListInteger(Optional<List<Integer>> value)
            throws EmptyOptionalException;

    @ThriftMethod
    List<String> echoOptionalListString(Optional<List<String>> value)
            throws EmptyOptionalException;

    @ThriftStruct
    class NullValueException
            extends Exception
    {}

    @ThriftStruct
    class EmptyOptionalException
            extends Exception
    {}
}
