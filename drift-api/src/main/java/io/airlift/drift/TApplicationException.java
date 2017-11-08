/*
 * Copyright (C) 2017 Facebook, Inc.
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
package io.airlift.drift;

import io.airlift.drift.annotations.ThriftConstructor;
import io.airlift.drift.annotations.ThriftField;
import io.airlift.drift.annotations.ThriftStruct;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

@ThriftStruct
public class TApplicationException
        extends TException
{
    public enum Type
    {
        UNKNOWN(0),
        UNKNOWN_METHOD(1),
        INVALID_MESSAGE_TYPE(2),
        WRONG_METHOD_NAME(3),
        BAD_SEQUENCE_ID(4),
        MISSING_RESULT(5),
        INTERNAL_ERROR(6),
        PROTOCOL_ERROR(7),
        INVALID_TRANSFORM(8),
        INVALID_PROTOCOL(9),
        UNSUPPORTED_CLIENT_TYPE(10);

        private final int type;

        Type(int type)
        {
            this.type = type;
        }

        public int getType()
        {
            return type;
        }

        private static final Map<Integer, Type> TYPES_BY_ID = unmodifiableMap(Arrays.stream(Type.values())
                .collect(Collectors.toMap(Type::getType, identity())));

        public static Optional<Type> fromTypeId(int typeId)
        {
            return Optional.ofNullable(TYPES_BY_ID.get(typeId));
        }
    }

    private final int typeValue;
    private final Optional<Type> type;

    public TApplicationException()
    {
        this(Type.UNKNOWN, null);
    }

    public TApplicationException(Type type, String message)
    {
        super(message);
        requireNonNull(type, "type is null");
        this.type = Optional.of(type);
        this.typeValue = type.getType();
    }

    @ThriftConstructor
    public TApplicationException(int typeValue, String message)
    {
        super(message);
        this.typeValue = typeValue;
        this.type = Type.fromTypeId(typeValue);
    }

    @Override
    @ThriftField(1)
    public String getMessage()
    {
        return super.getMessage();
    }

    @ThriftField(2)
    public int getTypeValue()
    {
        return typeValue;
    }

    public Optional<Type> getType()
    {
        return type;
    }
}
