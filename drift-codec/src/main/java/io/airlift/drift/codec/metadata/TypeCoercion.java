/*
 * Copyright (C) 2012 Facebook, Inc.
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
package io.airlift.drift.codec.metadata;

import javax.annotation.concurrent.Immutable;

import java.lang.reflect.Method;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@Immutable
public class TypeCoercion
{
    private final ThriftType thriftType;
    private final Method toThrift;
    private final Method fromThrift;

    public TypeCoercion(ThriftType thriftType, Method toThrift, Method fromThrift)
    {
        this.thriftType = requireNonNull(thriftType, "thriftType is null");
        this.toThrift = requireNonNull(toThrift, "toThrift is null");
        this.fromThrift = requireNonNull(fromThrift, "fromThrift is null");
    }

    public ThriftType getThriftType()
    {
        return thriftType;
    }

    public Method getToThrift()
    {
        return toThrift;
    }

    public Method getFromThrift()
    {
        return fromThrift;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("thriftType", thriftType)
                .add("toThrift", toThrift)
                .add("fromThrift", fromThrift)
                .toString();
    }
}
