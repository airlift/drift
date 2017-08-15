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
package io.airlift.drift.protocol;

import static java.util.Objects.requireNonNull;

public final class TField
{
    private final String name;
    private final byte type;
    private final short id;

    public TField(String name, byte type, short id)
    {
        this.name = requireNonNull(name, "name is null");
        this.type = type;
        this.id = id;
    }

    public String getName()
    {
        return name;
    }

    public byte getType()
    {
        return type;
    }

    public short getId()
    {
        return id;
    }
}
