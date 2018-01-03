/*
 * Copyright (C) 2017 Facebook, Inc.
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
package io.airlift.drift.idl.generator;

import io.airlift.drift.annotations.ThriftDocumentation;
import io.airlift.drift.annotations.ThriftField;
import io.airlift.drift.annotations.ThriftStruct;

import static io.airlift.drift.annotations.ThriftField.Requiredness.OPTIONAL;
import static io.airlift.drift.annotations.ThriftField.Requiredness.REQUIRED;

@ThriftDocumentation("Two dimensional point.")
@ThriftStruct
public class Point
{
    @ThriftField(value = 1, requiredness = REQUIRED)
    public int x;

    @ThriftField(value = 2, requiredness = REQUIRED)
    public int y;

    @ThriftDocumentation("Info about the point")
    @ThriftField(value = 3, requiredness = OPTIONAL)
    public String comment;

    @ThriftField(value = 4, requiredness = OPTIONAL)
    public int tag;
}
