/*
 * Copyright (C) 2020 The Drift Authors
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

import io.airlift.drift.annotations.ThriftConstructor;
import io.airlift.drift.annotations.ThriftField;
import io.airlift.drift.annotations.ThriftStruct;

@ThriftStruct
public class RetryableException
        extends Exception
{
    @ThriftConstructor
    public RetryableException(String message)
    {
        super(message);
    }

    @ThriftField(1)
    @Override
    public String getMessage()
    {
        return super.getMessage();
    }
}
