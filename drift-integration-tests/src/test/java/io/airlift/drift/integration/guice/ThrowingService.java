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

import io.airlift.drift.TException;
import io.airlift.drift.annotations.ThriftMethod;
import io.airlift.drift.annotations.ThriftService;
import io.airlift.units.DataSize;

import static io.airlift.units.DataSize.Unit.KILOBYTE;

@ThriftService("throwing")
public interface ThrowingService
{
    DataSize MAX_FRAME_SIZE = new DataSize(10, KILOBYTE);

    @ThriftMethod
    void fail(String message, boolean retryable)
            throws ExampleException;

    @ThriftMethod
    byte[] generateTooLargeFrame()
            throws TException;
}
