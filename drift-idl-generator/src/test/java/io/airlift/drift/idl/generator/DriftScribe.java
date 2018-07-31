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
package io.airlift.drift.idl.generator;

import io.airlift.drift.annotations.ThriftDocumentation;
import io.airlift.drift.annotations.ThriftId;
import io.airlift.drift.annotations.ThriftMethod;
import io.airlift.drift.annotations.ThriftOrder;
import io.airlift.drift.annotations.ThriftService;

import java.util.List;
import java.util.Map;

@ThriftDocumentation("Scribe logging service")
@ThriftService("Scribe")
public interface DriftScribe
{
    @ThriftDocumentation("Shutdown the service")
    @ThriftOrder(4)
    @ThriftMethod(oneway = true)
    void shutdown();

    @ThriftDocumentation({
            "Send a message to Scribe.",
            "",
            "@param messages the list of messages to send",
    })
    @ThriftOrder(1)
    @ThriftMethod
    DriftResultCode log(List<DriftLogEntry> messages)
            throws
            @ThriftId(1) ScribeDataException,
            @ThriftId(2) ScribeTransportException;

    @ThriftDocumentation("Send a formatted message to Scribe.")
    @ThriftOrder(2)
    @ThriftMethod
    DriftResultCode logFormattedMessage(String format, Map<String, DriftLogEntry> messages, int maxSize)
            throws
            @ThriftId(1) ScribeDataException,
            @ThriftId(2) ScribeTransportException,
            @ThriftId(3) ScribeMessageException;

    @ThriftDocumentation("Check if service is up")
    @ThriftOrder(3)
    @ThriftMethod
    void ping();
}
