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
package io.airlift.drift.transport.netty.scribe.apache;

import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.apache.thrift.TApplicationException.UNSUPPORTED_CLIENT_TYPE;

public class ScribeService
        implements scribe.Iface
{
    private final List<LogEntry> messages = new CopyOnWriteArrayList<>();

    public List<LogEntry> getMessages()
    {
        return messages;
    }

    @Override
    public ResultCode Log(List<LogEntry> messages)
            throws TException
    {
        if (messages != null) {
            for (LogEntry message : messages) {
                if (message.getCategory().equals("exception")) {
                    throw new TApplicationException(UNSUPPORTED_CLIENT_TYPE, message.getMessage());
                }
            }
            this.messages.addAll(messages);
        }
        return ResultCode.OK;
    }
}
