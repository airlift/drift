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
package io.airlift.drift.transport.netty.server;

import io.airlift.drift.TApplicationException;
import io.airlift.drift.TException;
import io.airlift.drift.codec.ThriftCodec;
import io.airlift.drift.codec.ThriftCodecManager;
import io.airlift.drift.protocol.TProtocolWriter;

final class ExceptionWriter
{
    private static final ThriftCodec<TApplicationException> CODEC =
            new ThriftCodecManager().getCodec(TApplicationException.class);

    private ExceptionWriter() {}

    public static void writeTApplicationException(TApplicationException exception, TProtocolWriter protocol)
            throws TException
    {
        try {
            CODEC.write(exception, protocol);
        }
        catch (TException e) {
            throw e;
        }
        catch (Exception e) {
            throw new TException(e);
        }
    }
}
