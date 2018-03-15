/*
 * Copyright (C) 2018 Facebook, Inc.
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
package io.airlift.drift.transport.netty.codec;

import io.airlift.drift.protocol.TBinaryProtocol;
import io.airlift.drift.protocol.TCompactProtocol;
import io.airlift.drift.protocol.TFacebookCompactProtocol;
import io.airlift.drift.protocol.TProtocol;
import io.airlift.drift.protocol.TTransport;

public enum Protocol
{
    BINARY {
        @Override
        public TProtocol createProtocol(TTransport transport)
        {
            return new TBinaryProtocol(transport);
        }

        @Override
        public int getHeaderTransportId()
        {
            return 0;
        }
    },
    COMPACT {
        @Override
        public TProtocol createProtocol(TTransport transport)
        {
            return new TCompactProtocol(transport);
        }

        @Override
        public int getHeaderTransportId()
        {
            throw new IllegalStateException("COMPACT can not be used with HEADER transport; use FB_COMPACT instead");
        }
    },
    FB_COMPACT {
        @Override
        public TProtocol createProtocol(TTransport transport)
        {
            return new TFacebookCompactProtocol(transport);
        }

        @Override
        public int getHeaderTransportId()
        {
            return 2;
        }
    };

    public abstract TProtocol createProtocol(TTransport transport);

    public abstract int getHeaderTransportId();

    public static Protocol getProtocolByHeaderTransportId(int headerTransportId)
    {
        if (headerTransportId == BINARY.getHeaderTransportId()) {
            return BINARY;
        }
        if (headerTransportId == FB_COMPACT.getHeaderTransportId()) {
            return FB_COMPACT;
        }
        throw new IllegalArgumentException("Unsupported header transport protocol ID: " + headerTransportId);
    }
}
