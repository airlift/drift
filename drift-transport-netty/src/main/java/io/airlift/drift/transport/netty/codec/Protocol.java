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
import io.airlift.drift.protocol.TProtocolFactory;

public enum Protocol
{
    BINARY {
        @Override
        public TProtocolFactory createProtocolFactory(Transport transport)
        {
            return new TBinaryProtocol.Factory();
        }
    },
    COMPACT {
        @Override
        public TProtocolFactory createProtocolFactory(Transport transport)
        {
            // Header transport uses the FB fork of the compact protocol
            if (transport == Transport.HEADER) {
                return new TFacebookCompactProtocol.Factory();
            }
            return new TCompactProtocol.Factory();
        }
    };

    public abstract TProtocolFactory createProtocolFactory(Transport transport);
}
