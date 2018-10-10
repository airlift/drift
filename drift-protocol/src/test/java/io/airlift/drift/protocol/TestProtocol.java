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
package io.airlift.drift.protocol;

import io.airlift.drift.TException;
import org.testng.annotations.Test;

import java.util.function.Function;

import static org.testng.Assert.assertEquals;

public class TestProtocol
{
    @Test
    public void testFloat()
            throws Exception
    {
        assertFloat(TBinaryProtocol::new);
        assertFloat(TCompactProtocol::new);
        assertFloat(TFacebookCompactProtocol::new);
    }

    private static void assertFloat(Function<TTransport, TProtocol> factory)
            throws TException
    {
        TTransport transport = new TMemoryBuffer(0);
        TProtocol protocol = factory.apply(transport);
        protocol.writeFloat(123.45f);
        assertEquals(protocol.readFloat(), 123.45f);
    }
}
