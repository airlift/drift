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

import io.airlift.drift.annotations.ThriftMethod;
import io.airlift.drift.annotations.ThriftService;
import io.airlift.drift.integration.scribe.drift.DriftLogEntry;

import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;

@ThriftService("mismatch")
public class MismatchServiceHandler
{
    @ThriftMethod
    public int extraClientArgs(int value)
    {
        return value;
    }

    @ThriftMethod
    public int extraServerArgs(
            boolean booleanArg,
            int intArg,
            long longArg,
            double doubleArg,
            String stringArg,
            byte[] binaryArg,
            DriftLogEntry structArg,
            Integer integerArg,
            List<Integer> listInteger,
            List<String> listString,
            OptionalInt optionalInt,
            OptionalLong optionalLong,
            OptionalDouble optionalDouble,
            Optional<String> optionalString,
            Optional<DriftLogEntry> optionalStruct,
            Optional<List<Integer>> optionalListInteger,
            Optional<List<String>> optionalListString)
    {
        assertFalse(booleanArg);
        assertEquals(intArg, 0);
        assertEquals(longArg, 0);
        assertEquals(doubleArg, 0.0);
        assertNull(stringArg);
        assertNull(binaryArg);
        assertNull(structArg);
        assertNull(integerArg);
        assertNull(listInteger);
        assertNull(listString);
        assertFalse(optionalString.isPresent());
        assertFalse(optionalInt.isPresent());
        assertFalse(optionalLong.isPresent());
        assertFalse(optionalDouble.isPresent());
        assertFalse(optionalStruct.isPresent());
        assertFalse(optionalListInteger.isPresent());
        assertFalse(optionalListString.isPresent());

        return 42;
    }
}
