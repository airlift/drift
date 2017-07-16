/*
 * Copyright (C) 2012 Facebook, Inc.
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
package io.airlift.drift.client;

import io.airlift.drift.TException;
import io.airlift.drift.annotations.ThriftException;
import io.airlift.drift.annotations.ThriftField;
import io.airlift.drift.annotations.ThriftMethod;
import io.airlift.drift.annotations.ThriftStruct;
import io.airlift.drift.codec.metadata.ThriftCatalog;
import io.airlift.drift.codec.metadata.ThriftFieldMetadata;
import io.airlift.drift.codec.metadata.ThriftType;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.testng.Assert.assertEquals;

public class TestThriftMethodMetadata
{
    private static final ThriftCatalog THRIFT_CATALOG = new ThriftCatalog();

    @Test
    public void testValidInferredFieldId()
    {
        ThriftMethodMetadata metadata = extractThriftMethodMetadata("validInferredFieldId");
        assertParameterId(metadata, 1);
    }

    @Test
    public void testValidNormalFieldId()
    {
        ThriftMethodMetadata metadata = extractThriftMethodMetadata("validNormalFieldId");
        assertParameterId(metadata, 4);
    }

    @Test
    public void testValidLegacyFieldId()
    {
        ThriftMethodMetadata metadata = extractThriftMethodMetadata("validLegacyFieldId");
        assertParameterId(metadata, -4);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "isLegacyId (must|should only) be specified.*")
    public void testInvalidNormalFieldIdMarkedLegacy()
    {
        extractThriftMethodMetadata("invalidNormalFieldIdMarkedLegacy");
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "isLegacyId (must|should only) be specified.*")
    public void invalidInferredFieldIdMarkedLegacy()
    {
        extractThriftMethodMetadata("invalidInferredFieldIdMarkedLegacy");
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "isLegacyId (must|should only) be specified.*")
    public void invalidLegacyFieldId()
    {
        extractThriftMethodMetadata("invalidLegacyFieldId");
    }

    @Test
    public void testNoExceptions()
            throws Exception
    {
        assertExceptions("noExceptions");
    }

    @Test
    public void testAnnotatedExceptions()
            throws Exception
    {
        assertExceptions("annotatedExceptions", ExceptionA.class, ExceptionB.class);
    }

    @Test
    public void testInferredException()
            throws Exception
    {
        assertExceptions("inferredException", ExceptionA.class);
    }

    @Test
    public void testInferredExceptionWithTException()
            throws Exception
    {
        assertExceptions("inferredExceptionWithTException", ExceptionA.class);
    }

    @Test
    public void testInferredExceptionWithRuntimeException()
            throws Exception
    {
        assertExceptions("inferredExceptionWithRuntimeException", ExceptionA.class);
    }

    @Test
    public void testInferredExceptionWithRuntimeAndTException()
            throws Exception
    {
        assertExceptions("inferredExceptionWithRuntimeAndTException", ExceptionA.class);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "ThriftMethod annotation must declare exception mapping when more than one custom exception is thrown")
    public void testInvalidInferredException()
            throws Exception
    {
        assertExceptions("invalidInferredException");
    }

    @SafeVarargs
    private static void assertExceptions(String methodName, Class<? extends Exception>... expectedExceptions)
    {
        ThriftMethodMetadata metadata = new ThriftMethodMetadata(getMethod(methodName), new ThriftCatalog());
        Map<Short, Type> actualIdMap = new TreeMap<>();
        Map<Short, Type> expectedIdMap = new TreeMap<>();

        for (Map.Entry<Short, ThriftType> entry : metadata.getExceptions().entrySet()) {
            actualIdMap.put(entry.getKey(), entry.getValue().getJavaType());
        }

        short expectedId = 1;
        for (Class<? extends Exception> expectedException : expectedExceptions) {
            expectedIdMap.put(expectedId, expectedException);
            expectedId++;
        }

        // string comparison produces more useful failure message (and is safe, given the types)
        if (!actualIdMap.equals(expectedIdMap)) {
            assertEquals(actualIdMap.toString(), expectedIdMap.toString());
        }
    }

    private static void assertParameterId(ThriftMethodMetadata metadata, int expectedFieldId)
    {
        List<ThriftFieldMetadata> parameters = metadata.getParameters();
        assertEquals(parameters.size(), 1);
        assertEquals(parameters.get(0).getId(), expectedFieldId);
    }

    private static ThriftMethodMetadata extractThriftMethodMetadata(String methodName)
    {
        return new ThriftMethodMetadata(getMethod(methodName, boolean.class), THRIFT_CATALOG);
    }

    private static Method getMethod(String name, Class<?>... parameterTypes)
    {
        try {
            return TestService.class.getMethod(name, parameterTypes);
        }
        catch (NoSuchMethodException e) {
            throw new AssertionError("Method not found: " + name, e);
        }
    }

    @SuppressWarnings("unused")
    public interface TestService
    {
        @ThriftMethod
        void validInferredFieldId(@ThriftField boolean parameter);

        @ThriftMethod
        void validNormalFieldId(@ThriftField(4) boolean parameter);

        @ThriftMethod
        void validLegacyFieldId(@ThriftField(value = -4, isLegacyId = true) boolean parameter);

        @ThriftMethod
        void invalidNormalFieldIdMarkedLegacy(@ThriftField(value = 5, isLegacyId = true) boolean parameter);

        @ThriftMethod
        void invalidInferredFieldIdMarkedLegacy(@ThriftField(isLegacyId = true) boolean parameter);

        @ThriftMethod
        void invalidLegacyFieldId(@ThriftField(-5) boolean parameter);

        @ThriftMethod
        void noExceptions();

        @ThriftMethod(exception = {@ThriftException(id = 1, type = ExceptionA.class), @ThriftException(id = 2, type = ExceptionB.class)})
        void annotatedExceptions()
                throws ExceptionA, ExceptionB;

        @ThriftMethod
        void inferredException()
                throws ExceptionA;

        @ThriftMethod
        void inferredExceptionWithTException()
                throws ExceptionA, TException;

        @ThriftMethod
        void inferredExceptionWithRuntimeException()
                throws IllegalArgumentException, ExceptionA;

        @ThriftMethod
        void inferredExceptionWithRuntimeAndTException()
                throws IllegalArgumentException, ExceptionA, TException;

        @ThriftMethod(exception = @ThriftException(id = 1, type = ExceptionA.class))
        void invalidInferredException()
                throws ExceptionA, ExceptionB;
    }

    @ThriftStruct
    public static final class ExceptionA
            extends Exception
    {
    }

    @ThriftStruct
    public static final class ExceptionB
            extends Exception
    {
    }
}
