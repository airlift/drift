/*
 * Copyright (C) 2012 ${project.organization.name}
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
package io.airlift.drift.codec.metadata;

import com.google.common.collect.ImmutableMap;
import io.airlift.drift.annotations.ThriftEnum;
import io.airlift.drift.annotations.ThriftEnumValue;
import io.airlift.drift.codec.Letter;
import org.testng.annotations.Test;

import static io.airlift.drift.codec.metadata.ThriftEnumMetadataBuilder.thriftEnumMetadata;
import static org.testng.Assert.assertEquals;

public class TestThriftEnumMetadata
{
    @Test
    public void testValid()
    {
        ThriftEnumMetadata<Letter> metadata = thriftEnumMetadata(Letter.class);
        assertEquals(metadata.getEnumClass(), Letter.class);
        assertEquals(metadata.getEnumName(), "Letter");
        assertEquals(metadata.getByEnumConstant(), ImmutableMap.<Letter, Integer>builder()
                .put(Letter.A, 65)
                .put(Letter.B, 66)
                .put(Letter.C, 67)
                .put(Letter.D, 68)
                .build());
        assertEquals(metadata.getByEnumValue(), ImmutableMap.<Integer, Letter>builder()
                .put(65, Letter.A)
                .put(66, Letter.B)
                .put(67, Letter.C)
                .put(68, Letter.D)
                .build());
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Enum class .*MissingEnumAnnotation is not annotated with @ThriftEnum")
    public void testMissingEnumAnnotation()
    {
        thriftEnumMetadata(MissingEnumAnnotation.class);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Enum class .*MissingValueMethod must have a method annotated with @ThriftEnumValue")
    public void testMissingValueMethod()
    {
        thriftEnumMetadata(MissingValueMethod.class);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Enum class .*MultipleValueMethods has multiple methods annotated with @ThriftEnumValue")
    public void testMultipleValueMethods()
    {
        thriftEnumMetadata(MultipleValueMethods.class);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Enum class .*DuplicateValues returned duplicate enum values: 42")
    public void testDuplicateValues()
    {
        thriftEnumMetadata(DuplicateValues.class);
    }

    public enum MissingEnumAnnotation
    {
        FOO;

        @ThriftEnumValue
        public int value()
        {
            return 42;
        }
    }

    @ThriftEnum
    public enum MissingValueMethod
    {
        FOO
    }

    @ThriftEnum
    public enum MultipleValueMethods
    {
        FOO;

        @ThriftEnumValue
        public int value1()
        {
            return 1;
        }

        @ThriftEnumValue
        public int value2()
        {
            return 2;
        }
    }

    @ThriftEnum
    public enum DuplicateValues
    {
        FOO, BAR;

        @ThriftEnumValue
        public int value()
        {
            return 42;
        }
    }
}
