/*
 * Copyright (C) 2017 Facebook
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
package io.airlift.drift.javadoc;

import io.airlift.drift.annotations.ThriftEnum;
import io.airlift.drift.annotations.ThriftService;
import io.airlift.drift.annotations.ThriftStruct;
import org.junit.Test;

import static io.airlift.drift.javadoc.ThriftAnnotations.THRIFT_ENUM;
import static io.airlift.drift.javadoc.ThriftAnnotations.THRIFT_SERVICE;
import static io.airlift.drift.javadoc.ThriftAnnotations.THRIFT_STRUCT;
import static org.assertj.core.api.Assertions.assertThat;

public class TestThriftAnnotations
{
    @Test
    public void testNames()
    {
        assertThat(THRIFT_ENUM).isEqualTo(ThriftEnum.class.getName());
        assertThat(THRIFT_SERVICE).isEqualTo(ThriftService.class.getName());
        assertThat(THRIFT_STRUCT).isEqualTo(ThriftStruct.class.getName());
    }
}
