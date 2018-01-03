/*
 * Copyright (C) 2013 Facebook, Inc.
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

import io.airlift.drift.annotations.ThriftDocumentation;
import io.airlift.drift.annotations.ThriftField;
import io.airlift.drift.annotations.ThriftMethod;
import io.airlift.drift.annotations.ThriftOrder;

final class ThriftAnnotations
{
    public static final String THRIFT_DOCUMENTATION = ThriftDocumentation.class.getName();
    public static final String THRIFT_ORDER = ThriftOrder.class.getName();
    public static final String THRIFT_FIELD = ThriftField.class.getName();
    public static final String THRIFT_METHOD = ThriftMethod.class.getName();

    public static final String THRIFT_ENUM = "io.airlift.drift.annotations.ThriftEnum";
    public static final String THRIFT_SERVICE = "io.airlift.drift.annotations.ThriftService";
    public static final String THRIFT_STRUCT = "io.airlift.drift.annotations.ThriftStruct";

    public static final String META_SUFFIX = "$DriftMeta";

    private ThriftAnnotations() {}
}
