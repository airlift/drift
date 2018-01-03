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
package io.airlift.drift.client;

import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Optional;

public interface DriftClient<T>
{
    default T get()
    {
        return get(Optional.empty());
    }

    default T get(Optional<String> addressSelectionContext)
    {
        return get(addressSelectionContext, ImmutableMap.of());
    }

    default T get(Map<String, String> headers)
    {
        return get(Optional.empty(), headers);
    }

    T get(Optional<String> addressSelectionContext, Map<String, String> headers);
}
