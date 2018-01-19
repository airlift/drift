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
package io.airlift.drift.client.address;

import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class TestContextAddressSelector
{
    @Test
    public void testSelector()
    {
        ContextAddressSelector selector = new ContextAddressSelector();

        assertThrows(IllegalArgumentException.class, () -> selector.selectAddress(Optional.empty()));

        assertEquals(
                selector.selectAddress(Optional.of("localhost:1234"))
                        .orElseThrow(AssertionError::new)
                        .getHostAndPort(),
                HostAndPort.fromParts("localhost", 1234));

        Set<HostAndPort> selected = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            selected.add(selector.selectAddress(Optional.of("abc:13,xyz:42"))
                    .orElseThrow(AssertionError::new)
                    .getHostAndPort());
        }
        assertEquals(selected, ImmutableSet.of(
                HostAndPort.fromParts("abc", 13),
                HostAndPort.fromParts("xyz", 42)));
    }
}
