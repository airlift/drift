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

import com.google.common.base.Splitter;
import com.google.common.net.HostAndPort;
import io.airlift.drift.client.DriftClient;
import io.airlift.drift.transport.Address;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Simple {@link AddressSelector} that chooses randomly among the comma-separated
 * list of addresses from the address selection context passed to {@link DriftClient}.
 * Each address must be a format accepted by {@link HostAndPort}.
 */
public class ContextAddressSelector
        implements AddressSelector<Address>
{
    @Override
    public Optional<Address> selectAddress(Optional<String> addressSelectionContext)
    {
        String context = addressSelectionContext.orElseThrow(() ->
                new IllegalArgumentException("addressSelectionContext must be set"));
        List<String> list = Splitter.on(',').splitToList(context);
        String value = list.get(ThreadLocalRandom.current().nextInt(list.size()));
        HostAndPort address = HostAndPort.fromString(value);
        return Optional.of(() -> address);
    }
}
