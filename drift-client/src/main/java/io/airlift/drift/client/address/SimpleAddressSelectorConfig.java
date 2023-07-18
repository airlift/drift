/*
 * Copyright (C) 2012 Facebook, Inc.
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
package io.airlift.drift.client.address;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import io.airlift.configuration.Config;
import jakarta.validation.constraints.NotNull;

import java.util.List;

import static java.util.stream.Collectors.toList;

public class SimpleAddressSelectorConfig
{
    private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    private List<HostAndPort> addresses;
    private boolean retrySameAddress = true;

    @NotNull
    public List<HostAndPort> getAddresses()
    {
        return addresses;
    }

    @Config("thrift.client.addresses")
    public SimpleAddressSelectorConfig setAddresses(String addresses)
    {
        if (addresses == null) {
            this.addresses = null;
        }
        else {
            this.addresses = ImmutableList.copyOf(SPLITTER.splitToList(addresses).stream()
                    .map(HostAndPort::fromString)
                    .collect(toList()));
        }
        return this;
    }

    public SimpleAddressSelectorConfig setAddressesList(List<HostAndPort> addresses)
    {
        this.addresses = ImmutableList.copyOf(addresses);
        return this;
    }

    public boolean isRetrySameAddress()
    {
        return retrySameAddress;
    }

    @Config("thrift.client.retry-same-address")
    public SimpleAddressSelectorConfig setRetrySameAddress(boolean retrySameAddress)
    {
        this.retrySameAddress = retrySameAddress;
        return this;
    }
}
