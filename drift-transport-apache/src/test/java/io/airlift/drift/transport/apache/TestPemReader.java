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
package io.airlift.drift.transport.apache;

import org.testng.annotations.Test;

import java.io.File;
import java.net.URL;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestPemReader
{
    @Test
    public void testLoadKeyStore()
            throws Exception
    {
        KeyStore keyStore = PemReader.loadKeyStore(getResourceFile("rsa.crt"), getResourceFile("rsa.key"), Optional.empty());
        assertCertificateChain(keyStore);
        assertNotNull(keyStore.getKey("key", new char[0]));
        assertNotNull(keyStore.getCertificate("key"));
    }

    @Test
    public void loadTrustStore()
            throws Exception
    {
        KeyStore keyStore = PemReader.loadTrustStore(getResourceFile("rsa.crt"));
        assertCertificateChain(keyStore);
    }

    private static void assertCertificateChain(KeyStore keyStore)
            throws KeyStoreException
    {
        ArrayList<String> aliases = Collections.list(keyStore.aliases());
        assertEquals(aliases.size(), 1);
        assertNotNull(keyStore.getCertificate(aliases.get(0)));
    }

    private static File getResourceFile(String name)
    {
        URL resource = TestPemReader.class.getClassLoader().getResource(name);
        if (resource == null) {
            throw new IllegalArgumentException("Resource not found " + name);
        }
        return new File(resource.getFile());
    }
}
