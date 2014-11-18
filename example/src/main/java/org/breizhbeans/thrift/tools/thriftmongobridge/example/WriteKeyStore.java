/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * Contains some contributions under the Thrift Software License.
 * Please see doc/old-thrift-license.txt in the Thrift distribution for
 * details.
 */
package org.breizhbeans.thrift.tools.thriftmongobridge.example;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.security.KeyStore;

public class WriteKeyStore {


  public static void main(String[] args) throws Exception {
    // ---------------------------------------------------------------
    // NON SECURED GET ALWAYS PASSWORD'S KEYSTORE FROM A SECURED PLACE
    // get it from a console / secured input....
    String keystorePassword = args[0];
    // get it from a console
    String keyProtectedPassword = args[1];
    // NON SECURED GET ALWAYS PASSWORD'S KEYSTORE FROM A SECURED PLACE
    // ---------------------------------------------------------------

    final String keyStoreFile = "./keystore/secretkey.keystore";
    KeyStore keyStore = createKeyStore(keyStoreFile, keystorePassword);

    // generate a secret key for AES encryption
    SecretKey secretKey = KeyGenerator.getInstance("AES").generateKey();

    // store the secret key
    KeyStore.SecretKeyEntry keyStoreEntry = new KeyStore.SecretKeyEntry(secretKey);
    KeyStore.PasswordProtection keyPassword = new KeyStore.PasswordProtection(keyProtectedPassword.toCharArray());
    keyStore.setEntry("aesKey", keyStoreEntry, keyPassword);
    keyStore.store(new FileOutputStream(keyStoreFile), keystorePassword.toCharArray());
  }

  private static KeyStore createKeyStore(String fileName, String pw) throws Exception {
    File file = new File(fileName);

    final KeyStore keyStore = KeyStore.getInstance("JCEKS");
    if (file.exists()) {
      // .keystore file already exists => load it
      keyStore.load(new FileInputStream(file), pw.toCharArray());
    } else {
      // .keystore file not created yet => create it
      keyStore.load(null, null);
      keyStore.store(new FileOutputStream(fileName), pw.toCharArray());
    }

    return keyStore;
  }
}
