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

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.breizhbeans.thrift.tools.thriftmongobridge.secured.TBSONSecuredWrapper;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.util.Arrays;

public class SecuredWrapper extends TBSONSecuredWrapper {

  private SecretKeySpec skeyspec;

  // Thrift serialize and deserializers
  private final TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
  private final TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());

  public SecuredWrapper(byte[] aesKey) throws Exception {
    skeyspec = new SecretKeySpec(aesKey, "AES");
  }

  @Override
  public long digest64(byte[] data) {
    // initialize the hash with the AES Hash code
    int seed = Arrays.hashCode(skeyspec.getEncoded());
    HashFunction hf = Hashing.murmur3_128(seed);

    com.google.common.hash.HashCode hashCode = hf.hashBytes(data);

    return hashCode.padToLong();
  }

  @Override
  public byte[] cipher(byte[] data) throws TException {
    try {
      Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
      cipher.init(Cipher.ENCRYPT_MODE, skeyspec);
      return cipher.doFinal(data);
    } catch (Exception exp) {
      throw new TException(exp);
    }
  }

  @Override
  public byte[] decipher(byte[] data) throws TException {
    try {
      Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
      cipher.init(Cipher.DECRYPT_MODE, skeyspec);
      return cipher.doFinal(data);
    } catch (Exception exp) {
      throw new TException(exp);
    }
  }


  @Override
  public byte[] cipher(TBase<?, ?> thriftObject) throws TException {
    byte[] serialized = serializer.serialize(thriftObject);
    return cipher(serialized);
  }


  @Override
  public TBase<?, ?> decipher(byte[] data, TBase<?, ?> thriftObject) throws TException {
    byte[] serialized = decipher(data);
    deserializer.deserialize(thriftObject, serialized);
    return thriftObject;
  }
}
