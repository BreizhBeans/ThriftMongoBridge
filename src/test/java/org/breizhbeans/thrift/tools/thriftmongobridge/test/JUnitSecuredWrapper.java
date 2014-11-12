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
 */
package org.breizhbeans.thrift.tools.thriftmongobridge.test;

import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.breizhbeans.thrift.tools.thriftmongobridge.secured.TBSONSecuredWrapper;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * Simple Example of implementation of the Junit SecuredWrapper
 * Adapt it
 */
public class JUnitSecuredWrapper extends TBSONSecuredWrapper {

  private SecretKeySpec skeyspec;

  // Thrift serialize and deserializers
  private final TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
  private final TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());

  public JUnitSecuredWrapper() throws Exception {
    skeyspec = new SecretKeySpec("strong ke or die".getBytes(), "AES");
  }

  @Override
  public long digest64(byte[] data) {
    // example based on the TOO simple JVM hashCode
    // use an adapted cryptographic hash function related to your collision risk
    long h = 1125899906842597L; // prime

    for(int i=0; i < data.length; i++) {
      h = 31*h + data[i];
    }
    return h;
  }

  @Override
  public byte[] cipher(byte[] data) throws TException{
    try{
      Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
      cipher.init(Cipher.ENCRYPT_MODE, skeyspec);
      return cipher.doFinal(data);
    } catch (Exception exp) {
      throw new TException(exp);
    }
  }

  @Override
  public byte[] decipher(byte[] data) throws TException{
    try {
      Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
      cipher.init(Cipher.DECRYPT_MODE, skeyspec);
      return cipher.doFinal(data);
    } catch (Exception exp) {
      throw new TException(exp);
    }
  }


  @Override
  public byte[] cipher(TBase<?, ?> thriftObject) throws TException{
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
