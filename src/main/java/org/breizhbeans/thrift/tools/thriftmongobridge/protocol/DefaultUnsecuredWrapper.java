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
package org.breizhbeans.thrift.tools.thriftmongobridge.protocol;


import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.breizhbeans.thrift.tools.thriftmongobridge.secured.TBSONSecuredWrapper;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Default Unsecured Wrapper
 *
 */
public class DefaultUnsecuredWrapper extends TBSONSecuredWrapper {


  public DefaultUnsecuredWrapper() {
  }

  @Override
  public long digest64(byte[] data) throws TException {
    throw new TException("SecureThriftFields digest unvailable on unsecured wrapper");
  }

  @Override
  public byte[] cipher(byte[] data) throws TException {
    throw new TException("SecureThriftFields cipher unvailable on unsecured wrapper");
  }

  @Override
  public byte[] cipher(TBase<?, ?> thriftObject) throws TException {
    throw new TException("SecureThriftFields cipher unvailable on unsecured wrapper");
  }

  @Override
  public byte[] decipher(byte[] data) throws TException {
    throw new TException("SecureThriftFields decipher unvailable on unsecured wrapper");
  }

  @Override
  public TBase<?, ?> decipher(byte[] data, TBase<?, ?> thriftObject) throws TException {
    throw new TException("SecureThriftFields decipher unvailable on unsecured wrapper");
  }

  /*
  @Override
  public void secureThriftFields(String structName, boolean hash, TFieldIdEnum... fields) throws TException {
    throw new TException("SecureThriftFields unvailable on unsecured wrapper");
  }
  */
}