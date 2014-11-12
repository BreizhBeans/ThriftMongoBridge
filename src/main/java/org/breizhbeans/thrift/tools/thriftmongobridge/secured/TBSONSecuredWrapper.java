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
package org.breizhbeans.thrift.tools.thriftmongobridge.secured;

import com.mongodb.DBObject;
import org.apache.commons.codec.binary.Hex;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class TBSONSecuredWrapper {


  public class ThriftSecuredField {
    private boolean secured;
    private boolean hash;

    // Default unsecured field
    public ThriftSecuredField() {
      secured = false;
      hash = false;
    }

    public ThriftSecuredField(final boolean secured, final boolean hash) {
      this.secured = secured;
      this.hash = hash;
    }

    public boolean isSecured() {
      return secured;
    }

    public boolean isHash() {
      return hash;
    }
  }

  private final ThriftSecuredField UNSECURED_FIELD = new ThriftSecuredField();

  private ConcurrentHashMap<Class<? extends TBase>, Map<Short, ThriftSecuredField>> securedFields = new ConcurrentHashMap<>();

  public void secureThriftFields(Class<? extends TBase> tbase, boolean hash, TFieldIdEnum... fields) throws Exception {
    Map<Short, ThriftSecuredField> classSecuredFields = securedFields.get(tbase);
    if(classSecuredFields==null) {
      classSecuredFields = new ConcurrentHashMap<>();
    }

    for(TFieldIdEnum field : fields) {
      classSecuredFields.put(field.getThriftFieldId(), new ThriftSecuredField(true, hash));
    }

    securedFields.put(tbase,classSecuredFields);
  }

  public void removeAll() {
    securedFields.clear();
  }

  public void removeSecuredField(Class<? extends TBase> tbase, TFieldIdEnum field) {
    Map<Short, ThriftSecuredField> classSecuredFields = securedFields.get(tbase);
    if(classSecuredFields!=null) {
      classSecuredFields.remove(field.getThriftFieldId());
    }
  }

  public void removeSecuredClass(Class<?> tbase) {
    securedFields.remove(tbase);
  }


  public boolean isSecured(Class<? extends TBase> tbase) {
    Map<Short, ThriftSecuredField> classSecuredFields = securedFields.get(tbase);
    if (classSecuredFields!=null && classSecuredFields.size()>0) {
      return true;
    }
    return false;
  }

  public ThriftSecuredField getField(Class<? extends TBase> tbase, Short id) {
    Map<Short, ThriftSecuredField> classSecuredFields = securedFields.get(tbase);

    if(classSecuredFields==null) {
      return UNSECURED_FIELD;
    }

    ThriftSecuredField securedField = classSecuredFields.get(id);

    if(securedField==null) {
      securedField=UNSECURED_FIELD;
    }
    return securedField;
  }

  public byte[] decipherSecuredField(Short id, DBObject securedWraper) {
    try {
      String key = Short.toString(id);
      String hexValue = (String) securedWraper.get(key);
      byte[] protectedData = Hex.decodeHex(hexValue.toCharArray());

      return decipher(protectedData);
    } catch(Exception exp) {

    }
    return null;
  }

  /**
   * @param data
   * @return  64bits hash value of the input data
   */
  abstract public long digest64(byte[] data) throws TException;


  /**
   * @param data to crypt
   * @return the input data secured
   */
  abstract public byte[] cipher(byte[] data) throws TException;

  /**
   * @param thriftObject to crypt fully
   * @return the input data secured
   */
  abstract public byte[] cipher(TBase<?, ?> thriftObject) throws TException;

  /**
   *
   * @param data coded data
   * @return decoded data
   */
  abstract public byte[] decipher(byte[] data) throws TException;

  /**
   * Decipher a secured thrift object
   * @param data wrapped
   * @param thriftObject new instance of Thrift object
   * @return unwrapped Thrift object
   */
  abstract public TBase<?, ?> decipher(byte[] data, TBase<?, ?> thriftObject) throws TException;
}
