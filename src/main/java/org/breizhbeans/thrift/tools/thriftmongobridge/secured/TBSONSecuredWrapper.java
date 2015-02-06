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

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.apache.commons.codec.binary.Hex;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.MapMetaData;
import org.apache.thrift.protocol.TType;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
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

    // get the Field class
    Class<?> fieldClass = null;
    Class<?>[] innerClasses = tbase.getClasses();
    for (Class<?> innerClass : innerClasses) {
      if ("_Fields".equals(innerClass.getSimpleName())) {
        fieldClass = innerClass;
        break;
      }
    }

    // extract _Fields
    Class[] findByNameArgs = new Class[1];
    findByNameArgs[0] = String.class;
    Method findByNameMethod = fieldClass.getMethod("findByName", findByNameArgs);

    // extract metadataMap
    Field metafaField = tbase.getField("metaDataMap");
    Map<?, FieldMetaData> metaDataMap = (Map<?, org.apache.thrift.meta_data.FieldMetaData>) metafaField.get(tbase);

    for(TFieldIdEnum field : fields) {
      // get the _Field instance
      org.apache.thrift.TFieldIdEnum  tfieldEnum = (TFieldIdEnum) findByNameMethod.invoke(null, field.getFieldName());

      // get the matadata
      FieldMetaData fieldMetaData = metaDataMap.get(tfieldEnum);

      // only string are supported
      switch(fieldMetaData.valueMetaData.type) {
        case TType.STRING:
          break;

        case TType.MAP:
          MapMetaData mapMetaData = (MapMetaData) fieldMetaData.valueMetaData;

          if (mapMetaData.valueMetaData.type != TType.STRING) {
            throw new UnsupportedTTypeException("Unsupported secured type - FIELD:" + field.getFieldName() + " TYPE:" + mapMetaData.valueMetaData.type);
          }
          break;
        default:
          throw new UnsupportedTTypeException("Unsupported secured type - FIELD:" + field.getFieldName() + " TYPE:" + fieldMetaData.valueMetaData.type);
      }

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

  public byte[] decipherValue(String hexValue) {
    try {
      byte[] protectedData = Hex.decodeHex(hexValue.toCharArray());
      return decipher(protectedData);
    } catch(Exception exp) {

    }
    return null;
  }


  public DBObject getBSON(String prefix, Class<? extends TBase> tbase, TFieldIdEnum field, String value) throws TException {
    try {
      DBObject bson = new BasicDBObject();

      ThriftSecuredField securedField = getField(tbase, field.getThriftFieldId());

      StringBuilder builder = new StringBuilder();

      if (prefix != null && prefix.length() > 0) {
        builder.append(prefix);
        builder.append(".");
      }

      builder.append(field.getFieldName());

      if (!securedField.isSecured()) {
        bson.put( builder.toString(), value);
      }

      // adds the hash if necessary
      if (securedField.isHash()) {
        bson.put( builder.toString() , digest64(value.getBytes()));
      }

      builder = new StringBuilder();

      if (prefix != null && prefix.length() > 0) {
        builder.append(prefix);
        builder.append(".");
      }
      builder.append("securedwrap.");
      builder.append(Short.toString(field.getThriftFieldId()));
      // adds the wrapped value if necessary
      bson.put( builder.toString(), Hex.encodeHexString(cipher(value.getBytes("UTF-8"))));
      return bson;
    } catch (UnsupportedEncodingException e) {
      throw new TException(e);
    }
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
