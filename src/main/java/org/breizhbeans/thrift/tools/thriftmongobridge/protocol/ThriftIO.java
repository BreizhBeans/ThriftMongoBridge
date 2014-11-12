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

import com.mongodb.DBObject;
import org.apache.thrift.TBase;

import java.util.Iterator;
import java.util.Map;
import java.util.Stack;

public class ThriftIO {

  public ThriftIO(Class<? extends TBase> thriftClass, DBObject dbObject) {
    this(thriftClass, dbObject, null, false, false);
  }

  public ThriftIO(Class<? extends TBase> thriftClass, DBObject dbObject, DBObject securedDBObject ) {
    this(thriftClass, dbObject, securedDBObject, false, false);
  }

  public ThriftIO(Class<? extends TBase> thriftClass, DBObject dbObject, Stack<ThriftFieldMetadata> fieldsStack) {
    this(thriftClass, dbObject, null, false, false);
    this.fieldsStack = fieldsStack;
  }

  public ThriftIO(Class<? extends TBase> thriftClass, DBObject dbObject, Boolean map) {
    this(thriftClass, dbObject, null, map, false);
  }

  public ThriftIO(Class<? extends TBase> thriftClass, DBObject dbObject, DBObject securedDBObject, Boolean map, Boolean list) {
    this.thriftClass=thriftClass;
    this.mongoIO = dbObject;
    this.securedMongoIO = securedDBObject;
    this.map = map;
    this.list = list;
  }

  public Class<? extends TBase> thriftClass;
  public DBObject mongoIO;
  public DBObject securedMongoIO;

  public Boolean map;
  public Boolean list;
  public String key;
  public int containerIndex = 0;

  public Iterator<Map.Entry<String, Object>> mapIterator;
  public Map.Entry<String, Object> mapEntry = null;
  public Stack<ThriftFieldMetadata> fieldsStack;

}
