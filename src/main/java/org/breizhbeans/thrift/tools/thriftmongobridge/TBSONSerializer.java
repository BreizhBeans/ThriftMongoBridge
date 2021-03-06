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
package org.breizhbeans.thrift.tools.thriftmongobridge;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocolFactory;

import com.mongodb.DBObject;
import org.breizhbeans.thrift.tools.thriftmongobridge.protocol.TBSONUnstackedProtocol;

 public class TBSONSerializer {

  private TBSONUnstackedProtocol protocol_;

  public TBSONSerializer() {
    this(new TBSONUnstackedProtocol.Factory());
  }

  private TBSONSerializer(TProtocolFactory protocolFactory) {
    protocol_ = (TBSONUnstackedProtocol) protocolFactory.getProtocol(null);
  }

  public DBObject serialize(TBase base) throws TException {
    protocol_.setBaseObject(base);
    base.write(protocol_);
    return protocol_.getDBObject();
  }
}

