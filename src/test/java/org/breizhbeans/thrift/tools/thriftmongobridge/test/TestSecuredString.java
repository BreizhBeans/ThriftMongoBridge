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

import com.mongodb.DBObject;
import org.apache.commons.codec.binary.Hex;
import org.breizhbeans.thrift.tools.thriftmongobridge.TBSONDeserializer;
import org.breizhbeans.thrift.tools.thriftmongobridge.TBSONSerializer;
import org.breizhbeans.thrift.tools.thriftmongobridge.protocol.TBSONProtocol;
import org.breizhbeans.thrift.tools.thriftmongobridge.protocol.TBSONUnstackedProtocol;
import org.junit.Test;

public class TestSecuredString {

  @Test
  public void testSerializeProtectedString() throws Exception {
    TBSONUnstackedProtocol.addSecuredWrapper(new JUnitSecuredWrapper());
    TBSONUnstackedProtocol.getSecuredWrapper().secureThriftFields(BSonObjectList.class, true, BSonObjectList._Fields.SIMPLE_STRING);

    TBSONSerializer tbsonSerializer = new TBSONSerializer();

    AnotherThrift anotherThrift1 = new AnotherThrift();
    anotherThrift1.setAnotherString("str1");
    anotherThrift1.setAnotherInteger(31);

    AnotherThrift anotherThrift2 = new AnotherThrift();
    anotherThrift2.setAnotherString("str2");
    anotherThrift2.setAnotherInteger(32);

    BSonObjectList bsonObjectList = new BSonObjectList();
    bsonObjectList.setSimpleString("simple string");
    bsonObjectList.addToAnotherThrift(anotherThrift1);
    bsonObjectList.addToAnotherThrift(anotherThrift2);

    // serialize into DBObject
    DBObject dbObject = tbsonSerializer.serialize(bsonObjectList);

    Long simpleStringHash = TBSONUnstackedProtocol.getSecuredWrapper().digest64("simple string".getBytes());

    // the hash is present
    assert dbObject.get(BSonObjectList._Fields.SIMPLE_STRING.getFieldName()).equals(simpleStringHash);
    // the protected field is crypted
    byte[] data = TBSONUnstackedProtocol.getSecuredWrapper().cipher("simple string".getBytes());
    String securedFieldData = Hex.encodeHexString(data);

    String securedFieldId = Short.toString(BSonObjectList._Fields.SIMPLE_STRING.getThriftFieldId());
    ((DBObject)dbObject.get("securedwrap")).get(securedFieldId).equals(securedFieldData);
  }

  @Test
  public void testDeserializeProtectedString() throws Exception {
    TBSONUnstackedProtocol.addSecuredWrapper(new JUnitSecuredWrapper());
    TBSONUnstackedProtocol.getSecuredWrapper().secureThriftFields(BSonObjectList.class, true, BSonObjectList._Fields.SIMPLE_STRING);

    TBSONSerializer tbsonSerializer = new TBSONSerializer();
    TBSONDeserializer tbsonDeserializer = new TBSONDeserializer();

    AnotherThrift anotherThrift1 = new AnotherThrift();
    anotherThrift1.setAnotherString("str1");
    anotherThrift1.setAnotherInteger(31);

    AnotherThrift anotherThrift2 = new AnotherThrift();
    anotherThrift2.setAnotherString("str2");
    anotherThrift2.setAnotherInteger(32);

    BSonObjectList bsonObjectList = new BSonObjectList();
    bsonObjectList.setSimpleString("simple string");
    bsonObjectList.addToAnotherThrift(anotherThrift1);
    bsonObjectList.addToAnotherThrift(anotherThrift2);

    // serialize into DBObject
    DBObject dbObject = tbsonSerializer.serialize(bsonObjectList);

    // deserialize the secured object
    BSonObjectList bsonObjectListOut = new BSonObjectList();
    tbsonDeserializer.deserialize(bsonObjectListOut, dbObject);

    assert "simple string".equals(bsonObjectListOut.getSimpleString());
  }

  @Test
  public void testDeserializeProtectedStringWithoutHash() throws Exception {
    TBSONUnstackedProtocol.addSecuredWrapper(new JUnitSecuredWrapper());
    TBSONUnstackedProtocol.getSecuredWrapper().secureThriftFields(BSonObjectList.class, false, BSonObjectList._Fields.SIMPLE_STRING);

    TBSONSerializer tbsonSerializer = new TBSONSerializer();
    TBSONDeserializer tbsonDeserializer = new TBSONDeserializer();

    AnotherThrift anotherThrift1 = new AnotherThrift();
    anotherThrift1.setAnotherString("str1");
    anotherThrift1.setAnotherInteger(31);

    AnotherThrift anotherThrift2 = new AnotherThrift();
    anotherThrift2.setAnotherString("str2");
    anotherThrift2.setAnotherInteger(32);

    BSonObjectList bsonObjectList = new BSonObjectList();
    bsonObjectList.setSimpleString("simple string");
    bsonObjectList.addToAnotherThrift(anotherThrift1);
    bsonObjectList.addToAnotherThrift(anotherThrift2);

    // serialize into DBObject
    DBObject dbObject = tbsonSerializer.serialize(bsonObjectList);

    // deserialize the secured object
    BSonObjectList bsonObjectListOut = new BSonObjectList();
    tbsonDeserializer.deserialize(bsonObjectListOut, dbObject);

    assert "simple string".equals(bsonObjectListOut.getSimpleString());
  }

}
