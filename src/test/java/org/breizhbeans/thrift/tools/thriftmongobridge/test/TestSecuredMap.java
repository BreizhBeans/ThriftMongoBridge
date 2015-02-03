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

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.apache.commons.codec.binary.Hex;
import org.breizhbeans.thrift.tools.thriftmongobridge.TBSONDeserializer;
import org.breizhbeans.thrift.tools.thriftmongobridge.TBSONSerializer;
import org.breizhbeans.thrift.tools.thriftmongobridge.protocol.TBSONUnstackedProtocol;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestSecuredMap {

  @Before
  public void setup() {
    TBSONUnstackedProtocol.resetSecuredWrapper();
  }

  @Test
  public void testSerializeProtectedMapValue() throws Exception {
    TBSONUnstackedProtocol.addSecuredWrapper(new JUnitSecuredWrapper());
    TBSONUnstackedProtocol.getSecuredWrapper().secureThriftFields(BSonThrift.class, true, BSonThrift._Fields.ONE_STRING_MAP);

    TBSONSerializer tbsonSerializer = new TBSONSerializer();

    BSonThrift bSonThrift = new BSonThrift();

    bSonThrift.putToOneStringMap("key1", "value1");
    bSonThrift.putToOneStringMap("key2", "value2");

    // serialize into DBObject
    DBObject dbObject = tbsonSerializer.serialize(bSonThrift);

    // the expected behavior is hash the value an preserve the key
    // the hashed value go into the securedWrap
    Long value1HashHash = TBSONUnstackedProtocol.getSecuredWrapper().digest64("value1".getBytes());
    Long value2HashHash = TBSONUnstackedProtocol.getSecuredWrapper().digest64("value2".getBytes());

    // the hash is present
    assert ((BasicDBObject)dbObject.get(BSonThrift._Fields.ONE_STRING_MAP.getFieldName()))
        .get("key1")
        .equals(value1HashHash);

    assert ((BasicDBObject)dbObject.get(BSonThrift._Fields.ONE_STRING_MAP.getFieldName()))
        .get("key2")
        .equals(value2HashHash);

    // the protected field is crypted
    byte[] dataValue1 = TBSONUnstackedProtocol.getSecuredWrapper().cipher("value1".getBytes());
    byte[] dataValue2 = TBSONUnstackedProtocol.getSecuredWrapper().cipher("value2".getBytes());

    String securedFieldId = Short.toString(BSonThrift._Fields.ONE_STRING_MAP.getThriftFieldId());
    DBObject securedMap = (DBObject)((DBObject)dbObject.get("securedwrap")).get(securedFieldId);
    assert securedMap.get("key1").equals(Hex.encodeHexString(dataValue1));
    assert securedMap.get("key2").equals(Hex.encodeHexString(dataValue2));
  }

  @Test
  public void testDeserializeProtectedMap() throws Exception {
    TBSONUnstackedProtocol.addSecuredWrapper(new JUnitSecuredWrapper());
    TBSONUnstackedProtocol.getSecuredWrapper().secureThriftFields(BSonThrift.class, true, BSonThrift._Fields.ONE_STRING_MAP);

    TBSONSerializer tbsonSerializer = new TBSONSerializer();
    TBSONDeserializer tbsonDeserializer = new TBSONDeserializer();

    BSonThrift bSonThrift = new BSonThrift();

    bSonThrift.putToOneStringMap("key1", "value1");
    bSonThrift.putToOneStringMap("key2", "value2");

    // serialize into DBObject
    DBObject dbObject = tbsonSerializer.serialize(bSonThrift);

    // deserialize it
    BSonThrift actualBSonThrift = new BSonThrift();
    tbsonDeserializer.deserialize(actualBSonThrift, dbObject);

    // assert the objects sould be the sames
    assert bSonThrift.equals(actualBSonThrift);

  }
}
