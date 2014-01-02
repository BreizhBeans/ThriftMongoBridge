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
import com.mongodb.util.JSON;
import com.mongodb.util.JSONParseException;
import org.apache.thrift.TBase;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TSimpleJSONProtocol;
import org.breizhbeans.thrift.tools.thriftmongobridge.TBSONSerializer;
import org.breizhbeans.thrift.tools.thriftmongobridge.ThriftMongoHelper;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestSerializer {

    @Test
    public void testTBSONObjectListEnum() throws Exception {
        TBSONSerializer tbsonSerializer = new TBSONSerializer();

        ThriftEnumList thriftEnumList = new ThriftEnumList();

        thriftEnumList.addToThriftEnums(ThriftEnum.VALUE_ONE);
        thriftEnumList.addToThriftEnums(ThriftEnum.VALUE_TWO);

        // serialize into DBObject
        DBObject dbObject = tbsonSerializer.serialize(thriftEnumList);

        assertEquals(thriftEnumList, dbObject);
    }
	
	@Test 
	public void testTBSONObjectList() throws Exception {
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

		assertEquals(bsonObjectList, dbObject);				
	}
	
	@Test
	public void testTBSONComposite() throws Exception {
		TBSONSerializer tbsonSerializer = new TBSONSerializer();
		
		BSonThrift inputBsonThrift = new BSonThrift();
		inputBsonThrift.setOneString("string value");
		
		BSonComposite bsonComposite = new BSonComposite();
		bsonComposite.setSimpleString("simple string");
		bsonComposite.setBsonThrift(inputBsonThrift);
		
		// serialize into DBObject
		DBObject dbObject = tbsonSerializer.serialize(bsonComposite);

		assertEquals(bsonComposite, dbObject);		
	}

	@Test
	public void testTBSONCompositeNLevel() throws Exception {
		TBSONSerializer tbsonSerializer = new TBSONSerializer();
		
		AnotherThrift anotherThrift = new AnotherThrift();
		anotherThrift.setAnotherString("str1");
		anotherThrift.setAnotherInteger(32);
		
		BSonThrift inputBsonThrift = new BSonThrift();
		inputBsonThrift.setOneString("string value");
		inputBsonThrift.setAnotherThrift(anotherThrift);
		
		BSonComposite bsonComposite = new BSonComposite();
		bsonComposite.setSimpleString("simple string");
		bsonComposite.setBsonThrift(inputBsonThrift);
		
		// serialize into DBObject
		DBObject dbObject = tbsonSerializer.serialize(bsonComposite);

		assertEquals(bsonComposite, dbObject);		
	}	
	
	@Test
	public void testTBSONSerializerList() throws Exception {
		TBSONSerializer tbsonSerializer = new TBSONSerializer();

		BSonThrift inputBsonThrift = new BSonThrift();
		inputBsonThrift.setOneString("string value");
		inputBsonThrift.setOneBigInteger(123456);

		// A list (like Java list)
		inputBsonThrift.addToOneStringList("toto1");
		inputBsonThrift.addToOneStringList("toto1");
		inputBsonThrift.addToOneStringList("toto3");
		
		// A set (like Java Set)
		inputBsonThrift.addToOneStringSet("set3");		
		inputBsonThrift.addToOneStringSet("set1");
		inputBsonThrift.addToOneStringSet("set2");
		inputBsonThrift.addToOneStringSet("set1");

		// serialize into DBObject
		DBObject dbObject = tbsonSerializer.serialize(inputBsonThrift);

		assertEquals(inputBsonThrift, dbObject);
	}

    @Test
    public void testTBSONSerializerListDouble() throws Exception {
        TBSONSerializer tbsonSerializer = new TBSONSerializer();

        BSonThrift inputBsonThrift = new BSonThrift();

        inputBsonThrift.addToOneDoubleList((double)8.324);
        inputBsonThrift.addToOneDoubleList((double)8.327);

        // serialize into DBObject
        DBObject dbObject = tbsonSerializer.serialize(inputBsonThrift);

        assertEquals(inputBsonThrift, dbObject);
    }

	@Test
	public void testTBSONSerializerMapStringString() throws Exception {
		TBSONSerializer tbsonSerializer = new TBSONSerializer();

		BSonThrift inputBsonThrift = new BSonThrift();
		inputBsonThrift.setOneString("string value");
		inputBsonThrift.setOneBigInteger(123456);


		// A Map like Java Map
		Map<String,String> oneStringMap = new HashMap<String,String>();
		oneStringMap.put("key1", "value1");
		oneStringMap.put("key2", "value2");
		inputBsonThrift.setOneStringMap(oneStringMap);
		// serialize into DBObject
		DBObject dbObject = tbsonSerializer.serialize(inputBsonThrift);

		assertEquals(inputBsonThrift, dbObject);
	}	

	@Test
	public void testTBSONSerializerMapStringObject() throws Exception {
		TBSONSerializer tbsonSerializer = new TBSONSerializer();

		BSonThrift inputBsonThrift = new BSonThrift();
		inputBsonThrift.setOneString("string value");
		inputBsonThrift.setOneBigInteger(123456);
		
		// A Map like Java Map
		Map<String,AnotherThrift> oneMap = new HashMap<String,AnotherThrift>();
		oneMap.put("key1", new AnotherThrift("value1", 1));
		oneMap.put("key2", new AnotherThrift("value2", 2));
		inputBsonThrift.setOneObjectMapAsValue(oneMap);
		
		// serialize into DBObject
		DBObject dbObject = tbsonSerializer.serialize(inputBsonThrift);

		assertEquals(inputBsonThrift, dbObject);
	}

    @Test
    public void testTBSonSerializerMapEnumString() throws Exception {
        TBSONSerializer tbsonSerializer = new TBSONSerializer();

        BSonThrift inputBSonThrift = new BSonThrift();
        inputBSonThrift.putToMapEnum( ThriftEnum.VALUE_ONE, "test");

        // serialize into DBObject
        DBObject dbObject = tbsonSerializer.serialize(inputBSonThrift);

        assertEquals(inputBSonThrift, dbObject);
    }


	// An map<object,object) is writable in thrift but not in JSON
	@Test(expected=JSONParseException.class)
	public void testTBSONSerializerMapObjectObject() throws Exception {
		TBSONSerializer tbsonSerializer = new TBSONSerializer();

		BSonThrift inputBsonThrift = new BSonThrift();
		inputBsonThrift.setOneString("string value");
		inputBsonThrift.setOneBigInteger(123456);
		
		// A Map like Java Map
		Map<KeyObject,AnotherThrift> oneMap = new HashMap<KeyObject,AnotherThrift>();
		oneMap.put(new KeyObject("key1",1), new AnotherThrift("value1", 1));
		oneMap.put(new KeyObject("key2",2), new AnotherThrift("value2", 2));
		inputBsonThrift.setOneMapObjectKeyObjectValue(oneMap);
		
		// serialize into DBObject
		DBObject dbObject = tbsonSerializer.serialize(inputBsonThrift);

		assertEquals(inputBsonThrift, dbObject);
	}	
	
	
	
	private void assertEquals( final TBase<?,?> thriftObject, final DBObject dbObject ) throws Exception {
		//serialize the thrift object in JSON
		TSerializer tjsonSerializer = new TSerializer(new TSimpleJSONProtocol.Factory());
		byte[] jsonObject = tjsonSerializer.serialize(thriftObject);
		
		// Parse the JSON into DBObject
		DBObject expectedDBObject = (DBObject) JSON.parse(new String(jsonObject));
		
		System.out.println("Thrift source=" + expectedDBObject.toString());
		System.out.println("DB     source=" + dbObject.toString());
		// Are the DBObject equals ?
		Assert.assertEquals(expectedDBObject.toString(), dbObject.toString());
	}
	
	@Test
	public void testSerializeIntegrity() throws Exception {
		BSonThrift inputBsonThrift = new BSonThrift();
		BSonThrift outputBsonThrift = new BSonThrift();

		inputBsonThrift.setOneString("string value");
		inputBsonThrift.setOneBigInteger(123456);
		List<String> oneStringList = new ArrayList<String>();
		oneStringList.add("toto1");
		oneStringList.add("toto2");
		oneStringList.add("toto3");
		inputBsonThrift.setOneStringList(oneStringList);

		DBObject dbObject = ThriftMongoHelper.thrift2DBObject(inputBsonThrift);
		outputBsonThrift = (BSonThrift) ThriftMongoHelper.DBObject2Thrift(dbObject, BSonThrift.class);

		Assert.assertEquals(inputBsonThrift, outputBsonThrift);
	}

    /*
	@Test
	public void testPerfThriftMongoHelper() throws Exception {
		Mongo mongo = new Mongo("localhost", 27017);
		DB db = mongo.getDB("mydb");

		// get a single collection
		DBCollection collection = db.getCollection("dummyColl");

        long startTime = System.currentTimeMillis();
		for (int i = 0; i < 500; i++) {

			AnotherThrift anotherThrift = new AnotherThrift();
			anotherThrift.setAnotherString("str1");
			anotherThrift.setAnotherInteger(32);
			
			BSonThrift inputBsonThrift = new BSonThrift();
			inputBsonThrift.setOneString("string value");
			inputBsonThrift.setAnotherThrift(anotherThrift);
			Map<String,AnotherThrift> oneObjectMapAsValue = new HashMap<String, AnotherThrift>();
			Map<String,String> oneStringMap = new HashMap<String, String>();
			for (int j =0; j < 500; j++ ) {
				inputBsonThrift.addToOneStringList("mylistIsWonderfull"+j);				
				inputBsonThrift.addToOneStringSet("mySetIsWonderfull"+j);
				
				oneObjectMapAsValue.put("simple key" + j, new AnotherThrift("str in an object" + j, j));
				oneStringMap.put("too simple key" + j, "string as value" + j);
			}

			inputBsonThrift.setOneObjectMapAsValue(oneObjectMapAsValue);
			inputBsonThrift.setOneStringMap(oneStringMap);
			
			BSonComposite bsonComposite = new BSonComposite();
			bsonComposite.setSimpleString("simple string");
			bsonComposite.setBsonThrift(inputBsonThrift);
			

			DBObject dbObject = ThriftMongoHelper.thrift2DBObject(bsonComposite);

			// Put the document with the thrift introspection and the binary
			collection.insert(dbObject);

		}
        long endTime = System.currentTimeMillis();
        System.out.println("serialisation  time=" + (endTime - startTime));

        startTime = System.currentTimeMillis();
		DBCursor cursorDoc = collection.find();
		while (cursorDoc.hasNext()) {
			DBObject dbObject = cursorDoc.next();


			BSonComposite thirftObject = (BSonComposite) ThriftMongoHelper.DBObject2Thrift(dbObject);

			//System.out.println("deserialisation nano time=" + (endTime - startTime));
		}
        endTime = System.currentTimeMillis();
        System.out.println("deserialisation time=" + (endTime - startTime));

		collection.remove(new BasicDBObject());
	}
	*/
}
