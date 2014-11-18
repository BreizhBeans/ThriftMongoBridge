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

import com.mongodb.*;
import org.breizhbeans.thrift.tools.thriftmongobridge.ThriftMongoHelper;
import org.breizhbeans.thrift.tools.thriftmongobridge.protocol.TBSONUnstackedProtocol;

import java.io.FileInputStream;
import java.security.KeyStore;

/**
 * Simple app example :
 *
 * Firstly : execute WriteKeyStore with 2 arguments : "keystore password", "key password"
 * Secondly : execure SimpleApp with the same arguments
 */
public class SimpleApp {

  public static void main(String[] args) throws Exception {
    // ---------------------------------------------------------------
    // NON SECURED GET ALWAYS PASSWORD'S KEYSTORE FROM A SECURED PLACE
    // get it from a secured console
    String pwd = args[0];
    // get it from a console
    String keyProtectedPassword = args[1];
    // NON SECURED GET ALWAYS PASSWORD'S KEYSTORE FROM A SECURED PLACE
    // ---------------------------------------------------------------

    // load key from the key store
    final KeyStore keyStore = KeyStore.getInstance("JCEKS");
    keyStore.load(new FileInputStream("./keystore/secretkey.keystore"), pwd.toCharArray());

    // Extract the AES key
    KeyStore.PasswordProtection keyPassword = new KeyStore.PasswordProtection(keyProtectedPassword.toCharArray());
    KeyStore.SecretKeyEntry aesKey = (KeyStore.SecretKeyEntry) keyStore.getEntry("aesKey", keyPassword);

    // AES KEY
    byte[] key = aesKey.getSecretKey().getEncoded();

    // Secured bridge initialisation
    // First create the wrapper
    SecuredWrapper securedWrapper = new SecuredWrapper(key);

    // Set the fields protected
    securedWrapper.secureThriftFields(People.class, true, People._Fields.FIRST_NAME);
    securedWrapper.secureThriftFields(People.class, true, People._Fields.LAST_NAME);
    securedWrapper.secureThriftFields(People.class, false, People._Fields.BIRTHDAY);
    securedWrapper.secureThriftFields(People.class, true, People._Fields.EMAIL);

    // Add the wrapper
    TBSONUnstackedProtocol.addSecuredWrapper(securedWrapper);

    // setup mongo
    // and get the collection
    MongoClient mongo = new MongoClient("localhost", 27017);
    DB db = mongo.getDB("example-thrift-mongo");
    DBCollection peoples = db.getCollection("people");


    // write people
    People people = new People();

    people.setFirstName("my secret first name");
    people.setLastName("my secret last name");
    people.setEmail("me@me");
    people.setInceptionDate(System.currentTimeMillis());
    people.setBirthday("1970-01-01");
    people.setLanguage("en");

    // serialize it
    DBObject dbObject = ThriftMongoHelper.thrift2DBObject(people);

    System.out.println("save=" + dbObject.toString());

    // write the document
    peoples.save(dbObject);

    // find document by secured field
    BasicDBObject searchQuery = new BasicDBObject();
    searchQuery.put(People._Fields.FIRST_NAME.getFieldName(), securedWrapper.digest64("my secret first name".getBytes()));

    System.out.println("query=" + searchQuery.toString());

    DBCursor cursor = peoples.find(searchQuery);

    while (cursor.hasNext()) {
      // print secured people
      DBObject dbSecuredObject = cursor.next();
      System.out.println("secured=" +dbSecuredObject);

      // deserialize the secured object
      People peopleDeserialized = (People) ThriftMongoHelper.DBObject2Thrift(dbSecuredObject, People.class);
      System.out.println("unwrapped=" +peopleDeserialized.toString());
    }

    // Bye
    System.out.println("Thrift and mongo rocks :D");
    System.out.println("Destroy the collection");
    peoples.drop();
  }
}
