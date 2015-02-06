# [Thrift Mongo Bridge v0.0.5]

You favorite cloud does not support Cassandra, and your business model is full Thrift ?  Don't worry, Thrift Mongo Bridge can save your life !

Why use MongoDB when Spring Data hides Mongo ? Keep the power with Thrift Mongo Bridge !

Thrift Mongo Bridge is a simple Helper for easier the development with Thrift and MongoDB. Created and maintained by [Sebastien Lambour](https://twitter.com/FinistSeb) and [Horacio Gonzalez](https://twitter.com/LostInBrittany)


BreizhBeans Apache Thrift tools by @FinistSeb and @LostInBrittany

## Quick Start

### Serialize Thrift POJOS into MongoDB 
The Helper usage is very simple :

Transform your Thrift objects into Mongo DBObject
* DBObject dbObject = ThriftMongoHelper.thrift2DBObject(inputThriftObject);

Transform your Mongo DBObject into Thrift objects 
* MyThriftObject myThriftObject = (MyThriftObject) ThriftMongoHelper.DBObject2Thrift(dbObject, MyThriftObject.class);

Use your wonderful Thrift objects in your business code, store it directly into MongoDB
What else ?

### Protect Thrift POJOS fields

You can protect any field (hash + cipher) directly.
Look at the project example :

 * the secret is stored in a keystore
 * SecuredWrapper.java implements a 64bits cryptographic hash + AES encryption
 * SimpleApp.java is an example of how secure field, request it, read it back.

## Versioning

Releases will be numbered with the following format:
'<major>.<minor>.<patch>'

And constructed with the following guidelines:
* Breaking backward compatibility or updates the Thrift compiler bumps the major (and resets the minor and patch)
* New additions without breaking backward compatibility bumps the minor ( and resets the patch)
* Bug fixes and changes bumps the patch

## RELEASES

### Future 0.0.6
* Adds a package ThriftMongoDialect for BSON manipulation based en TFields
* complete secured lists

### 0.0.5
* adds support of secured map<string,string>
* fix secured serializer/deserializer bugs
* Add exception on unsupported secured types
  
### 0.0.4
* Support of partial deserialization
* adds secured wrapper for TFields 

### 0.0.3 
* Support of primitive types on key maps (Long, Integer, Boolean, Double, Enum )

### 0.0.2
* First release deployed on Maven Central

### 0.0.1 (Not deployed)

## Bug tracker
Coming if necessary!

## Authors

**Sebastien Lambour**
+ http//twitter.com/FinistSeb
+ http://github.com/slambour

**Horacio Gonzalez**
+ http//twitter.com/LostInBrittany
+ http://github.com/LostInBrittany

## Copyright and license

Copyright 2014 BreizhBeans

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this work except in compliance with the License.
You may obtain a copy of the License in the LICENSE file, or at:

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
