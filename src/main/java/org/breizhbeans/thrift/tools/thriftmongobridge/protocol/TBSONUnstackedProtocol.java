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

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.apache.commons.codec.binary.Hex;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.*;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.TTransport;
import org.breizhbeans.thrift.tools.thriftmongobridge.secured.TBSONSecuredWrapper;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.*;

public class TBSONUnstackedProtocol extends TProtocol {

  private static ThreadLocal<Stack<ThriftFieldMetadata>> threadSafeFieldsStack = new ThreadLocal<>();

  // Contains the Thrift object + DBObject -> IO because I code it the Google IO's day :D
  private static ThreadLocal<Stack<ThriftIO>> threadSafeSIOStack = new ThreadLocal<>();

  private static ThreadLocal<DBObject> threadSafeDBObject = new ThreadLocal<>();
  private static ThreadLocal<TBase<?, ?>> threadSafeTBase = new ThreadLocal<>();

  // Fields filter
  private static ThreadLocal<Map<Class<?>,List<Short>>> threadSafeFieldIdsFilter = new ThreadLocal<>();

  //Cache objects Contains the metadata map the TField id Enums
  private static ThreadLocal<Map<Class<?>,Map<Short, ThriftFieldMetadata>>> threadSafeTFields = new ThreadLocal<>();
  private static ThreadLocal<Map<Class<?>,Map<String,org.apache.thrift.TFieldIdEnum>>> threadSafeTFieldsIdEnum  = new ThreadLocal<>();

  private static final TStruct ANONYMOUS_STRUCT = new TStruct();
  private static final ThriftFieldMetadata STOP_THRIFT_FIELD_METADATA = new ThriftFieldMetadata("STOP FIELD", TType.STOP, (short)0);

  private static TBSONSecuredWrapper tbsonSecuredWrapper = new DefaultUnsecuredWrapper();

  // Secured warpper
  public static void addSecuredWrapper(TBSONSecuredWrapper tbsonSecuredWrapper) {
    TBSONUnstackedProtocol.tbsonSecuredWrapper = tbsonSecuredWrapper;
  }

  public static TBSONSecuredWrapper getSecuredWrapper() {
    return TBSONUnstackedProtocol.tbsonSecuredWrapper;
  }

  /**
   * Factory
   */
  public static class Factory implements TProtocolFactory {
    public TProtocol getProtocol(TTransport trans) {
      return new TBSONUnstackedProtocol();
    }
  }

  /**
   * Constructor
   */
  public TBSONUnstackedProtocol() {
    super(null);
  }

  public DBObject getDBObject() {
    return threadSafeDBObject.get();
  }


  public void setDBOject(DBObject dbObject) {
    threadSafeDBObject.set(dbObject);
  }

  public void setBaseObject(TBase<?, ?> base) {
    threadSafeTBase.set(base);
    // clear all remaing stacks
    threadSafeSIOStack.set(null);
    threadSafeFieldsStack.set(null);
  }

  public void setFieldIdsFilter(TBase<?, ?> base, TFieldIdEnum[] fieldIds) {
    base.getClass();
    List<Short> filteredFields = new ArrayList<>();

    for(TFieldIdEnum tFieldIdEnum : fieldIds){
      filteredFields.add(tFieldIdEnum.getThriftFieldId());
    }
    Map<Class<?>,List<Short>> filter = new HashMap<>();
    filter.put(base.getClass(), filteredFields);
    threadSafeFieldIdsFilter.set(filter);
  }

  private Map<Short, ThriftFieldMetadata> getTBaseFields(Class<? extends TBase> tbase) throws TException {
    try {
      // First read if the ThriftFieldMetadata was not already readed
      Map<Class<?>,Map<Short, ThriftFieldMetadata>> thriftFields = threadSafeTFields.get();

      if(thriftFields==null){
        thriftFields = new HashMap<>();
      }

      Map<Short, ThriftFieldMetadata> thriftStructFields = thriftFields.get(tbase);

      // Finded return it
      if(thriftStructFields!=null){
        return thriftStructFields;
      }

      // Load it
      thriftStructFields = new HashMap<>();
      Field metafaField = tbase.getField("metaDataMap");
      Map<?, FieldMetaData> fields = (Map<?, org.apache.thrift.meta_data.FieldMetaData>) metafaField.get(tbase);
      // list all fields
      for (Map.Entry<?, FieldMetaData> entry : fields.entrySet()) {
        TFieldIdEnum field = (TFieldIdEnum) entry.getKey();

        ThriftFieldMetadata tfieldMetadata = new ThriftFieldMetadata();

        // An enum type is deserialized as an I32
        byte type = entry.getValue().valueMetaData.type;
        if (TType.ENUM == type) {
          type = TType.I32;
        }

        TBSONSecuredWrapper.ThriftSecuredField securedField=tbsonSecuredWrapper.getField(tbase, field.getThriftFieldId());

        tfieldMetadata.tfield = new TField(field.getFieldName(), type, field.getThriftFieldId());
        tfieldMetadata.fieldMetaData = entry.getValue();
        tfieldMetadata.tbaseClass = tbase;
        tfieldMetadata.securedFieldMetaData=securedField;
        thriftStructFields.put( field.getThriftFieldId(), tfieldMetadata);
      }

      //store it
      thriftFields.put(tbase, thriftStructFields);
      threadSafeTFields.set(thriftFields);
      return thriftStructFields;
    } catch(Exception exp) {
      throw new TException(exp);
    }
  }


  public org.apache.thrift.TFieldIdEnum getFieldId(Class<? extends TBase> tbase, String fieldName) throws TException {

    try {
      Map<Class<?>,Map<String,org.apache.thrift.TFieldIdEnum>> thriftTFields = threadSafeTFieldsIdEnum.get();

      if(thriftTFields==null){
        thriftTFields = new HashMap<>();
      }

      Map<String,org.apache.thrift.TFieldIdEnum> tfieldIdEnumByName = thriftTFields.get(tbase);

      // Load it in cache
      if(tfieldIdEnumByName==null) {

        Class<?>[] innerClasses = tbase.getClasses();

        for (Class<?> innerClass : innerClasses) {
          if ("_Fields".equals(innerClass.getSimpleName())) {
            Field fieldsByName = innerClass.getDeclaredField("byName");
            fieldsByName.setAccessible(true);

            tfieldIdEnumByName = (Map<String, org.apache.thrift.TFieldIdEnum>) fieldsByName.get(fieldsByName);

            // store in the thread local
            thriftTFields.put(tbase, tfieldIdEnumByName);
            threadSafeTFieldsIdEnum.set(thriftTFields);
          }
        }
      }

      return  tfieldIdEnumByName.get(fieldName);

    } catch (Exception e) {
      throw new TException(e);
    }
  }


  @Override
  public void writeStructBegin(TStruct tStruct) throws TException {
    //System.out.println("writeStructBegin " + tStruct.name);
    Stack<ThriftIO> structClass=threadSafeSIOStack.get();
    if(structClass == null) {
      structClass = new Stack<>();

      Class<? extends TBase> thriftClass = threadSafeTBase.get().getClass();


      if (tbsonSecuredWrapper.isSecured(thriftClass)) {
        structClass.push(new ThriftIO(threadSafeTBase.get().getClass(), new BasicDBObject(), new BasicDBObject()));
      } else {
        structClass.push(new ThriftIO(threadSafeTBase.get().getClass(), new BasicDBObject()));
      }


    } else {
      // The last field stack contains a struct field or a map field
      ThriftFieldMetadata lastField = threadSafeFieldsStack.get().peek();

      switch (lastField.fieldMetaData.valueMetaData.type) {
        case TType.MAP:
          MapMetaData mapMetaData = (MapMetaData) lastField.fieldMetaData.valueMetaData;
          if (mapMetaData.valueMetaData.isStruct()) {
            structClass.push(new ThriftIO(((StructMetaData) mapMetaData.valueMetaData).structClass, new BasicDBObject()));
          }
          break;
        case TType.LIST:
          ListMetaData listMetaData = (ListMetaData) lastField.fieldMetaData.valueMetaData;
          if (listMetaData.elemMetaData.isStruct()) {
            structClass.push(new ThriftIO(((StructMetaData) listMetaData.elemMetaData).structClass, new BasicDBObject()));
          }
          break;
        case TType.SET:
          SetMetaData setMetaData = (SetMetaData) lastField.fieldMetaData.valueMetaData;
          if (setMetaData.elemMetaData.isStruct()) {
            structClass.push(new ThriftIO(((StructMetaData) setMetaData.elemMetaData).structClass, new BasicDBObject()));
          }
          break;
        case TType.STRUCT:
          structClass.push(new ThriftIO(((StructMetaData) lastField.fieldMetaData.valueMetaData).structClass, new BasicDBObject()));
          break;
      }
    }
    threadSafeSIOStack.set(structClass);
  }

  @Override
  public void writeStructEnd() throws TException {
    DBObject outputDbObject = collapseIOStack();

    if(outputDbObject!=null){
      threadSafeDBObject.set(outputDbObject);
    }
  }

  private DBObject collapseIOStack() throws TException {
    // Get the DBObject produced
    Stack<ThriftIO> thriftIOStack = threadSafeSIOStack.get();
    ThriftIO thriftIO = thriftIOStack.pop();

    //System.out.println("Collapse thrift IO : " + thriftIO.toString());

    //Collapse
    if(thriftIOStack.size() > 0) {
      ThriftIO lastThriftIO = thriftIOStack.peek();

      if(lastThriftIO.map) {
        // add {key:value} to the current object and reset the key
        lastThriftIO.mongoIO.put(lastThriftIO.key, thriftIO.mongoIO);
        lastThriftIO.key = null;
      }else if(lastThriftIO.list){
        ((BasicDBList)lastThriftIO.mongoIO).add(thriftIO.mongoIO);
      }else{
        // add {fieldName:value} to the current object
        String fieldName = peekWriteField().tfield.name;
        lastThriftIO.mongoIO.put(fieldName, thriftIO.mongoIO);

        // Dont forget to collapse the secured wrap
        if(thriftIO.securedMongoIO != null) {
          lastThriftIO.mongoIO.put("securedwrap", thriftIO.securedMongoIO);
        }
      }
      return null;
    }

    // not collapsed
    // Dont forget to add the secured wrap
    if(thriftIO.securedMongoIO != null) {
      thriftIO.mongoIO.put("securedwrap", thriftIO.securedMongoIO);
    }
    return thriftIO.mongoIO;
  }

  @Override
  public void writeMessageBegin(TMessage tMessage) throws TException {

  }

  @Override
  public void writeMessageEnd() throws TException {

  }

  @Override
  public void writeFieldBegin(TField tField) throws TException {
    //System.out.println("writeFieldBegin " + tField.name);
    pushWriteField(tField.id);
  }

  @Override
  public void writeFieldEnd() throws TException {

    ThriftFieldMetadata thriftFieldMetadata = popWriteField();
    //System.out.println("writeFieldEnd " + thriftFieldMetadata.tfield.name);
  }

  private ThriftFieldMetadata popWriteField() throws TException {
    Stack<ThriftFieldMetadata> writeStack = threadSafeFieldsStack.get();
    if(writeStack.size()>0) {
      return writeStack.pop();
    }
    return null;
  }

  private void pushWriteField(Class<? extends TBase> tbase, String fieldName) throws TException {
    //get the field ID by name
    org.apache.thrift.TFieldIdEnum tfieldIdEnum = getFieldId(tbase, fieldName);
    if(tfieldIdEnum!=null) {
      //pushWriteField(tfieldIdEnum.getThriftFieldId());
      Stack<ThriftFieldMetadata> writeStack = threadSafeFieldsStack.get();

      // First push
      if(writeStack==null) {
        writeStack = new Stack<>();
      }

      // Take the tbase class at the top of the stack
      ThriftFieldMetadata thriftFieldMetadata = getTBaseFields(tbase).get(tfieldIdEnum.getThriftFieldId());
      writeStack.push(thriftFieldMetadata);

      threadSafeFieldsStack.set(writeStack);
    }
  }

  private void pushWriteField(Short id) throws TException {
    Stack<ThriftFieldMetadata> writeStack = threadSafeFieldsStack.get();

    // First push
    if(writeStack==null) {
      writeStack = new Stack<>();
    }


    ThriftIO thriftIO = peekIOStack();

    // Take the tbase class at the top of the stack
    ThriftFieldMetadata thriftFieldMetadata = getTBaseFields(thriftIO.thriftClass).get(id);
    writeStack.push(thriftFieldMetadata);

    threadSafeFieldsStack.set(writeStack);
  }

  private ThriftFieldMetadata peekWriteField() throws TException {
    Stack<ThriftFieldMetadata> writeStack = threadSafeFieldsStack.get();

    if(writeStack.size()>0) {
      return writeStack.peek();
    }else{
      return STOP_THRIFT_FIELD_METADATA;
    }
  }

  private void pushIOStack(ThriftIO thriftIO) throws TException {
    Stack<ThriftIO> stack = threadSafeSIOStack.get();
    stack.push(thriftIO);
    threadSafeSIOStack.set(stack);
  }

  private ThriftIO peekIOStack() throws TException {
    Stack<ThriftIO> stack = threadSafeSIOStack.get();
    return stack.peek();
  }

  private ThriftIO popIOStack() throws TException {
    Stack<ThriftIO> stack = threadSafeSIOStack.get();
    return stack.pop();
  }




  @Override
  public void writeFieldStop() throws TException {

  }

  @Override
  public void writeMapBegin(TMap tMap) throws TException {
    //System.out.println("writeMapBegin");
    // Replace the BasicDbObject by a DBList
    threadSafeSIOStack.get().push(new ThriftIO(null, new BasicDBObject(), true));
  }

  @Override
  public void writeMapEnd() throws TException {
    //System.out.println("writeMapEnd");
    // Collapse the map into the structure

    // Get the DBObject produced (the map)
    Stack<ThriftIO> thriftIOStack = threadSafeSIOStack.get();
    ThriftIO thriftIO = thriftIOStack.pop();

    // add {fieldName:value} to the current object
    String fieldName = peekWriteField().tfield.name;
    thriftIOStack.peek().mongoIO.put(fieldName, thriftIO.mongoIO);
  }

  @Override
  public void writeListBegin(TList tList) throws TException {
    //System.out.println("writeListBegin");
    // Replace the BasicDbObject by a DBList
    threadSafeSIOStack.get().push(new ThriftIO(null, new BasicDBList(), null, false, true));
  }

  @Override
  public void writeListEnd() throws TException {
    //System.out.println("writeListEnd");
    // collapse the list
    collapseIOStack();
  }

  @Override
  public void writeSetBegin(TSet tSet) throws TException {
    //System.out.println("writeSetBegin");
    // Replace the BasicDbObject by a DBList
    threadSafeSIOStack.get().push(new ThriftIO(null, new BasicDBList(), null, false, true));
  }

  @Override
  public void writeSetEnd() throws TException {
    //System.out.println("writeSetEnd");
    // collapse the list
    collapseIOStack();
  }

  @Override
  public void writeBool(boolean b) throws TException {
    writeByte(b ? (byte) 1 : (byte) 0);
  }

  @Override
  public void writeByte(byte b) throws TException {
    writeNumber((int)b);
  }

  @Override
  public void writeI16(short i) throws TException {
    writeNumber(i);
  }

  @Override
  public void writeI32(int i) throws TException {
    writeNumber(i);
  }

  @Override
  public void writeI64(long l) throws TException {
    writeNumber(l);
  }

  @Override
  public void writeDouble(double v) throws TException {
    writeNumber(v);
  }

  private void writeNumber(Number v) throws TException {
    try {
      ThriftFieldMetadata thriftFieldMetadata = peekWriteField();
      String key = thriftFieldMetadata.tfield.name;
      //System.out.println("write " + key + " " + v);

      ThriftIO thriftIO = threadSafeSIOStack.get().peek();

      //specific map treatment for the map key
      if(thriftIO.map && thriftIO.key==null){
        thriftIO.key = v.toString();
      }else if(thriftIO.map){
        thriftIO.mongoIO.put(thriftIO.key, v);
        thriftIO.key=null;
      }else if(thriftIO.list){
        ((BasicDBList)thriftIO.mongoIO).add(v);
      }else{
        thriftIO.mongoIO.put(key, v);
      }

    } catch (Exception e) {
      throw new TException(e);
    }
  }


  @Override
  public void writeString(String s) throws TException {
    try {
      //System.out.println("write " + key + " " + s);
      ThriftIO thriftIO = threadSafeSIOStack.get().peek();
      // Its a string field
      ThriftFieldMetadata thriftFieldMetadata = peekWriteField();


      byte[] butf8 = s.getBytes("UTF-8");
      Object sutf8 = new String(butf8);


      // write the value (secured)
      if(thriftFieldMetadata.securedFieldMetaData.isSecured()) {
        // crypt the value and add it to the secured field
        String securedField = Hex.encodeHexString(TBSONUnstackedProtocol.tbsonSecuredWrapper.cipher(butf8));
        thriftIO.securedMongoIO.put(Short.toString(thriftFieldMetadata.tfield.id), securedField);

        // compute hash from the value if needed
        if (thriftFieldMetadata.securedFieldMetaData.isHash()) {

        }
      }

      //specific map treatment for the map key
      //keys are unprotected (no sense)
      if(thriftIO.map && thriftIO.key==null) {
        thriftIO.key = (String)sutf8;
        return;
      } else if(thriftFieldMetadata.securedFieldMetaData.isSecured() && thriftFieldMetadata.securedFieldMetaData.isHash()) {
        sutf8 = new Long(TBSONUnstackedProtocol.tbsonSecuredWrapper.digest64(butf8));
      } else if(thriftFieldMetadata.securedFieldMetaData.isSecured() && !thriftFieldMetadata.securedFieldMetaData.isHash()) {
        // reset the uft8 field
        // never store it
        sutf8 = null;
        return;
      }

      // Write the field with a value or an hashed value

      //its a map value
      if(thriftIO.map){
        thriftIO.mongoIO.put(thriftIO.key, sutf8);
        thriftIO.key=null;
        return;
      }

      //its a list or a set
      if(thriftIO.list){
        ((BasicDBList)thriftIO.mongoIO).add(sutf8);
        return;
      }

      thriftIO.mongoIO.put(thriftFieldMetadata.tfield.name, sutf8);

    } catch (Exception e) {
      throw new TException(e);
    }
  }

  @Override
  public void writeBinary(ByteBuffer byteBuffer) throws TException {
    //System.out.println("write " + peekWriteField().tfield.name + byteBuffer);
    try {
      ThriftFieldMetadata thriftFieldMetadata = peekWriteField();
      String key = thriftFieldMetadata.tfield.name;
      //System.out.println("write binary " + key );

      ThriftIO thriftIO = threadSafeSIOStack.get().peek();

      byte[] b = byteBuffer.array();

      //specific map treatment for the map key
      if(thriftIO.map && thriftIO.key==null){
        thriftIO.key = new String(b);
      }else if(thriftIO.map){
        thriftIO.mongoIO.put(thriftIO.key, b);
        thriftIO.key=null;
      }else if(thriftIO.list){
        ((BasicDBList)thriftIO.mongoIO).add(b);
      }else{
        thriftIO.mongoIO.put(key, b);
      }
    } catch (Exception e) {
      throw new TException(e);
    }
  }

  @Override
  public TMessage readMessageBegin() throws TException {
    return null;
  }

  @Override
  public void readMessageEnd() throws TException {

  }

  private Stack<ThriftFieldMetadata> getFieldsStack(Class<? extends TBase> tbase, DBObject dbObject)  throws TException {
    // extract the fields (key from MongoDB)
    Set<String> mongoKeys = dbObject.keySet();

    Stack<ThriftFieldMetadata> writeStack = new Stack<>();

    for (Map.Entry<String, Object> pair : ((BasicDBObject) dbObject).entrySet()) {
      //System.out.println("push field " + mongoKey + " of " + tbase.getSimpleName());
      TFieldIdEnum tfieldIdEnum = getFieldId(tbase, pair.getKey());
      if (tfieldIdEnum != null) {
        ThriftFieldMetadata thriftFieldMetadata = getTBaseFields(tbase).get(tfieldIdEnum.getThriftFieldId());
        writeStack.push(thriftFieldMetadata);
      }
    }

    return writeStack;
  }

  @Override
  public TStruct readStructBegin() throws TException {
    Stack<ThriftIO> structClass=threadSafeSIOStack.get();

    // Init the stack with the TBase deserialisation struct
    if(structClass == null) {
      //Initialisation of the IO stack
      structClass = new Stack<>();

      //System.out.println("readStructBegin " + threadSafeTBase.get().getClass().getSimpleName() );

      // extract the fields (key from MongoDB)
      Stack<ThriftFieldMetadata> fieldsStack = getFieldsStack(threadSafeTBase.get().getClass(), threadSafeDBObject.get());

      structClass.push(new ThriftIO(threadSafeTBase.get().getClass(), threadSafeDBObject.get(), fieldsStack));

      threadSafeSIOStack.set(structClass);
      return ANONYMOUS_STRUCT;
    }

    // The stack is already initialized
    ThriftIO currentIO = peekIOStack();

    // Push a struct from the BasicDBList and increment the index
    if(currentIO.list && currentIO.thriftClass != null ) {
      // Extract the DBObject
      DBObject dbObject = (DBObject)((BasicDBList)currentIO.mongoIO).get(currentIO.containerIndex);
      currentIO.containerIndex++;
      Stack<ThriftFieldMetadata> fieldsStack = getFieldsStack(currentIO.thriftClass, dbObject);
      structClass.push(new ThriftIO(currentIO.thriftClass, dbObject, fieldsStack));
      threadSafeSIOStack.set(structClass);
      return ANONYMOUS_STRUCT;
    }

    if(currentIO.map && currentIO.thriftClass != null) {
      DBObject dbObject = (DBObject) currentIO.mapEntry.getValue();
      currentIO.mapEntry = null;
      Stack<ThriftFieldMetadata> fieldsStack = getFieldsStack(currentIO.thriftClass, dbObject);
      structClass.push(new ThriftIO(currentIO.thriftClass, dbObject, fieldsStack));
      threadSafeSIOStack.set(structClass);
      return ANONYMOUS_STRUCT;
    }

    // the next field is a struct
    // push it on the stack
    ThriftFieldMetadata thriftFieldMetadata = currentIO.fieldsStack.peek();

    switch(thriftFieldMetadata.tfield.type){
      case TType.STRUCT:
        // extract the fields (key from MongoDB)
        // Struct related to the field
        Class<? extends TBase> thriftClass = ((StructMetaData)thriftFieldMetadata.fieldMetaData.valueMetaData).structClass;
        // DbObject related to the field
        DBObject dbObject = (DBObject)peekIOStack().mongoIO.get(thriftFieldMetadata.tfield.name);
        // extraction of the fields
        Stack<ThriftFieldMetadata> fieldsStack = getFieldsStack( thriftClass, dbObject);
        // push the structure
        structClass.push(new ThriftIO(thriftClass, dbObject, fieldsStack));
        break;
    }
    threadSafeSIOStack.set(structClass);
    return ANONYMOUS_STRUCT;
  }


  @Override
  public void readStructEnd() throws TException {
    // only occurs when a Stop field occurs
    ThriftIO thriftIO = popIOStack();
    //System.out.println("readStructEnd " + thriftIO.thriftClass.getSimpleName());
  }

  @Override
  public TField readFieldBegin() throws TException {
    if( peekIOStack().fieldsStack.size() == 0 ) {
      return STOP_THRIFT_FIELD_METADATA.tfield;
    }

    ThriftFieldMetadata currentField = peekIOStack().fieldsStack.peek();

    // partial desarialize
    // IF the field is skipped change the type to void
    Map<Class<?>, List<Short>> filter = threadSafeFieldIdsFilter.get();

    if(filter!=null) {
      List<Short> fieldsFiltered =  filter.get(currentField.tbaseClass);
      if(fieldsFiltered != null && fieldsFiltered.contains(currentField.tfield.id)) {
        return new TField(currentField.tfield.name, TType.VOID, currentField.tfield.id);
      }
    }

    return currentField.tfield;
  }

  @Override
  public void readFieldEnd() throws TException {
    ThriftFieldMetadata lastField = peekIOStack().fieldsStack.pop();
    //System.out.println("readFieldEnd " + lastField.tfield.name);
  }

  @Override
  public TMap readMapBegin() throws TException {
    //System.out.println("readMapBegin");
    // Get the IO Stack
    Stack<ThriftIO> stack = threadSafeSIOStack.get();

    ThriftIO currentIO =stack.peek();
    ThriftFieldMetadata thriftFieldMetadata = currentIO.fieldsStack.peek();

    // field related to the list
    MapMetaData mapMetaData = (MapMetaData) thriftFieldMetadata.fieldMetaData.valueMetaData;

    // extract the DBMap (BasicDbObject)
    BasicDBObject dbObject = (BasicDBObject) currentIO.mongoIO.get(currentIO.fieldsStack.peek().tfield.name);

    ThriftIO thriftListIO = null;
    if (mapMetaData.valueMetaData.isStruct()) {
      thriftListIO = new ThriftIO(((StructMetaData) mapMetaData.valueMetaData).structClass, dbObject, null, true, false);
    } else {
      thriftListIO = new ThriftIO(null, dbObject, null, true, false);
    }
    thriftListIO.mapIterator = dbObject.entrySet().iterator();

    stack.push(thriftListIO);
    threadSafeSIOStack.set(stack);

    return new TMap(TType.STRING, TType.STRING,dbObject.size());
  }

  @Override
  public void readMapEnd() throws TException {
    //System.out.println("readMapEnd");
    popIOStack();
  }

  @Override
  public TList readListBegin() throws TException {
    //System.out.println("readListBegin");

    // Get the IO Stack
    Stack<ThriftIO> stack = threadSafeSIOStack.get();

    ThriftIO currentIO =stack.peek();
    ThriftFieldMetadata thriftFieldMetadata = currentIO.fieldsStack.peek();

    // field related to the list
    ListMetaData listMetaData = (ListMetaData) thriftFieldMetadata.fieldMetaData.valueMetaData;

    // extract the DBList
    BasicDBList dbList = (BasicDBList) currentIO.mongoIO.get(currentIO.fieldsStack.peek().tfield.name);

    ThriftIO thriftListIO = null;
    if (listMetaData.elemMetaData.isStruct()) {
      thriftListIO = new ThriftIO(((StructMetaData) listMetaData.elemMetaData).structClass, dbList, null, false, true);
    } else {
      thriftListIO = new ThriftIO(null, dbList, null, false, true);
    }

    stack.push(thriftListIO);
    threadSafeSIOStack.set(stack);

    return new TList(TType.LIST, dbList.size());
  }

  @Override
  public void readListEnd() throws TException {
    //System.out.println("readListEnd");
    popIOStack();
  }

  @Override
  public TSet readSetBegin() throws TException {
    //System.out.println("readSetBegin");

    // Get the IO Stack
    Stack<ThriftIO> stack = threadSafeSIOStack.get();

    ThriftIO currentIO =stack.peek();
    ThriftFieldMetadata thriftFieldMetadata = currentIO.fieldsStack.peek();

    // field related to the list
    SetMetaData setMetaData = (SetMetaData) thriftFieldMetadata.fieldMetaData.valueMetaData;

    // extract the DBList
    BasicDBList dbList = (BasicDBList) currentIO.mongoIO.get(currentIO.fieldsStack.peek().tfield.name);

    ThriftIO thriftListIO = null;
    if (setMetaData.elemMetaData.isStruct()) {
      thriftListIO = new ThriftIO(((StructMetaData) setMetaData.elemMetaData).structClass, dbList, null, false, true);
    } else {
      thriftListIO = new ThriftIO(null, dbList, null, false, true);
    }

    stack.push(thriftListIO);
    threadSafeSIOStack.set(stack);

    return new TSet(TType.SET, dbList.size());
  }

  @Override
  public void readSetEnd() throws TException {
    //System.out.println("readSetEnd");
    popIOStack();
  }

  public boolean readBool() throws TException {
    return (readByte() == 1);
  }

  public byte readByte() throws TException {
    return ((Number) getCurrentFieldValue(TType.BYTE)).byteValue();
  }

  public short readI16() throws TException {
    return ((Number) getCurrentFieldValue(TType.I16)).shortValue();
  }

  public int readI32() throws TException {
    return ((Number) getCurrentFieldValue(TType.I32)).intValue();
  }

  public long readI64() throws TException {
    return ((Number) getCurrentFieldValue(TType.I64)).longValue();
  }

  public double readDouble() throws TException {
    return ((Number) getCurrentFieldValue(TType.DOUBLE)).doubleValue();
  }

  @Override
  public String readString() throws TException {
    return (String) getCurrentFieldValue(TType.STRING);
  }

  @Override
  public ByteBuffer readBinary() throws TException {
    Object value = getCurrentFieldValue(TType.VOID);
    return ByteBuffer.wrap((byte[]) value);
  }

  public void reset() {
    threadSafeDBObject.remove();
  }


  private Object getCurrentFieldValue(byte ttype)  throws TException {
    ThriftIO thriftIO = peekIOStack();

    Object fieldReaded = null;
    if( thriftIO.list) {
      fieldReaded = ((BasicDBList) thriftIO.mongoIO).get(thriftIO.containerIndex);
      thriftIO.containerIndex++;
    } else if( thriftIO.map && thriftIO.mapEntry == null) {
      // a first read for the key
      thriftIO.mapEntry = thriftIO.mapIterator.next();

      // IF YOU READ A KEY YOU MUST CONVERT THE STRING INTO NUMBER
      switch( ttype ) {
        case TType.BYTE:
          fieldReaded = Byte.parseByte(thriftIO.mapEntry.getKey());
          break;
        case TType.I32:
        case TType.I16:
          fieldReaded = Integer.parseInt(thriftIO.mapEntry.getKey());
          break;
        case TType.I64:
          fieldReaded =  Long.parseLong(thriftIO.mapEntry.getKey());
          break;
        case TType.DOUBLE:
          fieldReaded =  Double.parseDouble(thriftIO.mapEntry.getKey());
          break;
        default:
          fieldReaded = thriftIO.mapEntry.getKey();
      }
    } else if( thriftIO.map && thriftIO.mapEntry != null) {
      // a second read for the value
      fieldReaded = thriftIO.mapEntry.getValue();
      thriftIO.mapEntry = null;
    } else {
      // normal field read
      // if the filed if secured unwrap it first
      ThriftFieldMetadata fieldMetadata = thriftIO.fieldsStack.peek();

      if (fieldMetadata.securedFieldMetaData.isSecured()) {
        byte[] data = TBSONUnstackedProtocol.tbsonSecuredWrapper.decipherSecuredField(fieldMetadata.tfield.id, (DBObject)thriftIO.mongoIO.get("securedwrap"));
        fieldReaded = new String(data);
      } else {
        fieldReaded = thriftIO.mongoIO.get(fieldMetadata.tfield.name);
      }

    }
    return fieldReaded;
  }


}
