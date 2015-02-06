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

import org.breizhbeans.thrift.tools.thriftmongobridge.protocol.TBSONUnstackedProtocol;
import org.breizhbeans.thrift.tools.thriftmongobridge.secured.UnsupportedTTypeException;
import org.junit.Before;
import org.junit.Test;

public class TestUnsupportedTypes {

  @Before
  public void setup() {
    TBSONUnstackedProtocol.resetSecuredWrapper();
  }

  // primitive types BOOL
  @Test(expected = UnsupportedTTypeException.class)
  public void testUnsupportedTTypeBOOL() throws Exception {
    TBSONUnstackedProtocol.addSecuredWrapper(new JUnitSecuredWrapper());
    TBSONUnstackedProtocol.getSecuredWrapper().secureThriftFields(BSonTTypes.class, false, BSonTTypes._Fields.TTYPE_BOOL);
  }

  // primitive types BYTE
  @Test(expected = UnsupportedTTypeException.class)
  public void testUnsupportedTTypeBYTE() throws Exception {
    TBSONUnstackedProtocol.addSecuredWrapper(new JUnitSecuredWrapper());
    TBSONUnstackedProtocol.getSecuredWrapper().secureThriftFields(BSonTTypes.class, false, BSonTTypes._Fields.TTYPE_BYTE);
  }

  // primitive types DOUBLE
  @Test(expected = UnsupportedTTypeException.class)
  public void testUnsupportedTTypeDOUBLE() throws Exception {
    TBSONUnstackedProtocol.addSecuredWrapper(new JUnitSecuredWrapper());
    TBSONUnstackedProtocol.getSecuredWrapper().secureThriftFields(BSonTTypes.class, false, BSonTTypes._Fields.TTYPE_DOUBLE);
  }

  // primitive types I16
  @Test(expected = UnsupportedTTypeException.class)
  public void testUnsupportedTTypeI16() throws Exception {
    TBSONUnstackedProtocol.addSecuredWrapper(new JUnitSecuredWrapper());
    TBSONUnstackedProtocol.getSecuredWrapper().secureThriftFields(BSonTTypes.class, false, BSonTTypes._Fields.TTYPE_I16);
  }

  // primitive types I32
  @Test(expected = UnsupportedTTypeException.class)
  public void testUnsupportedTTypeI32() throws Exception {
    TBSONUnstackedProtocol.addSecuredWrapper(new JUnitSecuredWrapper());
    TBSONUnstackedProtocol.getSecuredWrapper().secureThriftFields(BSonTTypes.class, false, BSonTTypes._Fields.TTYPE_I32);
  }

  // primitive types I64
  @Test(expected = UnsupportedTTypeException.class)
  public void testUnsupportedTTypeI64() throws Exception {
    TBSONUnstackedProtocol.addSecuredWrapper(new JUnitSecuredWrapper());
    TBSONUnstackedProtocol.getSecuredWrapper().secureThriftFields(BSonTTypes.class, false, BSonTTypes._Fields.TTYPE_I64);
  }

  // primitive types ENUM
  @Test(expected = UnsupportedTTypeException.class)
  public void testUnsupportedTTypeENUM() throws Exception {
    TBSONUnstackedProtocol.addSecuredWrapper(new JUnitSecuredWrapper());
    TBSONUnstackedProtocol.getSecuredWrapper().secureThriftFields(BSonTTypes.class, false, BSonTTypes._Fields.TTYPE_ENUM);
  }

  @Test(expected = UnsupportedTTypeException.class)
  public void testUnsupportedTypeList() throws Exception {
    TBSONUnstackedProtocol.addSecuredWrapper(new JUnitSecuredWrapper());
    TBSONUnstackedProtocol.getSecuredWrapper().secureThriftFields(BSonTTypes.class, false, BSonTTypes._Fields.TTYPE_LIST_DOUBLE);
  }

  @Test(expected = UnsupportedTTypeException.class)
  public void testUnsupportedTypeMap() throws Exception {
    TBSONUnstackedProtocol.addSecuredWrapper(new JUnitSecuredWrapper());
    TBSONUnstackedProtocol.getSecuredWrapper().secureThriftFields(BSonTTypes.class, false, BSonTTypes._Fields.TTYPE_MAP_DOUBLE);
  }

  @Test(expected = UnsupportedTTypeException.class)
  public void testUnsupportedTypeSet() throws Exception {
    TBSONUnstackedProtocol.addSecuredWrapper(new JUnitSecuredWrapper());
    TBSONUnstackedProtocol.getSecuredWrapper().secureThriftFields(BSonTTypes.class, false, BSonTTypes._Fields.TTYPE_SET_DOUBLE);
  }

}
