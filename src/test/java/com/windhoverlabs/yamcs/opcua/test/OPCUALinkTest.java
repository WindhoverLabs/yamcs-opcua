package com.windhoverlabs.yamcs.opcua.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Unit test for simple App. */
public class OPCUALinkTest extends AbstractIntegrationTest {

  String yamcsInstance2 = "IntegrationTest";

  @BeforeAll
  public static void initOPCUAServer() throws Exception {
    //    setupYamcs();
    //	  FIXME:Add dummy server to run tests
  }

  @Test
  public void testOPCUALink() {
    assertEquals(true, true);
  }
}
