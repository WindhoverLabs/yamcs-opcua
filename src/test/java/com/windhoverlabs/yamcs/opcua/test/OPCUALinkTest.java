package com.windhoverlabs.yamcs.opcua.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.yamcs.client.ClientException;

/** Unit test for simple App. */
public class OPCUALinkTest extends AbstractIntegrationTest {

  String yamcsInstance2 = "IntegrationTest";
  String test = "";

  @BeforeEach
  @Override
  public void before() throws ClientException {
    System.out.println("setupYamcs*************call");
    super.before();

    try {
      //      setupYamcs();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    //	  FIXME:Add dummy server to run tests
    //    test = "testVar";
  }

  @Test
  public void testOPCUALink() {
    System.out.println("setupYamcs*************call3");
    assertEquals(test, "");
  }

  @Test
  public void testOPCUALink2() {
    System.out.println("setupYamcs*************call4");
    assertEquals(test, "");
  }
}
