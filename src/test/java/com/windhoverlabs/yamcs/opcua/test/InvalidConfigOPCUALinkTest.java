package com.windhoverlabs.yamcs.opcua.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.yamcs.client.processor.ProcessorClient;
import org.yamcs.protobuf.Pvalue.AcquisitionStatus;
import org.yamcs.protobuf.Pvalue.ParameterValue;
import org.yamcs.protobuf.Yamcs.NamedObjectId;
import org.yamcs.protobuf.Yamcs.Value;
import org.yamcs.protobuf.Yamcs.Value.Type;

/** Unit test for simple App. */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class InvalidConfigOPCUALinkTest extends AbstractInvalidConfigOPCUAIntegrationTest {

  private ProcessorClient processorClient;

  private static ExampleServer opcuaServer = null;

  @BeforeEach
  public void prepare() {
    //    processorClient = yamcsClient.createProcessorClient(yamcsInstance, "realtime");
  }

  private void checkPvals(
      int expectedNumParams, List<ParameterValue> pvals, RefMdbPacketGenerator packetProvider) {
    assertNotNull(pvals);
    assertEquals(expectedNumParams, pvals.size());

    for (ParameterValue p : pvals) {
      // Due to unit tests waiting for certain events, it's quite plausible to
      // receive expired parameter values.
      assertTrue(
          AcquisitionStatus.ACQUIRED == p.getAcquisitionStatus()
              || AcquisitionStatus.EXPIRED == p.getAcquisitionStatus());
      Value praw = p.getRawValue();
      assertNotNull(praw);
      Value peng = p.getEngValue();
      NamedObjectId id = p.getId();
      if ("/REFMDB/SUBSYS1/IntegerPara1_1_6".equals(id.getName())
          || "para6alias".equals(p.getId().getName())) {
        assertEquals(Type.UINT32, praw.getType());
        assertEquals(packetProvider.pIntegerPara1_1_6, praw.getUint32Value());

        assertEquals(Type.UINT32, peng.getType());
        assertEquals(packetProvider.pIntegerPara1_1_6, peng.getUint32Value());

      } else if ("/REFMDB/SUBSYS1/IntegerPara1_1_7".equals(id.getName())) {
        assertEquals(Type.UINT32, praw.getType());
        assertEquals(packetProvider.pIntegerPara1_1_7, praw.getUint32Value());

        assertEquals(Type.UINT32, peng.getType());
        assertEquals(packetProvider.pIntegerPara1_1_7, peng.getUint32Value());
      } else {
        fail("Unknown parameter '" + id + "'");
      }
    }
  }

  @Test
  @Order(1)
  public void testOPCUALink() throws Exception {
    //    super.before();
    com.google.common.util.concurrent.UncheckedExecutionException exception =
        assertThrows(
            com.google.common.util.concurrent.UncheckedExecutionException.class,
            () -> super.before());

    //    assertEquals("Missing required argument endpoint_url", exception.getMessage());
  }
}
