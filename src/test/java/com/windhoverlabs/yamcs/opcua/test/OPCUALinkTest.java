package com.windhoverlabs.yamcs.opcua.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.gson.JsonObject;
import com.windhoverlabs.yamcs.opcua.OPCUALink;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.yamcs.YamcsServer;
import org.yamcs.client.processor.ProcessorClient;
import org.yamcs.protobuf.Pvalue.AcquisitionStatus;
import org.yamcs.protobuf.Pvalue.ParameterValue;
import org.yamcs.protobuf.Yamcs.NamedObjectId;
import org.yamcs.protobuf.Yamcs.Value;
import org.yamcs.protobuf.Yamcs.Value.Type;
import org.yamcs.tctm.Link.Status;
import org.yamcs.tctm.LinkAction;

/** Unit test for simple App. */
public class OPCUALinkTest extends AbstractOPCUAIntegrationTest {

  String yamcsInstance2 = "IntegrationTest";
  String test = "";

  private ProcessorClient processorClient;

  @BeforeEach
  public void prepare() {

    processorClient = yamcsClient.createProcessorClient(yamcsInstance, "realtime");
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

  //
  //  @Test
  //  public void testMdbParameters() throws InterruptedException, ExecutionException {
  //    //	  FIXME:Add OPCUA-specific test.
  //    var mdbClient = yamcsClient.createMissionDatabaseClient(yamcsInstance);
  //
  //    var refParam = mdbClient.getParameter("/REFMDB/SUBSYS1/IntegerPara1_1_6").get();
  //  }

  @Test
  public void testOPCUALink() throws Exception {
    assertEquals(test, "");

    var mdbClient = yamcsClient.createMissionDatabaseClient(yamcsInstance);

    var refParam =
        mdbClient
            .getParameter(
                "/instruments/tvac/NodeId{ns=2, id=HelloWorld/Dynamic/Boolean}/Variable/Boolean/Value")
            .get(200, TimeUnit.MILLISECONDS);
    assertNotNull(refParam);

    OPCUALink l =
        (OPCUALink)
            YamcsServer.getServer().getInstance(yamcsInstance).getLinkManager().getLink("tm_ocpua");

    assertEquals(l.getLinkStatus(), Status.OK);

    assertEquals(l.connectionStatus(), Status.OK);

    LinkAction action = l.getAction("query_all");

    assertNotNull(action);

    action.execute(l, new JsonObject());

    Thread.sleep(2000);

    l.resetCounters();
    l.doDisable();
    assertEquals(l.connectionStatus(), Status.DISABLED);
    assertEquals("DISABLED", l.getDetailedStatus());
  }
}
