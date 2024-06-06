/****************************************************************************
 *
 *   Copyright (c) 2024 Windhover Labs, L.L.C. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 * 3. Neither the name Windhover Labs nor the names of its
 *    contributors may be used to endorse or promote products derived
 *    from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
 * FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
 * COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS
 * OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED
 * AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 *****************************************************************************/

package com.windhoverlabs.yamcs.opcua;

import static org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned.uint;
import static org.eclipse.milo.opcua.stack.core.util.ConversionUtil.toList;
import static org.yamcs.xtce.NameDescription.qualifiedName;

import com.google.gson.JsonObject;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.logging.Logger;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.api.config.OpcUaClientConfig;
import org.eclipse.milo.opcua.sdk.client.nodes.UaNode;
import org.eclipse.milo.opcua.sdk.client.subscriptions.ManagedDataItem;
import org.eclipse.milo.opcua.sdk.client.subscriptions.ManagedSubscription;
import org.eclipse.milo.opcua.stack.client.DiscoveryClient;
import org.eclipse.milo.opcua.stack.core.AttributeId;
import org.eclipse.milo.opcua.stack.core.Identifiers;
import org.eclipse.milo.opcua.stack.core.UaException;
import org.eclipse.milo.opcua.stack.core.security.DefaultTrustListManager;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
import org.eclipse.milo.opcua.stack.core.types.enumerated.BrowseDirection;
import org.eclipse.milo.opcua.stack.core.types.enumerated.BrowseResultMask;
import org.eclipse.milo.opcua.stack.core.types.enumerated.NodeClass;
import org.eclipse.milo.opcua.stack.core.types.structured.BrowseDescription;
import org.eclipse.milo.opcua.stack.core.types.structured.BrowseResult;
import org.eclipse.milo.opcua.stack.core.types.structured.EndpointDescription;
import org.eclipse.milo.opcua.stack.core.types.structured.ReferenceDescription;
import org.slf4j.LoggerFactory;
import org.yamcs.ConfigurationException;
import org.yamcs.Spec;
import org.yamcs.Spec.OptionType;
import org.yamcs.StandardTupleDefinitions;
import org.yamcs.ValidationException;
import org.yamcs.YConfiguration;
import org.yamcs.YamcsServer;
import org.yamcs.parameter.ParameterValue;
import org.yamcs.parameter.SystemParametersProducer;
import org.yamcs.parameter.SystemParametersService;
import org.yamcs.parameter.Value;
import org.yamcs.protobuf.Yamcs;
import org.yamcs.protobuf.Yamcs.Value.Type;
import org.yamcs.tctm.AbstractLink;
import org.yamcs.tctm.Link;
import org.yamcs.tctm.LinkAction;
import org.yamcs.tctm.PacketInputStream;
import org.yamcs.tctm.ParameterSink;
import org.yamcs.utils.ValueUtility;
import org.yamcs.xtce.AbsoluteTimeParameterType;
import org.yamcs.xtce.AggregateParameterType;
import org.yamcs.xtce.BaseDataType;
import org.yamcs.xtce.BinaryParameterType;
import org.yamcs.xtce.BooleanParameterType;
import org.yamcs.xtce.EnumeratedParameterType;
import org.yamcs.xtce.FloatParameterType;
import org.yamcs.xtce.IntegerParameterType;
import org.yamcs.xtce.Member;
import org.yamcs.xtce.NameDescription;
import org.yamcs.xtce.Parameter;
import org.yamcs.xtce.ParameterType;
import org.yamcs.xtce.StringParameterType;
import org.yamcs.xtce.UnitType;
import org.yamcs.xtce.XtceDb;
import org.yamcs.yarch.DataType;
import org.yamcs.yarch.FileSystemBucket;
import org.yamcs.yarch.Stream;
import org.yamcs.yarch.StreamSubscriber;
import org.yamcs.yarch.Tuple;
import org.yamcs.yarch.TupleDefinition;
import org.yamcs.yarch.YarchDatabase;
import org.yamcs.yarch.YarchDatabaseInstance;

public class OPCUALink extends AbstractLink
    implements Runnable, StreamSubscriber, SystemParametersProducer {

  class NodeIDAttrPair {
    NodeId nodeID;
    AttributeId attrID;

    public NodeIDAttrPair(NodeId newNodeID, AttributeId newAttrID) {
      this.nodeID = newNodeID;
      this.attrID = newAttrID;
    }

    public int hashCode() {
      /**
       * The reason we don't use the hash() function from NodeID class is because it uses the
       * generic hash(), maybe...
       */
      //      int hash = this.nodeID.toParseableString().hashCode() + this.attrID.hashCode();
      int hash = this.nodeID.hashCode() + this.attrID.hashCode();

      return hash;
    }

    public boolean equals(Object obj) {
      return (this.hashCode() == obj.hashCode());
    }
  }

  /* Configuration Defaults */
  static final String STREAM_NAME = "opcua_params";

  private Parameter OPCUAServerStatus;

  /* Configuration Parameters */
  protected long initialDelay;
  protected long period;
  boolean ignoreSpacecraftID;
  boolean ignoreProcessorID;

  /* Internal member attributes. */
  protected FileSystemBucket csvBucket;
  protected YConfiguration packetInputStreamArgs;
  protected PacketInputStream packetInputStream;
  protected Thread thread;

  private String opcuaStreamName;

  static final String DATA_EVENT_CNAME = "data";

  Integer appNameMax;
  Integer eventMsgMax;

  //  FIXME:Make the namespace configurable

  // /yamcs/<server_id>
  private String namespace;
  private String serverId;
  XtceDb mdb;

  Stream opcuaStream;

  ParameterSink paraSink;
  private static TupleDefinition gftdef = StandardTupleDefinitions.PARAMETER.copy();

  private DefaultTrustListManager trustListManager;
  private AggregateParameterType opcuaAttrsType;
  private ManagedSubscription opcuaSubscription;

  private static final Logger internalLogger = Logger.getLogger(OPCUALink.class.getName());

  LinkAction startAction =
      new LinkAction("query_all", "Query All OPCUA Server Data") {
        @Override
        public JsonObject execute(Link link, JsonObject jsonObject) {

          internalLogger.info("Executing query_all action");
          CompletableFuture.supplyAsync(
                  (Supplier<Integer>)
                      () -> {
                        queryAllOPCUAData();

                        return 0;
                      })
              .whenComplete(
                  (vaue, e) -> {
                    internalLogger.info("query_all action Complete");
                  });

          return jsonObject;
        }
      };

  //  NOTE:ALWAYS re-use params as org.yamcs.parameter.ParameterRequestManager.param2RequestMap
  //  uses the object inside a map that was added to the mdb for the very fist time.
  //  If when publishing the PV, we create a new VariableParam object clients will NOT
  //  receive real-time updates as the new object VariableParam inside the new PV won't match the
  // one
  //  inside org.yamcs.parameter.ParameterRequestManager.param2RequestMap since the object hashes
  //  do not match (since VariableParam does not override its hash function).

  private ConcurrentHashMap<NodeIDAttrPair, VariableParam> nodeIDToParamsMap =
      new ConcurrentHashMap<NodeIDAttrPair, VariableParam>();

  private OpcUaClient client;

  protected AtomicLong inCount = new AtomicLong(0);

  @Override
  public Spec getSpec() {
    Spec spec = new Spec();

    /* Define our configuration parameters. */
    spec.addOption("name", OptionType.STRING).withRequired(true);
    spec.addOption("class", OptionType.STRING).withRequired(true);
    spec.addOption("opcua_stream", OptionType.STRING).withRequired(true);

    return spec;
  }

  @Override
  public void init(String yamcsInstance, String serviceName, YConfiguration config)
      throws ConfigurationException {
    super.init(yamcsInstance, serviceName, config);

    /* Local variables */
    this.config = config;
    /* Validate the configuration that the user passed us. */
    try {
      config = getSpec().validate(config);
    } catch (ValidationException e) {
      log.error("Failed configuration validation.", e);
    }
    YarchDatabaseInstance ydb = YarchDatabase.getInstance(yamcsInstance);

    this.opcuaStreamName = config.getString("opcua_stream");

    opcuaStream = getStream(ydb, opcuaStreamName);

    mdb = YamcsServer.getServer().getInstance(yamcsInstance).getXtceDb();

    //    TODO:Make this configurable
    namespace = "/instruments/tvac";

    opcuaInit();
  }

  private void opcuaInit() throws ConfigurationException {
    //  	FIXME:Might need to move this function to start(), maybe...
    try {
      runOPCUAClient();
    } catch (Exception e) {

      e.printStackTrace();
    }
  }

  private static Stream getStream(YarchDatabaseInstance ydb, String streamName) {
    Stream stream = ydb.getStream(streamName);
    if (stream == null) {
      try {
        ydb.execute("create stream " + streamName + gftdef.getStringDefinition());
      } catch (Exception e) {
        throw new ConfigurationException(e);
      }

      stream = ydb.getStream(streamName);
    }
    return stream;
  }

  @Override
  public void doDisable() {
    /* If the thread is created, interrupt it. */
    if (thread != null) {
      thread.interrupt();
    }
  }

  @Override
  public void doEnable() {
    /* Create and start the new thread. */
    thread = new Thread(this);
    thread.setName(this.getClass().getSimpleName() + "-" + linkName);
    thread.start();
  }

  @Override
  public String getDetailedStatus() {
    if (isDisabled()) {
      return String.format("DISABLED");
    } else {
      return String.format("OK, received %d packets", inCount.get());
    }
  }

  @Override
  protected Status connectionStatus() {
    return Status.OK;
  }

  @Override
  protected void doStart() {
    if (!isDisabled()) {
      doEnable();
    }
    startAction.addChangeListener(
        () -> {
          /**
           * TODO:Might be useful if we want turn off any functionality when are action is disabled
           * for instance..
           */
        });
    addAction(startAction);
    startAction.setEnabled(true);
    notifyStarted();
  }

  @Override
  protected void doStop() {
    if (thread != null) {
      thread.interrupt();
    }

    //    FIXME
    //                        client.disconnect().get();
    //      Stack.releaseSharedResources();

    notifyStopped();
  }

  @Override
  public void run() {

    /* Enter our main loop */
    while (isRunningAndEnabled()) {
      /* Iterate through all our watch keys. */

    }
  }

  private void queryAllOPCUAData() {

    Tuple t = null;
    TupleDefinition tdef = gftdef.copy();
    List<Object> cols = new ArrayList<>(4 + nodeIDToParamsMap.keySet().size());

    tdef = gftdef.copy();
    long gentime = timeService.getMissionTime();
    cols.add(gentime);
    cols.add(namespace);
    cols.add(0);
    cols.add(gentime);

    int columnCount = 0;

    Set<NodeId> nodeSet = new HashSet<NodeId>();
    /**
     * FIXME:This is super inefficient... The reason we collect these nodeIDs in a set is because
     * otherwise we will have redundant subscription(s) since there is more than 1 attribute per
     * nodeID given how nodeIDToParamsMap is designed
     */
    for (NodeIDAttrPair pair : nodeIDToParamsMap.keySet()) {
      nodeSet.add(pair.nodeID);
    }

    for (NodeId nId : nodeSet) {
      UaNode node;

      try {
        node = client.getAddressSpace().getNode(nId);

        DataValue nodeClass = node.readAttribute(AttributeId.NodeClass);

        switch (NodeClass.from((int) nodeClass.getValue().getValue())) {
          case DataType:
            //                tdef.addColumn(pair.getValue().getQualifiedName(),
            // DataType.PARAMETER_VALUE);
            //                cols.add(getPV(pair.getValue(), Instant.now().toEpochMilli(),
            // "PlaceHolder"));
            columnCount++;
            break;
          case Method:
            //                tdef.addColumn(pair.getValue().getQualifiedName(),
            // DataType.PARAMETER_VALUE);
            //                cols.add(getPV(pair.getValue(), Instant.now().toEpochMilli(),
            // "PlaceHolder"));
            columnCount++;
            break;
          case Object:
            //                tdef.addColumn(pair.getValue().getQualifiedName(),
            // DataType.PARAMETER_VALUE);
            //                cols.add(getPV(pair.getValue(), Instant.now().toEpochMilli(),
            // "PlaceHolder"));
            columnCount++;
            break;
          case ObjectType:
            //                tdef.addColumn(pair.getValue().getQualifiedName(),
            // DataType.PARAMETER_VALUE);
            //                cols.add(getPV(pair.getValue(), Instant.now().toEpochMilli(),
            // "PlaceHolder"));
            columnCount++;
            break;
          case ReferenceType:
            //                tdef.addColumn(pair.getValue().getQualifiedName(),
            // DataType.PARAMETER_VALUE);
            //                cols.add(getPV(pair.getValue(), Instant.now().toEpochMilli(),
            // "PlaceHolder"));
            columnCount++;
            break;
          case Unspecified:
            //                tdef.addColumn(pair.getValue().getQualifiedName(),
            // DataType.PARAMETER_VALUE);
            //                cols.add(getPV(pair.getValue(), Instant.now().toEpochMilli(),
            // "PlaceHolder"));
            columnCount++;
            break;
          case Variable:
            //                tdef.addColumn(pair.getValue().getQualifiedName(),
            // DataType.PARAMETER_VALUE);
            //                cols.add(getPV(pair.getValue(), Instant.now().toEpochMilli(),
            // "PlaceHolder"));
            columnCount++;
            for (AttributeId attr : AttributeId.VARIABLE_ATTRIBUTES) {
              String value = "";
              if (node.readAttribute(attr).getValue().isNull()) {
                value = "NULL";
              } else {
                value = node.readAttribute(attr).getValue().getValue().toString();
              }

              VariableParam p = nodeIDToParamsMap.get(new NodeIDAttrPair(nId, attr));
              tdef.addColumn(p.getQualifiedName(), DataType.PARAMETER_VALUE);
              cols.add(getPV(p, Instant.now().toEpochMilli(), value));

              columnCount++;
            }
            break;
          case VariableType:
            //                tdef.addColumn(pair.getValue().getQualifiedName(),
            // DataType.PARAMETER_VALUE);
            //                cols.add(getPV(pair.getValue(), Instant.now().toEpochMilli(),
            // "PlaceHolder"));
            columnCount++;
            break;
          case View:
            //                tdef.addColumn(pair.getValue().getQualifiedName(),
            // DataType.PARAMETER_VALUE);
            //                cols.add(getPV(pair.getValue(), Instant.now().toEpochMilli(),
            // "PlaceHolder"));
            columnCount++;
            break;
          default:
            break;
        }

      } catch (UaException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
        continue;
      }
    }

    /**
     * FIXME:Need to come up with a mechanism to not update certain values that are up to date...
     * The more I think about it, it might make sense to have "static" and "runtime" namespaces
     */
    t = new Tuple(tdef, cols);

    opcuaStream.emitTuple(t);

    inCount.getAndAdd(columnCount);
  }

  private static ParameterType getOrCreateType(
      XtceDb mdb, String name, UnitType unit, Supplier<ParameterType.Builder<?>> supplier) {

    String units;
    if (unit != null) {
      units = unit.getUnit();
      if (!"1".equals(unit.getFactor())) {
        units = unit.getFactor() + "x" + units;
      }
      if (unit.getPower() != 1) {
        units = units + "^" + unit.getPower();
      }
      name = name + "_" + units.replaceAll("/", "_");
    }

    String fqn = XtceDb.YAMCS_SPACESYSTEM_NAME + NameDescription.PATH_SEPARATOR + name;
    ParameterType ptype = mdb.getParameterType(fqn);
    if (ptype != null) {
      return ptype;
    }
    ParameterType.Builder<?> typeb = supplier.get().setName(name);
    if (unit != null) {
      ((BaseDataType.Builder<?>) typeb).addUnit(unit);
    }

    ptype = typeb.build();
    ((NameDescription) ptype).setQualifiedName(fqn);

    return mdb.addSystemParameterType(ptype);
  }

  public static ParameterType getBasicType(XtceDb mdb, Type type, UnitType unit) {

    switch (type) {
      case BINARY:
        return getOrCreateType(mdb, "binary", unit, () -> new BinaryParameterType.Builder());
      case BOOLEAN:
        return getOrCreateType(mdb, "boolean", unit, () -> new BooleanParameterType.Builder());
      case STRING:
        return getOrCreateType(mdb, "string", unit, () -> new StringParameterType.Builder());
      case FLOAT:
        return getOrCreateType(
            mdb, "float32", unit, () -> new FloatParameterType.Builder().setSizeInBits(32));
      case DOUBLE:
        return getOrCreateType(
            mdb, "float64", unit, () -> new FloatParameterType.Builder().setSizeInBits(64));
      case SINT32:
        return getOrCreateType(
            mdb,
            "sint32",
            unit,
            () -> new IntegerParameterType.Builder().setSizeInBits(32).setSigned(true));
      case SINT64:
        return getOrCreateType(
            mdb,
            "sint64",
            unit,
            () -> new IntegerParameterType.Builder().setSizeInBits(64).setSigned(true));
      case UINT32:
        return getOrCreateType(
            mdb,
            "uint32",
            unit,
            () -> new IntegerParameterType.Builder().setSizeInBits(32).setSigned(false));
      case UINT64:
        return getOrCreateType(
            mdb,
            "uint64",
            unit,
            () -> new IntegerParameterType.Builder().setSizeInBits(64).setSigned(false));
      case TIMESTAMP:
        return getOrCreateType(mdb, "time", unit, () -> new AbsoluteTimeParameterType.Builder());
      case ENUMERATED:
        return getOrCreateType(mdb, "enum", unit, () -> new EnumeratedParameterType.Builder());
      default:
        throw new IllegalArgumentException(type + "is not a basic type");
    }
  }

  @Override
  public void onTuple(Stream stream, Tuple tuple) {}

  @Override
  public void setupSystemParameters(SystemParametersService sysParamCollector) {
    super.setupSystemParameters(sysParamCollector);
    OPCUAServerStatus =
        sysParamCollector.createSystemParameter(
            linkName + "/outOfSync",
            Yamcs.Value.Type.BOOLEAN,
            "Are the downlinked events not in sync wtih the ones from the log?");
  }

  @Override
  public List<ParameterValue> getSystemParameters() {
    long time = getCurrentTime();

    ArrayList<ParameterValue> list = new ArrayList<>();
    try {
      collectSystemParameters(time, list);
    } catch (Exception e) {
      log.error("Exception caught when collecting link system parameters", e);
    }
    return list;
  }

  @Override
  protected void collectSystemParameters(long time, List<ParameterValue> list) {
    super.collectSystemParameters(time, list);
    //    FIXME
    //    list.add(SystemParametersService.getPV(OPCUAServerStatus, time, outOfSync));
  }

  public static ParameterValue getNewPv(Parameter parameter, long time) {
    ParameterValue pv = new ParameterValue(parameter);
    pv.setAcquisitionTime(time);
    pv.setGenerationTime(time);
    return pv;
  }

  public static ParameterValue getPV(Parameter parameter, long time, String v) {
    ParameterValue pv = getNewPv(parameter, time);
    pv.setEngValue(ValueUtility.getStringValue(v));
    return pv;
  }

  public static ParameterValue getPV(Parameter parameter, long time, double v) {
    ParameterValue pv = getNewPv(parameter, time);
    pv.setEngValue(ValueUtility.getDoubleValue(v));
    return pv;
  }

  public static ParameterValue getPV(Parameter parameter, long time, float v) {
    ParameterValue pv = getNewPv(parameter, time);
    pv.setEngValue(ValueUtility.getFloatValue(v));
    return pv;
  }

  public static ParameterValue getPV(Parameter parameter, long time, boolean v) {
    ParameterValue pv = getNewPv(parameter, time);
    pv.setEngValue(ValueUtility.getBooleanValue(v));
    return pv;
  }

  public static ParameterValue getPV(Parameter parameter, long time, long v) {
    ParameterValue pv = getNewPv(parameter, time);
    pv.setEngValue(ValueUtility.getSint64Value(v));
    return pv;
  }

  public static ParameterValue getUnsignedIntPV(Parameter parameter, long time, int v) {
    ParameterValue pv = getNewPv(parameter, time);
    pv.setEngValue(ValueUtility.getUint64Value(v));
    return pv;
  }

  public static <T extends Enum<T>> ParameterValue getPV(Parameter parameter, long time, T v) {
    ParameterValue pv = getNewPv(parameter, time);
    pv.setEngValue(ValueUtility.getEnumeratedValue(v.ordinal(), v.name()));
    return pv;
  }

  public static ParameterValue getPV(Parameter parameter, long time, Value v) {
    ParameterValue pv = getNewPv(parameter, time);
    pv.setEngValue(v);
    return pv;
  }

  @Override
  public Status getLinkStatus() {
    // TODO Auto-generated method stub
    return Status.OK;
  }

  @Override
  public void enable() {
    // TODO Auto-generated method stub

  }

  @Override
  public void disable() {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean isDisabled() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public long getDataInCount() {
    // TODO Auto-generated method stub
    return inCount.get();
  }

  @Override
  public long getDataOutCount() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void resetCounters() {
    // TODO Auto-generated method stub
    inCount.set(0);
  }

  private OpcUaClient createClient() throws Exception {
    Path securityTempDir = Paths.get(System.getProperty("java.io.tmpdir"), "client", "security");
    Files.createDirectories(securityTempDir);
    if (!Files.exists(securityTempDir)) {
      throw new Exception("unable to create security dir: " + securityTempDir);
    }

    File pkiDir = securityTempDir.resolve("pki").toFile();

    LoggerFactory.getLogger(getClass()).info("security dir: {}", securityTempDir.toAbsolutePath());
    LoggerFactory.getLogger(getClass()).info("security pki dir: {}", pkiDir.getAbsolutePath());

    trustListManager = new DefaultTrustListManager(pkiDir);

    //    FIXME:Make url configurable
    List<EndpointDescription> endpoint =
        DiscoveryClient.getEndpoints("opc.tcp://localhost:4840/").get();

    OpcUaClientConfig builder = OpcUaClientConfig.builder().setEndpoint(endpoint.get(0)).build();

    return OpcUaClient.create(builder);
  }

  private void browseNode(String indent, OpcUaClient client, NodeId browseRoot) {
    BrowseDescription browse =
        new BrowseDescription(
            browseRoot,
            BrowseDirection.Forward,
            Identifiers.References,
            true,
            uint(NodeClass.Object.getValue() | NodeClass.Variable.getValue()),
            uint(BrowseResultMask.All.getValue()));

    try {
      BrowseResult browseResult = client.browse(browse).get();

      List<ReferenceDescription> references = toList(browseResult.getReferences());

      for (ReferenceDescription rd : references) {
        Object desc = null;
        Object value = null;
        try {
          UaNode node =
              client
                  .getAddressSpace()
                  .getNode(rd.getNodeId().toNodeId(client.getNamespaceTable()).get());
          DataValue attr = node.readAttribute(AttributeId.Description);
          desc = attr.getValue().getValue();

          attr = node.readAttribute(AttributeId.Value);

          value = attr.getValue();

        } catch (UaException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }

        if (rd.getBrowseName()
            .getName()
            .contains(Character.toString(NameDescription.PATH_SEPARATOR))) {
          log.info(
              "{} ignored since it contains a {} character",
              rd.getBrowseName().getName(),
              Character.toString(NameDescription.PATH_SEPARATOR));
        } else {

          //        FIXME:Remember to re-use these params (Do NOT create new objects when pushing
          // PVs
          // out to streams)

          /**
           * NOTE:For now we'll just flatten all the attributes instead of using an aggregate type
           * for attributes
           */
          //          p.setParameterType(opcuaAttrsType);

          for (AttributeId attr : AttributeId.values()) {

            ParameterType ptype = getBasicType(mdb, Yamcs.Value.Type.STRING, null);
            Parameter p =
                VariableParam.getForFullyQualifiedName(
                    qualifiedName(
                        namespace
                            + NameDescription.PATH_SEPARATOR
                            + rd.getNodeId().toNodeId(client.getNamespaceTable()).get()
                            + NameDescription.PATH_SEPARATOR
                            + rd.getNodeClass()
                            + NameDescription.PATH_SEPARATOR
                            + rd.getBrowseName().getName(),
                        attr.toString()));

            p.setParameterType(ptype);

            //        TODO:Add Map of node_id -> Params
            if (mdb.getParameter(p.getQualifiedName()) == null) {
              log.debug("Adding OPCUA object as parameter to mdb:{}", p.getQualifiedName());
              mdb.addParameter(p, true);

              nodeIDToParamsMap.put(
                  new NodeIDAttrPair(
                      rd.getNodeId().toNodeId(client.getNamespaceTable()).get(), attr),
                  (VariableParam) p);

              TupleDefinition tdef = gftdef.copy();
              List<Object> cols = new ArrayList<>(4 + 1);
              long gentime = timeService.getMissionTime();
              cols.add(gentime);
              cols.add(namespace);
              cols.add(0);
              cols.add(gentime);

              //            tdef.addColumn(
              //                nodeIDToParamsMap.get(items.get(i).getNodeId()).getQualifiedName(),
              //                DataType.PARAMETER_VALUE);
              //
              //            cols.add(
              //                getPV(
              //                    nodeIDToParamsMap.get(items.get(i).getNodeId()),
              //                    Instant.now().toEpochMilli(),
              //                    values.get(i).getValue().toString()));

              //              tdef.addColumn(
              //                  nodeIDToParamsMap
              //
              // .get(rd.getNodeId().toNodeId(client.getNamespaceTable()).get())
              //                      .getQualifiedName(),
              //                  DataType.PARAMETER_VALUE);
              //
              //              cols.add(
              //                  getPV(
              //                      nodeIDToParamsMap.get(
              //
              // rd.getNodeId().toNodeId(client.getNamespaceTable()).get()),
              //                      Instant.now().toEpochMilli(),
              //                      "PlaceHolder"));
              //
              //              Tuple t = new Tuple(tdef, cols);
              //
              //              log.info(
              //                  "Data({}) chnage triggered for {} and pushing placeholder PV",
              //                  "PlaceHolder",
              //                  nodeIDToParamsMap
              //
              // .get(rd.getNodeId().toNodeId(client.getNamespaceTable()).get())
              //                      .getQualifiedName());
              //
              //              opcuaStream.emitTuple(t);
            }
          }
        }

        log.debug(
            "{} Node={}, Desc={}, Value={}", indent, rd.getBrowseName().getName(), desc, value);
        {
        }

        // recursively browse to children
        rd.getNodeId()
            .toNodeId(client.getNamespaceTable())
            .ifPresent(nodeId -> browseNode(indent + "  ", client, nodeId));
      }
    } catch (InterruptedException | ExecutionException e) {
      log.error("Browsing nodeId={} failed: {}", browseRoot, e.getMessage(), e);
    }
  }

  private void createOPCUASubscriptions() {
    Set<NodeId> nodeSet = new HashSet<NodeId>();
    /**
     * FIXME:This is super inefficient... The reason we collect these nodeIDs in a set is because
     * otherwise we will have redundant subscription(s) since there is more than 1 attribute per
     * nodeID given how nodeIDToParamsMap is designed
     */
    for (NodeIDAttrPair pair : nodeIDToParamsMap.keySet()) {
      nodeSet.add(pair.nodeID);
    }
    for (NodeId id : nodeSet) {
      Variant nodeClass = null;
      try {
        UaNode node = client.getAddressSpace().getNode(id);

        nodeClass = node.readAttribute(AttributeId.NodeClass).getValue();

      } catch (UaException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      try {
        switch (NodeClass.from((int) nodeClass.getValue())) {
          case DataType:
            break;
          case Method:
            break;
          case Object:
            break;
          case ObjectType:
            break;
          case ReferenceType:
            break;
          case Unspecified:
            break;
          case Variable:
            ManagedDataItem dataItem = opcuaSubscription.createDataItem(id);
            log.debug("Status code for dataItem:{}", dataItem.getStatusCode());
            break;
          case VariableType:
            break;
          case View:
            break;
          default:
            break;
        }
      } catch (UaException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }

  public void connectToOPCUAServer(OpcUaClient client, CompletableFuture<OpcUaClient> future)
      throws Exception {
    // synchronous connect
    client.connect().get();

    // start browsing at root folder
    browseNode("", client, Identifiers.RootFolder);

    future.complete(client);
  }

  public void runOPCUAClient() {

    createOPCUAAttrAggregateType();
    mdb.addParameterType(opcuaAttrsType, true);

    final CompletableFuture<OpcUaClient> future = new CompletableFuture<>();

    client = null;
    try {
      client = createClient();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    try {
      connectToOPCUAServer(client, future);

      try {
        opcuaSubscription = ManagedSubscription.create(client, 1);
      } catch (UaException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }

      opcuaSubscription.addDataChangeListener(
          (items, values) -> {
            for (int i = 0; i < items.size(); i++) {
              NodeIDAttrPair nodeAttrKey =
                  new NodeIDAttrPair(items.get(i).getNodeId(), AttributeId.Value);
              log.debug(
                  "subscription value received: item={}, value={}",
                  items.get(i).getNodeId(),
                  values.get(i).getValue());

              log.debug(
                  "Pushing new PV for param name {} which is mapped to NodeID {}",
                  nodeIDToParamsMap.get(nodeAttrKey),
                  items.get(i).getNodeId());

              TupleDefinition tdef = gftdef.copy();
              List<Object> cols = new ArrayList<>(4 + 1);
              long gentime = timeService.getMissionTime();
              cols.add(gentime);
              cols.add(namespace);
              cols.add(0);
              cols.add(gentime);

              /**
               * TODO:Not sure if this is the best way to do this since the aggregate values will be
               * partially updated. Another potential approach might be to decouple the live OPCUA
               * data(subscriptions) via namespaces. For example; have a "special" namespace called
               * "subscriptions" that ONLY gets updated with items. And maybe another namespace for
               * static data...maybe.
               *
               * <p>Another option is to flatten everything and have no aggregate types at all. That
               * approach might even simplify the code quite a bit...
               *
               * <p>Another question worth answering before moving forward is to find whether or not
               * it is concrete in the OPCUA protocol what data can change in real time and which
               * data is "static". Not sure if there is any "static" data given that clients have
               * the ability of writing to values... might be worth a test.
               */

              // FIMXE:Properly add aggregatevalues instead of getPV flat values
              //            AggregateValue v = new
              // AggregateValue(fileStoreAggrType.getMemberNames());
              //            v.setMemberValue("total", ValueUtility.getSint64Value(ts / 1024));
              //            v.setMemberValue("available", ValueUtility.getSint64Value(av / 1024));
              //            v.setMemberValue("percentageUse", ValueUtility.getFloatValue(perc));
              //
              //            ParameterValue pv = new ParameterValue(storep.param);
              //            pv.setGenerationTime(gentime);
              //            pv.setAcquisitionTime(gentime);
              //            pv.setAcquisitionStatus(AcquisitionStatus.ACQUIRED);
              //            pv.setEngValue(v);

              log.debug(
                  "Data({}) chnage triggered for {}",
                  values.get(i).getValue(),
                  nodeIDToParamsMap.get(nodeAttrKey));

              if (nodeIDToParamsMap.get(nodeAttrKey) == null) {
                log.debug("No parameter mapping found for {}", nodeAttrKey.nodeID);
                continue;
              } else {
                log.debug(
                    String.format(
                        "parameter mapping found for {} and {}",
                        nodeAttrKey.nodeID,
                        nodeAttrKey.attrID));
              }

              if (values.get(i).getValue() != null) {
                tdef.addColumn(
                    nodeIDToParamsMap.get(nodeAttrKey).getQualifiedName(),
                    DataType.PARAMETER_VALUE);

                cols.add(
                    getPV(
                        nodeIDToParamsMap.get(nodeAttrKey),
                        Instant.now().toEpochMilli(),
                        values.get(i).getValue().toString()));

                Tuple t = new Tuple(tdef, cols);
                opcuaStream.emitTuple(t);
                inCount.getAndAdd(1);
              } else {
                tdef.addColumn(
                    nodeIDToParamsMap.get(nodeAttrKey).getQualifiedName(),
                    DataType.PARAMETER_VALUE);

                cols.add(
                    getPV(
                        nodeIDToParamsMap.get(nodeAttrKey),
                        Instant.now().toEpochMilli(),
                        "PlaceHolder**"));

                log.info(
                    "Data({}) chnage triggered for {} and pushing placeholder PV",
                    values.get(i).getValue(),
                    nodeIDToParamsMap.get(nodeAttrKey).getQualifiedName());

                Tuple t = new Tuple(tdef, cols);

                //                opcuaStream.emitTuple(t);

                //                inCount.getAndAdd(1);
              }
            }
          });

      createOPCUASubscriptions();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  /**
   * This method is here for future growth in case we find there is a benefit to using aggregate
   * types
   */
  private void createOPCUAAttrAggregateType() {

    AggregateParameterType.Builder opcuaAttrsTypeBuidlder = new AggregateParameterType.Builder();

    opcuaAttrsType = new AggregateParameterType.Builder().setName("OPCUObjectAttributes").build();

    opcuaAttrsTypeBuidlder.setName("OPCUObjectAttributes");
    for (AttributeId attr : AttributeId.values()) {
      opcuaAttrsTypeBuidlder.addMember(
          new Member(attr.toString(), getBasicType(mdb, Type.STRING, null)));
    }

    opcuaAttrsType = opcuaAttrsTypeBuidlder.build();
    ((NameDescription) opcuaAttrsType)
        .setQualifiedName(qualifiedName(namespace, opcuaAttrsType.getName()));
  }

  private ParameterType OPCUAAttrTypeToParamType(AttributeId attr) {
    ParameterType pType = null;

    switch (attr) {
      case AccessLevel:
        break;
      case ArrayDimensions:
        break;
      case BrowseName:
        pType = getBasicType(mdb, Type.STRING, null);
        break;
      case ContainsNoLoops:
        break;
      case DataType:
        break;
      case Description:
        pType = getBasicType(mdb, Type.STRING, null);
        break;
      case DisplayName:
        pType = getBasicType(mdb, Type.STRING, null);
        break;
      case EventNotifier:
        break;
      case Executable:
        break;
      case Historizing:
        break;
      case InverseName:
        pType = getBasicType(mdb, Type.STRING, null);
        break;
      case IsAbstract:
        break;
      case MinimumSamplingInterval:
        break;
      case NodeClass:
        break;
      case NodeId:
        pType = getBasicType(mdb, Type.STRING, null);
        break;
      case Symmetric:
        break;
      case UserAccessLevel:
        break;
      case UserExecutable:
        break;
      case UserWriteMask:
        break;
      case Value:
        break;
      case ValueRank:
        break;
      case WriteMask:
        break;
      default:
        break;
    }

    return pType;
  }
}
