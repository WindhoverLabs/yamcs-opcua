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

import static com.google.common.collect.Lists.newArrayList;
import static org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned.uint;
import static org.eclipse.milo.opcua.stack.core.util.ConversionUtil.l;
import static org.eclipse.milo.opcua.stack.core.util.ConversionUtil.toList;
import static org.yamcs.xtce.NameDescription.qualifiedName;

import com.google.gson.JsonObject;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.logging.Logger;
import org.eclipse.milo.opcua.sdk.client.AddressSpace.BrowseOptions;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.api.config.OpcUaClientConfig;
import org.eclipse.milo.opcua.sdk.client.api.subscriptions.UaMonitoredItem;
import org.eclipse.milo.opcua.sdk.client.api.subscriptions.UaSubscription;
import org.eclipse.milo.opcua.sdk.client.nodes.UaNode;
import org.eclipse.milo.opcua.sdk.client.subscriptions.ManagedDataItem;
import org.eclipse.milo.opcua.sdk.client.subscriptions.ManagedSubscription;
import org.eclipse.milo.opcua.stack.client.DiscoveryClient;
import org.eclipse.milo.opcua.stack.core.AttributeId;
import org.eclipse.milo.opcua.stack.core.Identifiers;
import org.eclipse.milo.opcua.stack.core.UaException;
import org.eclipse.milo.opcua.stack.core.security.DefaultTrustListManager;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.ExtensionObject;
import org.eclipse.milo.opcua.stack.core.types.builtin.LocalizedText;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.QualifiedName;
import org.eclipse.milo.opcua.stack.core.types.builtin.StatusCode;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UInteger;
import org.eclipse.milo.opcua.stack.core.types.enumerated.BrowseDirection;
import org.eclipse.milo.opcua.stack.core.types.enumerated.BrowseResultMask;
import org.eclipse.milo.opcua.stack.core.types.enumerated.IdType;
import org.eclipse.milo.opcua.stack.core.types.enumerated.MonitoringMode;
import org.eclipse.milo.opcua.stack.core.types.enumerated.NodeClass;
import org.eclipse.milo.opcua.stack.core.types.enumerated.TimestampsToReturn;
import org.eclipse.milo.opcua.stack.core.types.structured.BrowseDescription;
import org.eclipse.milo.opcua.stack.core.types.structured.BrowsePath;
import org.eclipse.milo.opcua.stack.core.types.structured.BrowsePathResult;
import org.eclipse.milo.opcua.stack.core.types.structured.BrowseResult;
import org.eclipse.milo.opcua.stack.core.types.structured.ContentFilter;
import org.eclipse.milo.opcua.stack.core.types.structured.EndpointDescription;
import org.eclipse.milo.opcua.stack.core.types.structured.EventFilter;
import org.eclipse.milo.opcua.stack.core.types.structured.MonitoredItemCreateRequest;
import org.eclipse.milo.opcua.stack.core.types.structured.MonitoringParameters;
import org.eclipse.milo.opcua.stack.core.types.structured.ReadValueId;
import org.eclipse.milo.opcua.stack.core.types.structured.ReferenceDescription;
import org.eclipse.milo.opcua.stack.core.types.structured.RelativePath;
import org.eclipse.milo.opcua.stack.core.types.structured.RelativePathElement;
import org.eclipse.milo.opcua.stack.core.types.structured.SimpleAttributeOperand;
import org.eclipse.milo.opcua.stack.core.types.structured.TranslateBrowsePathsToNodeIdsResponse;
import org.slf4j.LoggerFactory;
import org.yamcs.ConfigurationException;
import org.yamcs.Spec;
import org.yamcs.Spec.OptionType;
import org.yamcs.StandardTupleDefinitions;
import org.yamcs.ValidationException;
import org.yamcs.YConfiguration;
import org.yamcs.YamcsServer;
import org.yamcs.parameter.ParameterValue;
import org.yamcs.protobuf.Yamcs;
import org.yamcs.protobuf.Yamcs.Value.Type;
import org.yamcs.tctm.AbstractLink;
import org.yamcs.tctm.Link;
import org.yamcs.tctm.LinkAction;
import org.yamcs.tctm.PacketInputStream;
import org.yamcs.tctm.ParameterSink;
import org.yamcs.utils.ValueUtility;
import org.yamcs.xtce.AggregateParameterType;
import org.yamcs.xtce.Member;
import org.yamcs.xtce.NameDescription;
import org.yamcs.xtce.Parameter;
import org.yamcs.xtce.ParameterType;
import org.yamcs.xtce.StringParameterType;
import org.yamcs.xtce.XtceDb;
import org.yamcs.yarch.DataType;
import org.yamcs.yarch.FileSystemBucket;
import org.yamcs.yarch.Stream;
import org.yamcs.yarch.Tuple;
import org.yamcs.yarch.TupleDefinition;
import org.yamcs.yarch.YarchDatabase;
import org.yamcs.yarch.YarchDatabaseInstance;

public class OPCUALink extends AbstractLink implements Runnable {

  class NodeIDAttrPair {
    NodeId nodeID;
    AttributeId attrID;

    public NodeIDAttrPair(NodeId newNodeID, AttributeId newAttrID) {
      this.nodeID = newNodeID;
      this.attrID = newAttrID;
    }

    public int hashCode() {
      return Objects.hash(this.nodeID, this.attrID);
    }

    public boolean equals(Object obj) {
      return (this.hashCode() == obj.hashCode());
    }
  }

  class NodePath {
    String path;

    HashMap<Object, Object> rootNodeID = new HashMap<Object, Object>();
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
  private String parametersNamespace;
  private String serverId;
  XtceDb mdb;

  Stream opcuaStream;

  ParameterSink paraSink;
  private static TupleDefinition gftdef = StandardTupleDefinitions.PARAMETER.copy();

  private DefaultTrustListManager trustListManager;
  private AggregateParameterType opcuaAttrsType;
  private ManagedSubscription opcuaSubscription;

  private static final Logger internalLogger = Logger.getLogger(OPCUALink.class.getName());

  private int rootNamespaceIndex;

  private String rootIdentifier; // Relative to the rootNamespaceIndex

  private IdType rootIdentifierType; // Relative to the rootNamespaceIndex

  private int relativePathNamespaceIndex;

  private String relativePathIdentifier; // Relative to the rootNamespaceIndex

  private IdType relativePathIdentifierType; // Relative to the rootNamespaceIndex

  private String relativeNodePath;

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

  private String endpointURL;

  private Status linkStatus = Status.OK;

  private String discoverURL;

  private boolean queryAllNodesAtStartup;

  private ArrayList<NodePath> relativeNodePaths = new ArrayList<NodePath>();

  private final AtomicLong clientHandles = new AtomicLong(1L);

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

  @Override
  public Spec getSpec() {
    Spec spec = new Spec();

    /* Define our configuration parameters. */
    spec.addOption("name", OptionType.STRING).withRequired(true);
    spec.addOption("class", OptionType.STRING).withRequired(true);
    spec.addOption("opcuaStream", OptionType.STRING).withRequired(true);
    spec.addOption("endpointUrl", OptionType.STRING).withRequired(true);
    spec.addOption("discoveryUrl", OptionType.STRING).withRequired(true);
    spec.addOption("parametersNamespace", OptionType.STRING).withRequired(true);
    spec.addOption("queryAllNodesAtStartup", OptionType.BOOLEAN).withRequired(false);

    Spec rootNodeIDSpec = new Spec();

    rootNodeIDSpec.addOption("namespaceIndex", OptionType.INTEGER).withRequired(true);
    rootNodeIDSpec.addOption("identifier", OptionType.STRING).withRequired(true);
    rootNodeIDSpec.addOption("identifierType", OptionType.STRING).withRequired(true);

    spec.addOption("rootNodeID", OptionType.MAP).withRequired(true).withSpec(rootNodeIDSpec);

    Spec nodePathSpec = new Spec();
    nodePathSpec.addOption("path", OptionType.STRING);
    nodePathSpec
        .addOption("rootNodeID", OptionType.MAP)
        .withRequired(true)
        .withSpec(rootNodeIDSpec);

    spec.addOption("nodePaths", OptionType.LIST)
        .withElementType(OptionType.MAP)
        .withRequired(true)
        .withSpec(nodePathSpec);

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

    this.opcuaStreamName = config.getString("opcuaStream");

    opcuaStream = getStream(ydb, opcuaStreamName);

    this.endpointURL = config.getString("endpointUrl");

    this.discoverURL = config.getString("discoveryUrl");

    this.parametersNamespace = config.getString("parametersNamespace");

    queryAllNodesAtStartup = config.getBoolean("queryAllNodesAtStartup", false);

    Map<Object, Object> root = config.getMap("rootNodeID");

    rootNamespaceIndex = (int) root.get("namespaceIndex");

    rootIdentifier = (String) root.get("identifier");
    rootIdentifierType = IdType.valueOf((String) root.get("identifierType"));

    List<Map<Object, Object>> nodePaths = config.getList("nodePaths");

    for (Map<Object, Object> path : nodePaths) {
      NodePath nodePath = new NodePath();
      nodePath.path = (String) path.get("path");
      nodePath.rootNodeID = (HashMap<Object, Object>) path.get("rootNodeID");
      relativeNodePaths.add(nodePath);
    }

    mdb = YamcsServer.getServer().getInstance(yamcsInstance).getXtceDb();
  }

  private void opcuaInit() {
    //  	FIXME:Might need to move this function to start(), maybe...

    createOPCUAAttrAggregateType();
    mdb.addParameterType(opcuaAttrsType, true);

    final CompletableFuture<OpcUaClient> future = new CompletableFuture<>();

    client = null;
    try {
      client = configureClient();

      connectToOPCUAServer(client, future);

      browseOPCUATree(client, future);

      subscribeToEvents(client);

    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      return;
    }
    try {
      createOPCUASubscriptions();
    } catch (Exception e) {
      // TODO Auto-generated catch block
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

    linkStatus = Status.DISABLED;
  }

  @Override
  public void doEnable() {
    linkStatus = Status.OK;
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
  public Status connectionStatus() {
    return linkStatus;
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

    /* Create and start the new thread. */
    thread = new Thread(this);
    thread.setName(this.getClass().getSimpleName() + "-" + linkName);
    thread.start();

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

    opcuaInit();
    if (queryAllNodesAtStartup) {
      queryAllOPCUAData();
    }
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
    cols.add(parametersNamespace);
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
            //            columnCount++;
            //            break;
          case Method:
            //                tdef.addColumn(pair.getValue().getQualifiedName(),
            // DataType.PARAMETER_VALUE);
            //                cols.add(getPV(pair.getValue(), Instant.now().toEpochMilli(),
            // "PlaceHolder"));
            //            columnCount++;
            //            break;
          case Object:
            //                tdef.addColumn(pair.getValue().getQualifiedName(),
            // DataType.PARAMETER_VALUE);
            //                cols.add(getPV(pair.getValue(), Instant.now().toEpochMilli(),
            // "PlaceHolder"));
            //            columnCount++;
            //            break;
          case ObjectType:
            //                tdef.addColumn(pair.getValue().getQualifiedName(),
            // DataType.PARAMETER_VALUE);
            //                cols.add(getPV(pair.getValue(), Instant.now().toEpochMilli(),
            // "PlaceHolder"));
            //            columnCount++;
            //            break;
          case ReferenceType:
            //                tdef.addColumn(pair.getValue().getQualifiedName(),
            // DataType.PARAMETER_VALUE);
            //                cols.add(getPV(pair.getValue(), Instant.now().toEpochMilli(),
            // "PlaceHolder"));
            //            columnCount++;
            //            break;
          case Unspecified:
            //                tdef.addColumn(pair.getValue().getQualifiedName(),
            // DataType.PARAMETER_VALUE);
            //                cols.add(getPV(pair.getValue(), Instant.now().toEpochMilli(),
            // "PlaceHolder"));
            //            columnCount++;
            //            break;
          case Variable:
            //                tdef.addColumn(pair.getValue().getQualifiedName(),
            // DataType.PARAMETER_VALUE);
            //                cols.add(getPV(pair.getValue(), Instant.now().toEpochMilli(),
            // "PlaceHolder"));
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

              log.debug("Pushing {} to stream", p.toString());

              columnCount++;
            }
            break;
            //          case VariableType:
            //            //                tdef.addColumn(pair.getValue().getQualifiedName(),
            //            // DataType.PARAMETER_VALUE);
            //            //                cols.add(getPV(pair.getValue(),
            // Instant.now().toEpochMilli(),
            //            // "PlaceHolder"));
            //            //            columnCount++;
            ////            break;
            //          case View:
            //            //                tdef.addColumn(pair.getValue().getQualifiedName(),
            //            // DataType.PARAMETER_VALUE);
            //            //                cols.add(getPV(pair.getValue(),
            // Instant.now().toEpochMilli(),
            //            // "PlaceHolder"));
            //            //            columnCount++;
            ////            break;
            //          default:
            //            break;
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
    pushTuple(tdef, cols);

    inCount.getAndAdd(columnCount);
  }

  private synchronized void pushTuple(TupleDefinition tdef, List<Object> cols) {
    Tuple t;
    t = new Tuple(tdef, cols);

    opcuaStream.emitTuple(t);
  }

  private static ParameterType getOrCreateType(
      XtceDb mdb, String name, Supplier<ParameterType.Builder<?>> supplier) {

    String fqn = XtceDb.YAMCS_SPACESYSTEM_NAME + NameDescription.PATH_SEPARATOR + name;
    ParameterType ptype = mdb.getParameterType(fqn);
    if (ptype != null) {
      return ptype;
    }
    ParameterType.Builder<?> typeb = supplier.get().setName(name);

    ptype = typeb.build();
    ((NameDescription) ptype).setQualifiedName(fqn);

    return mdb.addSystemParameterType(ptype);
  }

  public static ParameterType getBasicType(XtceDb mdb, Type type) {
    ParameterType pType = null;
    switch (type) {
        //      case BINARY:
        //        return getOrCreateType(mdb, "binary", unit, () -> new
        // BinaryParameterType.Builder());
        //      case BOOLEAN:
        //        return getOrCreateType(mdb, "boolean", unit, () -> new
        // BooleanParameterType.Builder());
      case STRING:
        pType = getOrCreateType(mdb, "string", () -> new StringParameterType.Builder());
        break;
        //      case FLOAT:
        //        return getOrCreateType(
        //            mdb, "float32", unit, () -> new
        // FloatParameterType.Builder().setSizeInBits(32));
        //      case DOUBLE:
        //        return getOrCreateType(
        //            mdb, "float64", unit, () -> new
        // FloatParameterType.Builder().setSizeInBits(64));
        //      case SINT32:
        //        return getOrCreateType(
        //            mdb,
        //            "sint32",
        //            unit,
        //            () -> new IntegerParameterType.Builder().setSizeInBits(32).setSigned(true));
        //      case SINT64:
        //        return getOrCreateType(
        //            mdb,
        //            "sint64",
        //            unit,
        //            () -> new IntegerParameterType.Builder().setSizeInBits(64).setSigned(true));
        //      case UINT32:
        //        return getOrCreateType(
        //            mdb,
        //            "uint32",
        //            unit,
        //            () -> new IntegerParameterType.Builder().setSizeInBits(32).setSigned(false));
        //      case UINT64:
        //        return getOrCreateType(
        //            mdb,
        //            "uint64",
        //            unit,
        //            () -> new IntegerParameterType.Builder().setSizeInBits(64).setSigned(false));
        //      case TIMESTAMP:
        //        return getOrCreateType(mdb, "time", unit, () -> new
        // AbsoluteTimeParameterType.Builder());
        //      case ENUMERATED:
        //        return getOrCreateType(mdb, "enum", unit, () -> new
        // EnumeratedParameterType.Builder());
    }

    return pType;
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

  //  public static ParameterValue getPV(Parameter parameter, long time, double v) {
  //    ParameterValue pv = getNewPv(parameter, time);
  //    pv.setEngValue(ValueUtility.getDoubleValue(v));
  //    return pv;
  //  }
  //
  //  public static ParameterValue getPV(Parameter parameter, long time, float v) {
  //    ParameterValue pv = getNewPv(parameter, time);
  //    pv.setEngValue(ValueUtility.getFloatValue(v));
  //    return pv;
  //  }
  //
  //  public static ParameterValue getPV(Parameter parameter, long time, boolean v) {
  //    ParameterValue pv = getNewPv(parameter, time);
  //    pv.setEngValue(ValueUtility.getBooleanValue(v));
  //    return pv;
  //  }
  //
  //  public static ParameterValue getPV(Parameter parameter, long time, long v) {
  //    ParameterValue pv = getNewPv(parameter, time);
  //    pv.setEngValue(ValueUtility.getSint64Value(v));
  //    return pv;
  //  }
  //
  //  public static ParameterValue getUnsignedIntPV(Parameter parameter, long time, int v) {
  //    ParameterValue pv = getNewPv(parameter, time);
  //    pv.setEngValue(ValueUtility.getUint64Value(v));
  //    return pv;
  //  }
  //
  //  public static <T extends Enum<T>> ParameterValue getPV(Parameter parameter, long time, T v) {
  //    ParameterValue pv = getNewPv(parameter, time);
  //    pv.setEngValue(ValueUtility.getEnumeratedValue(v.ordinal(), v.name()));
  //    return pv;
  //  }
  //
  //  public static ParameterValue getPV(Parameter parameter, long time, Value v) {
  //    ParameterValue pv = getNewPv(parameter, time);
  //    pv.setEngValue(v);
  //    return pv;
  //  }

  @Override
  public Status getLinkStatus() {
    // TODO Auto-generated method stub
    return Status.OK;
  }

  @Override
  public boolean isDisabled() {
    // TODO Auto-generated method stub
    return linkStatus == Status.DISABLED;
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

  private OpcUaClient configureClient() throws Exception {
    Path securityTempDir = Paths.get(System.getProperty("java.io.tmpdir"), "client", "security");
    Files.createDirectories(securityTempDir);
    if (!Files.exists(securityTempDir)) {
      throw new Exception("unable to create security dir: " + securityTempDir);
    }

    File pkiDir = securityTempDir.resolve("pki").toFile();

    LoggerFactory.getLogger(getClass()).info("security dir: {}", securityTempDir.toAbsolutePath());
    LoggerFactory.getLogger(getClass()).info("security pki dir: {}", pkiDir.getAbsolutePath());

    System.out.println("pkiDir.getAbsolutePath():" + pkiDir.getAbsolutePath());
    trustListManager = new DefaultTrustListManager(pkiDir);
    List<EndpointDescription> endpoints = DiscoveryClient.getEndpoints(discoverURL).get();

    //    FIXME:At the moment, we do not support certificates...
    EndpointDescription selectedEndpoint = null;
    for (var endpoint : endpoints) {
      switch (endpoint.getSecurityMode()) {
        case Invalid:
          //			FIXME:Add log message
          break;
        case None:
          //			FIXME:Add log message
          selectedEndpoint = endpoint;
          break;
          //			FIXME:Add log message
        case Sign:
          break;
        case SignAndEncrypt:
          //			FIXME:Add log message
          break;
        default:
          break;
      }

      if (selectedEndpoint != null) {
        break;
      }
    }

    //    CertificateFactory fact = CertificateFactory.getInstance("X.509");
    //    X509Certificate cer =
    //        (X509Certificate)
    //            fact.generateCertificate(
    //                new ByteArrayInputStream(endpoints.get(2).getServerCertificate().bytes()));
    //    KeyStoreLoader loader = new KeyStoreLoader().loadFromCert(securityTempDir, cer);
    //
    //    trustListManager.addTrustedCertificate(loader.getClientCertificate());

    if (selectedEndpoint == null) {
      throw new Exception("No viable endpoint found from list:" + endpoints);
    }

    OpcUaClientConfig builder = OpcUaClientConfig.builder().setEndpoint(selectedEndpoint).build();

    return OpcUaClient.create(builder);
  }

  private void browseNodes(String indent, OpcUaClient client, NodeId browseRoot) {
    //     ObjectNode
    List<? extends UaNode> nodes = null;
    try {
      nodes =
          client
              .getAddressSpace()
              .browseNodes(
                  browseRoot,
                  BrowseOptions.builder()
                      .setNodeClassMask(
                          uint(NodeClass.Object.getValue() | NodeClass.Variable.getValue()))
                      .build());
    } catch (UaException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    //     System.out.println("Regular nodes-->" + nodes);

    for (UaNode node : nodes) {
      //       // logger.info("{} Node={}", indent, node.getBrowseName().getName());
      System.out.println("NODE:" + node.getNodeId());

      //       // recursively browse to children
      //            browseNode(indent + "  ", client, node.getNodeId());
    }
  }

  private void browseNodeWithReferences(String indent, OpcUaClient client, NodeId browseRoot) {
    BrowseDescription browse =
        new BrowseDescription(
            browseRoot,
            BrowseDirection.Forward,
            Identifiers.References,
            true,
            uint(NodeClass.Object.getValue() | NodeClass.Variable.getValue()),
            uint(BrowseResultMask.All.getValue()));

    try {

      System.out.println("browseNode1");
      BrowseResult browseResult = client.browse(browse).get();

      List<ReferenceDescription> references = toList(browseResult.getReferences());

      System.out.println("browseNode2:" + references);

      if (references.isEmpty()) {

        //    	  browseRoot.findMemberNodeId()
        System.out.println("Empty list, return:" + references);
        System.out.println("node with empty list:" + browseRoot.getType());

        return;
      }

      for (ReferenceDescription rd : references) {
        Object desc = null;
        Object value = null;
        UaNode node = null;
        try {

          node =
              client
                  .getAddressSpace()
                  .getNode(rd.getNodeId().toNodeId(client.getNamespaceTable()).get());
          DataValue attr = node.readAttribute(AttributeId.Description);
          desc = attr.getValue().getValue();

          attr = node.readAttribute(AttributeId.Value);

          System.out.println("browseNode3");

          value = attr.getValue();

        } catch (UaException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }

        if (node != null) {
          addOPCUAPV(client, node);

          log.debug(
              "{} Node={}, Desc={}, Value={}", indent, rd.getBrowseName().getName(), desc, value);

          if (rd.getIsForward()) {}

          // recursively browse to children
          rd.getNodeId()
              .toNodeId(client.getNamespaceTable())
              .ifPresent(nodeId -> browseNodeWithReferences(indent + "  ", client, nodeId));
        }
      }

      System.out.println("browseRoot:" + browseRoot.toParseableString());

    } catch (InterruptedException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    } catch (ExecutionException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
  }

  private void addOPCUAPV(OpcUaClient client, UaNode node) {
    if (node.getBrowseName()
        .getName()
        .contains(Character.toString(NameDescription.PATH_SEPARATOR))) {
      log.info(
          "{} ignored since it contains a {} character",
          node.getBrowseName().getName(),
          Character.toString(NameDescription.PATH_SEPARATOR));

      System.out.println("IGNORING:" + node.getBrowseName().getName());
    } else {

      //        FIXME:Remember to re-use these params (Do NOT create new objects when pushing
      // PVs
      // out to streams)

      /**
       * NOTE:For now we'll just flatten all the attributes instead of using an aggregate type for
       * attributes
       */
      //          p.setParameterType(opcuaAttrsType);

      for (AttributeId attr : AttributeId.values()) {

        ParameterType ptype = getBasicType(mdb, Yamcs.Value.Type.STRING);

        String opcuaTranslatedQName = translateNodeToParamQName(client, node, attr);
        Parameter p = VariableParam.getForFullyQualifiedName(opcuaTranslatedQName);

        p.setParameterType(ptype);

        //        TODO:Add Map of node_id -> Params
        if (mdb.getParameter(p.getQualifiedName()) == null) {
          log.debug("Adding OPCUA object as parameter to mdb:{}", p.getQualifiedName());
          mdb.addParameter(p, true);

          nodeIDToParamsMap.put(new NodeIDAttrPair(node.getNodeId(), attr), (VariableParam) p);
        }
      }
    }
  }

  private String translateNodeToParamQName(OpcUaClient client, UaNode node, AttributeId attr) {

    //    UaNode node = null;
    //    try {
    //      node =
    //          client
    //              .getAddressSpace()
    //              .getNode(rd.getNodeId().toNodeId(client.getNamespaceTable()).get());
    //    } catch (UaException e) {
    //      // TODO Auto-generated catch block
    //      e.printStackTrace();
    //    }
    LocalizedText localizedDisplayName = null;
    try {

      localizedDisplayName =
          (LocalizedText) (node.readAttribute(AttributeId.DisplayName).getValue().getValue());
    } catch (UaException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    String opcuaTranslatedQName =
        qualifiedName(
            parametersNamespace
                + NameDescription.PATH_SEPARATOR
                + node.getNodeId().toParseableString().replace(";", "-")
                + NameDescription.PATH_SEPARATOR
                + localizedDisplayName.getText(),
            attr.toString());

    return opcuaTranslatedQName;
  }

  /**
   * Same as translateNodeToParamQName but uses display name from the node instead of the
   *
   * @param client
   * @param rd
   * @param attr
   * @return
   */
  private String translateNodeDisplayNameToParamQName(
      OpcUaClient client, ReferenceDescription rd, AttributeId attr) {
    String opcuaTranslatedQName =
        qualifiedName(
            parametersNamespace
                + NameDescription.PATH_SEPARATOR
                + rd.getNodeId()
                    .toNodeId(client.getNamespaceTable())
                    .get()
                    .toParseableString()
                    .replace(";", "-"),
            attr.toString());

    UaNode node = null;
    try {
      node =
          client
              .getAddressSpace()
              .getNode(rd.getNodeId().toNodeId(client.getNamespaceTable()).get());
    } catch (UaException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    String displayName = null;
    LocalizedText localizedDisplayName = null;
    try {
      displayName = node.readAttribute(AttributeId.DisplayName).getValue().getValue().toString();

      System.out.println(
          "Intrinsic object type-->"
              + node.readAttribute(AttributeId.DisplayName).getValue().getValue().getClass());

      localizedDisplayName =
          (LocalizedText) (node.readAttribute(AttributeId.DisplayName).getValue().getValue());
    } catch (UaException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    //	    String opcuaTranslatedQName =
    //	        qualifiedName(
    //	            parametersNamespace + NameDescription.PATH_SEPARATOR +
    // localizedDisplayName.getText(),
    //	            attr.toString());
    return opcuaTranslatedQName;
  }

  /**
   * @param indent
   * @param client
   * @param browseRoot
   * @param nodePath in the format of "0:Root,0:Objects,2:HelloWorld,2:MyObject,2:Bar"
   */
  private void browsePath(String indent, OpcUaClient client, NodeId startingNode, String nodePath) {

    System.out.println("startingNode-->" + startingNode);
    ArrayList<String> rPathTokens = new ArrayList<String>();
    ArrayList<RelativePathElement> relaitivePathElements = new ArrayList<RelativePathElement>();

    for (var pathToken : nodePath.split(",")) {
      rPathTokens.add(nodePath);

      int namespaceIndex = 0;

      String namespaceName = "";

      namespaceIndex = Integer.parseInt(pathToken.split(":")[0]);

      namespaceName = pathToken.split(":")[1];

      relaitivePathElements.add(
          new RelativePathElement(
              Identifiers.HierarchicalReferences,
              false,
              true,
              new QualifiedName(namespaceIndex, namespaceName)));
    }

    ArrayList<BrowsePath> list = new ArrayList<BrowsePath>();

    RelativePathElement[] elements = new RelativePathElement[relaitivePathElements.size()];

    relaitivePathElements.toArray(elements);

    list.add(new BrowsePath(startingNode, new RelativePath(elements)));

    TranslateBrowsePathsToNodeIdsResponse response = null;
    try {
      response = client.translateBrowsePaths(list).get();
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (ExecutionException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    BrowsePathResult result = Arrays.asList(response.getResults()).get(0);
    StatusCode statusCode = result.getStatusCode();

    if (statusCode.isBad()) {
      //    	FIXME:Make all these prints log messages
      System.out.println("Bad status code:" + statusCode);
      return;
    }
    System.out.println("statusCode-->" + statusCode);
    //          logger.info("Status={}", statusCode);

    System.out.println(
        "node id from relative path"
            + result.getTargets()[0].getTargetId().toNodeId(client.getNamespaceTable()));

    try {
      UaNode node =
          client
              .getAddressSpace()
              .getNode(
                  result.getTargets()[0].getTargetId().toNodeId(client.getNamespaceTable()).get());

      System.out.println("Node from path--->" + node);

      addOPCUAPV(client, node);

      for (AttributeId attr : AttributeId.VARIABLE_ATTRIBUTES) {
        String value = "";
        if (node.readAttribute(attr).getValue().isNull()) {
          value = "NULL";
        } else {
          value = node.readAttribute(attr).getValue().getValue().toString();
        }

        System.out.println("value:" + value);

        //                log.debug("Pushing {} to stream", p.toString());

      }
    } catch (UaException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    System.out.println(
        "targets:" + result.getTargets()[0].getTargetId().toNodeId(client.getNamespaceTable()));

    l(result.getTargets())
        .forEach(target -> System.out.println("TargetId={}" + target.getTargetId()));

    //          future.complete(client);

  }

  private void createOPCUASubscriptions() {
    createDataChangeListener();
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
            //          case DataType:
            //            break;
            //          case Method:
            //            break;
            //          case Object:
            //            break;
            //          case ObjectType:
            //            break;
            //          case ReferenceType:
            //            break;
            //          case Unspecified:
            //            break;
          case Variable:
            ManagedDataItem dataItem = opcuaSubscription.createDataItem(id);
            log.debug("Status code for dataItem:{}", dataItem.getStatusCode());
            break;
            //          case VariableType:
            //            break;
            //          case View:
            //            break;
            //          default:
            //            break;
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
    System.out.println("Connecting...");
    client.connect().get();
  }

  /**
   * Browses the tree on the OPCUA server and maps them to YAMCS Parameters.
   *
   * @param client
   * @param future
   */
  private void browseOPCUATree(OpcUaClient client, CompletableFuture<OpcUaClient> future) {
    // start browsing at root folder
    System.out.println("Browsing node...");

    for (var p : relativeNodePaths) {

      int namespaceIndex = (int) p.rootNodeID.get("namespaceIndex");

      String identifier = (String) p.rootNodeID.get("identifier");
      IdType identifierType = IdType.valueOf((String) p.rootNodeID.get("identifierType"));

      browsePath(
          endpointURL, client, getNewNodeID(identifierType, namespaceIndex, identifier), p.path);
    }

    NodeId nodeID = null;
    nodeID = getNewNodeID(rootIdentifierType, rootNamespaceIndex, rootIdentifier);

    //  FIXME:Make root default when no namespaceIndex/identifier pair is specified
    browseNodeWithReferences("", client, nodeID);

    future.complete(client);
  }

  private NodeId getNewNodeID(IdType rootIdentifierType, int NamespaceIndex, String Identifier) {
    NodeId nodeID = null;
    switch (rootIdentifierType) {
      case Guid:
        //		FIXME
        break;
      case Numeric:
        nodeID = new NodeId(NamespaceIndex, Integer.parseInt(Identifier));
        //        browseNodes(endpointURL, client, nodeID);
        break;
      case Opaque:
        //		FIXME
        break;
      case String:
        nodeID = new NodeId(NamespaceIndex, Identifier);
        //        browseNodes(endpointURL, client, nodeID);
        break;
      default:
        break;
    }
    return nodeID;
  }

  private void createDataChangeListener() {
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
            cols.add(parametersNamespace);
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
             * it is concrete in the OPCUA protocol what data can change in real time and which data
             * is "static". Not sure if there is any "static" data given that clients have the
             * ability of writing to values... might be worth a test.
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
                  nodeIDToParamsMap.get(nodeAttrKey).getQualifiedName(), DataType.PARAMETER_VALUE);

              cols.add(
                  getPV(
                      nodeIDToParamsMap.get(nodeAttrKey),
                      Instant.now().toEpochMilli(),
                      values.get(i).getValue().toString()));

              pushTuple(tdef, cols);
              inCount.getAndAdd(1);
            } else {
              // TODO:Add some type emptyValue count for OPS.
              log.warn(
                  "Data chnage triggered for {}, but it empty. This should not happen.",
                  nodeIDToParamsMap.get(nodeAttrKey).getQualifiedName());
            }
          }
        });
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
      opcuaAttrsTypeBuidlder.addMember(new Member(attr.toString(), getBasicType(mdb, Type.STRING)));
    }

    opcuaAttrsType = opcuaAttrsTypeBuidlder.build();
    ((NameDescription) opcuaAttrsType)
        .setQualifiedName(qualifiedName(parametersNamespace, opcuaAttrsType.getName()));
  }

  //  private ParameterType OPCUAAttrTypeToParamType(AttributeId attr) {
  //    ParameterType pType = null;
  //
  //    switch (attr) {
  //      case AccessLevel:
  //        break;
  //      case ArrayDimensions:
  //        break;
  //      case BrowseName:
  //        pType = getBasicType(mdb, Type.STRING, null);
  //        break;
  //      case ContainsNoLoops:
  //        break;
  //      case DataType:
  //        break;
  //      case Description:
  //        pType = getBasicType(mdb, Type.STRING, null);
  //        break;
  //      case DisplayName:
  //        pType = getBasicType(mdb, Type.STRING, null);
  //        break;
  //      case EventNotifier:
  //        break;
  //      case Executable:
  //        break;
  //      case Historizing:
  //        break;
  //      case InverseName:
  //        pType = getBasicType(mdb, Type.STRING, null);
  //        break;
  //      case IsAbstract:
  //        break;
  //      case MinimumSamplingInterval:
  //        break;
  //      case NodeClass:
  //        break;
  //      case NodeId:
  //        pType = getBasicType(mdb, Type.STRING, null);
  //        break;
  //      case Symmetric:
  //        break;
  //      case UserAccessLevel:
  //        break;
  //      case UserExecutable:
  //        break;
  //      case UserWriteMask:
  //        break;
  //      case Value:
  //        break;
  //      case ValueRank:
  //        break;
  //      case WriteMask:
  //        break;
  //      default:
  //        break;
  //    }
  //
  //    return pType;
  //  }

  private void subscribeToEvents(OpcUaClient client)
      throws InterruptedException, ExecutionException {
    // create a subscription and a monitored item
    UaSubscription subscription = client.getSubscriptionManager().createSubscription(1000.0).get();

    ReadValueId readValueId =
        new ReadValueId(
            Identifiers.Server, AttributeId.EventNotifier.uid(), null, QualifiedName.NULL_VALUE);

    // client handle must be unique per item
    UInteger clientHandle = uint(clientHandles.getAndIncrement());

    EventFilter eventFilter =
        new EventFilter(
            new SimpleAttributeOperand[] {
              new SimpleAttributeOperand(
                  Identifiers.BaseEventType,
                  new QualifiedName[] {new QualifiedName(0, "EventId")},
                  AttributeId.Value.uid(),
                  null),
              new SimpleAttributeOperand(
                  Identifiers.BaseEventType,
                  new QualifiedName[] {new QualifiedName(0, "EventType")},
                  AttributeId.Value.uid(),
                  null),
              new SimpleAttributeOperand(
                  Identifiers.BaseEventType,
                  new QualifiedName[] {new QualifiedName(0, "Severity")},
                  AttributeId.Value.uid(),
                  null),
              new SimpleAttributeOperand(
                  Identifiers.BaseEventType,
                  new QualifiedName[] {new QualifiedName(0, "Time")},
                  AttributeId.Value.uid(),
                  null),
              new SimpleAttributeOperand(
                  Identifiers.BaseEventType,
                  new QualifiedName[] {new QualifiedName(0, "Message")},
                  AttributeId.Value.uid(),
                  null)
            },
            new ContentFilter(null));

    MonitoringParameters parameters =
        new MonitoringParameters(
            clientHandle,
            0.0,
            ExtensionObject.encode(client.getStaticSerializationContext(), eventFilter),
            uint(10),
            true);

    MonitoredItemCreateRequest request =
        new MonitoredItemCreateRequest(readValueId, MonitoringMode.Reporting, parameters);

    List<UaMonitoredItem> items =
        subscription.createMonitoredItems(TimestampsToReturn.Both, newArrayList(request)).get();

    // do something with the value updates
    UaMonitoredItem monitoredItem = items.get(0);

    final AtomicInteger eventCount = new AtomicInteger(0);

    monitoredItem.setEventConsumer(
        (item, vs) -> {
          //            logger.info(
          //                "Event Received from {}",
          //                item.getReadValueId().getNodeId());

          System.out.println("Event Received from" + item.getReadValueId().getNodeId());

          for (int i = 0; i < vs.length; i++) {
            //                logger.info("\tvariant[{}]: {}", i, vs[i].getValue());
            System.out.println("tvariant:" + vs[i].getValue());
          }

          if (eventCount.incrementAndGet() == 3) {
            //                future.complete(client);
          }
        });
  }
}
