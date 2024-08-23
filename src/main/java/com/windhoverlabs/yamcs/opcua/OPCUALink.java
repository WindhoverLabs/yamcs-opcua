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
import static org.yamcs.xtce.NameDescription.qualifiedName;

import com.google.gson.JsonObject;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.temporal.ChronoUnit;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
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
import org.eclipse.milo.opcua.stack.core.types.builtin.ByteString;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.DateTime;
import org.eclipse.milo.opcua.stack.core.types.builtin.ExtensionObject;
import org.eclipse.milo.opcua.stack.core.types.builtin.LocalizedText;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.QualifiedName;
import org.eclipse.milo.opcua.stack.core.types.builtin.StatusCode;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UInteger;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UShort;
import org.eclipse.milo.opcua.stack.core.types.enumerated.IdType;
import org.eclipse.milo.opcua.stack.core.types.enumerated.MonitoringMode;
import org.eclipse.milo.opcua.stack.core.types.enumerated.NodeClass;
import org.eclipse.milo.opcua.stack.core.types.enumerated.TimestampsToReturn;
import org.eclipse.milo.opcua.stack.core.types.structured.BrowsePath;
import org.eclipse.milo.opcua.stack.core.types.structured.BrowsePathResult;
import org.eclipse.milo.opcua.stack.core.types.structured.ContentFilter;
import org.eclipse.milo.opcua.stack.core.types.structured.EndpointDescription;
import org.eclipse.milo.opcua.stack.core.types.structured.EventFilter;
import org.eclipse.milo.opcua.stack.core.types.structured.MonitoredItemCreateRequest;
import org.eclipse.milo.opcua.stack.core.types.structured.MonitoringParameters;
import org.eclipse.milo.opcua.stack.core.types.structured.ReadValueId;
import org.eclipse.milo.opcua.stack.core.types.structured.RelativePath;
import org.eclipse.milo.opcua.stack.core.types.structured.RelativePathElement;
import org.eclipse.milo.opcua.stack.core.types.structured.SimpleAttributeOperand;
import org.eclipse.milo.opcua.stack.core.types.structured.TranslateBrowsePathsToNodeIdsResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yamcs.ConfigurationException;
import org.yamcs.Spec;
import org.yamcs.Spec.OptionType;
import org.yamcs.StandardTupleDefinitions;
import org.yamcs.ValidationException;
import org.yamcs.YConfiguration;
import org.yamcs.YamcsServer;
import org.yamcs.http.NotFoundException;
import org.yamcs.mdb.XtceAssembler;
import org.yamcs.parameter.ParameterValue;
import org.yamcs.parameter.SystemParametersProducer;
import org.yamcs.parameter.SystemParametersService;
import org.yamcs.protobuf.Event.EventSeverity;
import org.yamcs.protobuf.Yamcs.NamedObjectId;
import org.yamcs.protobuf.Yamcs.Value.Type;
import org.yamcs.tctm.AbstractLink;
import org.yamcs.tctm.Link;
import org.yamcs.tctm.LinkAction;
import org.yamcs.utils.ValueUtility;
import org.yamcs.xtce.BooleanParameterType;
import org.yamcs.xtce.EnumeratedParameterType;
import org.yamcs.xtce.FloatParameterType;
import org.yamcs.xtce.IntegerParameterType;
import org.yamcs.xtce.NameDescription;
import org.yamcs.xtce.Parameter;
import org.yamcs.xtce.ParameterType;
import org.yamcs.xtce.SpaceSystem;
import org.yamcs.xtce.StringParameterType;
import org.yamcs.xtce.XtceDb;
import org.yamcs.yarch.DataType;
import org.yamcs.yarch.Stream;
import org.yamcs.yarch.Tuple;
import org.yamcs.yarch.TupleDefinition;
import org.yamcs.yarch.YarchDatabase;
import org.yamcs.yarch.YarchDatabaseInstance;
import org.yamcs.yarch.protobuf.Db.Event;

/**
 * Implementation of the OPCUA protocol as a YAMCS link. Maps configured nodes(see docs for details)
 * to yamcs PVs and subscribes to OPCUA variables for realtime updates.
 *
 * @author Lorenzo Gomez
 */
public class OPCUALink extends AbstractLink implements Runnable, SystemParametersProducer {

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

  /** Useful status for tracking initialization status of the link. */
  public enum OPCUAINITStatus {
    OPCUA_INIT_CONFIG,
    OPCUA_INIT_TREE,
    OPCUA_INIT_TREE_FAILED,
    OPCUA_INIT_GENERATE_XTCE,
    OPCUA_INIT_EVENTS,
    OPCUA_INIT_DATA_SUBSCRIPTION,
    OPCUA_INIT_ALL_DATA_QUERY,
    OPCUA_INIT_OK
  }

  /* Configuration Defaults */
  static final String STREAM_NAME = "opcua_params";

  /* Internal member attributes. */
  protected Thread thread;
  private String opcuaStreamName;
  private String parametersNamespace;
  XtceDb mdb;
  Stream opcuaStream;
  private static TupleDefinition gftdef = StandardTupleDefinitions.PARAMETER.copy();
  private ManagedSubscription opcuaSubscription;

  private static final Logger internalLogger = LoggerFactory.getLogger(OPCUALink.class.getName());

  /**
   * @note ALWAYS re-use params as org.yamcs.parameter.ParameterRequestManager.param2RequestMap uses
   *     the object inside a map that was added to the mdb for the very fist time. If when
   *     publishing the PV, we create a new VariableParam object clients will NOT receive real-time
   *     updates as the new object VariableParam inside the new PV won't match the one inside
   *     org.yamcs.parameter.ParameterRequestManager.param2RequestMap since the object hashes do not
   *     match (since VariableParam does not override its hash function).
   */
  private ConcurrentHashMap<NodeIDAttrPair, Parameter> nodeIDToParamsMap =
      new ConcurrentHashMap<NodeIDAttrPair, Parameter>();

  private OpcUaClient client;

  protected AtomicLong inCount = new AtomicLong(0);

  // realtimeCount is the same as inCount, except that it cannot be reset by users.
  //  Used specifically for deciding subStrikeCount
  protected AtomicLong realtimeCount = new AtomicLong(0);

  protected AtomicLong subStrikeCount = new AtomicLong(0);

  protected AtomicLong lastRealtimeCount = new AtomicLong(0);

  protected int subStrikeCountThreshold;

  private long subStrikeCountCheckTimeoutSecs;

  private Status linkStatus = Status.OK;

  private boolean enabledAtStartup = false;

  private boolean useGroundTimeForQueryAllNodes = false;

  private boolean useGroundTimeForRealtimeData = false;

  /* Configuration Parameters */

  private String discoverURL;
  private String endpointURL;
  private boolean queryAllNodesAtStartup;
  private String outputFile;
  private int publishInterval; // milliseconds

  private ArrayList<NodePath> relativeNodePaths = new ArrayList<NodePath>();

  private final AtomicLong clientHandles = new AtomicLong(1L);

  /* System parameters*/

  private Parameter OPCUAInitStatusParam;
  private OPCUAINITStatus currentOPCUAStatus = OPCUAINITStatus.OPCUA_INIT_CONFIG;
  private Parameter OPCUAActiveSubsParam;
  private Parameter realtimeCountParam;
  private Parameter lastRealtimeCountParam;

  private Parameter subStrikeCountCheckTimeoutSecsParam;

  private Parameter subStrikeCountParam;
  private Parameter subStrikeCountThresholdParam;
  private AtomicLong OPCUAActiveSubs = new AtomicLong(0);

  private int reconnectCount = 0;
  private Parameter reconnectCountParam;

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

  LinkAction reconnectAction =
      new LinkAction("reconnect", "Reconnect to server.") {
        @Override
        public JsonObject execute(Link link, JsonObject jsonObject) {

          internalLogger.info("Executing query_all action");
          CompletableFuture.supplyAsync(
                  (Supplier<Integer>)
                      () -> {
                        reconnect();

                        return 0;
                      })
              .whenComplete(
                  (vaue, e) -> {
                    internalLogger.info("query_all action Complete");
                  });

          return jsonObject;
        }
      };

  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

  public OPCUAINITStatus getCurrentOPCUAStatus() {
    return currentOPCUAStatus;
  }

  @Override
  public Spec getSpec() {
    Spec spec = new Spec();

    /* Define our configuration parameters. */
    spec.addOption("name", OptionType.STRING).withRequired(true);
    spec.addOption("class", OptionType.STRING).withRequired(true);
    spec.addOption("opcuaStream", OptionType.STRING).withRequired(true);
    spec.addOption("endpointUrl", OptionType.STRING).withRequired(true);
    spec.addOption("discoveryUrl", OptionType.STRING).withRequired(true);
    spec.addOption("xtceOutputFile", OptionType.STRING).withRequired(true);
    spec.addOption("parametersNamespace", OptionType.STRING).withRequired(true);
    spec.addOption("publishInterval", OptionType.INTEGER).withRequired(true);
    spec.addOption("subStrikeCountThreshold", OptionType.INTEGER)
        .withDefault(3)
        .withRequired(false);

    spec.addOption("subStrikeCountCheckTimeoutSecs", OptionType.INTEGER)
        .withDefault(15)
        .withRequired(false);

    spec.addOption("queryAllNodesAtStartup", OptionType.BOOLEAN).withRequired(false);

    spec.addOption("enabledAtStartup", OptionType.BOOLEAN).withRequired(true);

    spec.addOption("useGroundTimeForQueryAllNodes", OptionType.BOOLEAN)
        .withRequired(false)
        .withDefault(false);

    spec.addOption("useGroundTimeForRealtimeData", OptionType.BOOLEAN)
        .withRequired(false)
        .withDefault(false);

    Spec rootNodeIDSpec = new Spec();

    rootNodeIDSpec.addOption("namespaceIndex", OptionType.INTEGER).withRequired(true);
    rootNodeIDSpec.addOption("identifier", OptionType.STRING).withRequired(true);
    rootNodeIDSpec.addOption("identifierType", OptionType.STRING).withRequired(true);

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
      notifyFailed(e);
    }
    YarchDatabaseInstance ydb = YarchDatabase.getInstance(yamcsInstance);

    this.opcuaStreamName = config.getString("opcuaStream");
    this.opcuaStream = getStream(ydb, opcuaStreamName);
    this.parametersNamespace = config.getString("parametersNamespace");
    this.mdb = YamcsServer.getServer().getInstance(yamcsInstance).getXtceDb();

    readOPCUAConfig(config);
    readNodePathsConfig(config);

    outputFile = config.getString("xtceOutputFile");

    subStrikeCountThreshold = config.getInt("subStrikeCountThreshold");

    subStrikeCountCheckTimeoutSecs = config.getInt("subStrikeCountCheckTimeoutSecs");

    enabledAtStartup = config.getBoolean("enabledAtStartup");

    useGroundTimeForRealtimeData = config.getBoolean("useGroundTimeForRealtimeData", false);

    useGroundTimeForQueryAllNodes = config.getBoolean("useGroundTimeForQueryAllNodes", false);

    if (!enabledAtStartup) {
      linkStatus = Status.DISABLED;
      super.disable();
    }
  }

  private void readOPCUAConfig(YConfiguration config) {
    this.endpointURL = config.getString("endpointUrl");
    this.discoverURL = config.getString("discoveryUrl");
    this.publishInterval = config.getInt("publishInterval");
    this.queryAllNodesAtStartup = config.getBoolean("queryAllNodesAtStartup", false);
  }

  private void readNodePathsConfig(YConfiguration config) {
    List<Map<Object, Object>> nodePaths = config.getList("nodePaths");

    for (Map<Object, Object> path : nodePaths) {
      NodePath nodePath = new NodePath();
      nodePath.path = (String) path.get("path");
      nodePath.rootNodeID = (HashMap<Object, Object>) path.get("rootNodeID");
      relativeNodePaths.add(nodePath);
    }
  }

  private static SpaceSystem verifySpaceSystem(XtceDb mdb, String pathName) {
    String namespace;
    String name;
    int lastSlash = pathName.lastIndexOf('/');
    if ("/".equals(pathName)) {
      namespace = "";
      name = "";
    } else if (lastSlash == -1 || lastSlash == pathName.length() - 1) {
      namespace = "";
      name = pathName;
    } else {
      namespace = pathName.substring(0, lastSlash);
      name = pathName.substring(lastSlash + 1);
    }

    // First try with a prefixed slash (should be the common case)
    NamedObjectId id =
        NamedObjectId.newBuilder().setNamespace("/" + namespace).setName(name).build();
    SpaceSystem spaceSystem = mdb.getSpaceSystem(id);
    if (spaceSystem != null) {
      return spaceSystem;
    }

    // Maybe some non-xtce namespace like MDB:OPS Name
    id = NamedObjectId.newBuilder().setNamespace(namespace).setName(name).build();
    spaceSystem = mdb.getSpaceSystem(id);
    if (spaceSystem != null) {
      return spaceSystem;
    }

    throw new NotFoundException("No such space system");
  }

  /**
   * Initializes all PV mappings to OPCUA nodes and realtime subscriptions(managed data items in
   * OPCUA terms).
   */
  private void opcuaInit() {
    try {

      currentOPCUAStatus = OPCUAINITStatus.OPCUA_INIT_TREE;
      browseOPCUATree(client);
      currentOPCUAStatus = OPCUAINITStatus.OPCUA_INIT_GENERATE_XTCE;
      exportXTCE();

    } catch (Exception e) {
      internalLogger.warn(e.toString());
      currentOPCUAStatus = OPCUAINITStatus.OPCUA_INIT_TREE_FAILED;
      return;
    }
    try {
      currentOPCUAStatus = OPCUAINITStatus.OPCUA_INIT_EVENTS;
      subscribeToEvents(client);
      currentOPCUAStatus = OPCUAINITStatus.OPCUA_INIT_DATA_SUBSCRIPTION;
      createOPCUASubscriptions();
    } catch (Exception e) {
      internalLogger.warn(e.toString());
      return;
    }
    if (queryAllNodesAtStartup) {
      currentOPCUAStatus = OPCUAINITStatus.OPCUA_INIT_ALL_DATA_QUERY;
      queryAllOPCUAData();
    }

    currentOPCUAStatus = OPCUAINITStatus.OPCUA_INIT_OK;
  }

  private void exportXTCE() throws IOException {
    var spaceSystem = verifySpaceSystem(mdb, parametersNamespace);
    var xtce = new XtceAssembler().toXtce(mdb, spaceSystem.getQualifiedName(), fqn -> true);
    BufferedWriter writer = null;

    if (outputFile != null) {
      writer =
          Files.newBufferedWriter(
              Paths.get(outputFile),
              StandardOpenOption.CREATE,
              StandardOpenOption.TRUNCATE_EXISTING);

      writer.write(xtce);

      writer.flush();
      writer.close();
    }
  }

  private void opcuaClientConnect() throws Exception {
    client = configureClient();
    connectToOPCUAServer(client);
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

    try {
      if (client != null) {
        client.disconnect().get();
        OPCUAActiveSubs.set(0);
      }
    } catch (InterruptedException | ExecutionException e) {
      internalLogger.warn(e.toString());
    }
    if (thread != null) {
      thread.interrupt();
    }

    linkStatus = Status.DISABLED;
  }

  @Override
  public void doEnable() {
    try {
      opcuaClientConnect();
    } catch (Exception e) {
      internalLogger.warn(e.toString());
      linkStatus = Status.FAILED;
      notifyFailed(e);
      return;
    }

    startAction.addChangeListener(
        () -> {
          /**
           * TODO:Might be useful if we want turn off any functionality when the action is disabled
           * for instance..
           */
        });

    /* Create and start the new thread. */
    thread = new Thread(this);
    thread.setName(this.getClass().getSimpleName() + "-" + linkName);
    thread.start();
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

    notifyStarted();
  }

  @Override
  protected void doStop() {
    try {
      if (client != null) {
        client.disconnect().get();
      }
    } catch (InterruptedException | ExecutionException e) {
      internalLogger.warn(e.toString());
    }
    if (thread != null) {
      thread.interrupt();
    }

    notifyStopped();
  }

  @Override
  public void run() {
    opcuaInit();
    /* Enter our main loop */

    scheduleStrikeCountThread();

    while (isRunningAndEnabled()) {}
  }

  private void reconnect() {
    //            Reconnect to realtime data
    try {
      org.yamcs.yarch.protobuf.Db.Event ev =
          Event.newBuilder()
              .setGenerationTime(YamcsServer.getTimeService(yamcsInstance).getMissionTime())
              .setGenerationTime(YamcsServer.getTimeService(yamcsInstance).getMissionTime())
              .setSource(this.linkName)
              .setType(this.linkName)
              .setMessage(String.format("Disconnecting"))
              .setSeverity(EventSeverity.ERROR)
              .build();
      eventProducer.sendEvent(ev);

      if (client != null) {
        client.disconnect().get();
        OPCUAActiveSubs.set(0);
      }
    } catch (InterruptedException | ExecutionException e) {
      internalLogger.warn(e.toString());
    }

    try {
      opcuaClientConnect();
    } catch (Exception e) {
      internalLogger.warn(e.toString());
      linkStatus = Status.FAILED;

      org.yamcs.yarch.protobuf.Db.Event ev =
          Event.newBuilder()
              .setGenerationTime(YamcsServer.getTimeService(yamcsInstance).getMissionTime())
              .setGenerationTime(YamcsServer.getTimeService(yamcsInstance).getMissionTime())
              .setSource(this.linkName)
              .setType(this.linkName)
              .setMessage(String.format("Reconnect failed."))
              .setSeverity(EventSeverity.ERROR)
              .build();
      eventProducer.sendEvent(ev);
      notifyFailed(e);
      return;
    }

    try {
      org.yamcs.yarch.protobuf.Db.Event ev =
          Event.newBuilder()
              .setGenerationTime(YamcsServer.getTimeService(yamcsInstance).getMissionTime())
              .setGenerationTime(YamcsServer.getTimeService(yamcsInstance).getMissionTime())
              .setSource(this.linkName)
              .setType(this.linkName)
              .setMessage(String.format("Resubscribing to events and realtime values."))
              .setSeverity(EventSeverity.INFO)
              .build();
      eventProducer.sendEvent(ev);
      subscribeToEvents(client);
      createOPCUASubscriptions();
      linkStatus = Status.OK;
    } catch (Exception e) {

      org.yamcs.yarch.protobuf.Db.Event ev =
          Event.newBuilder()
              .setGenerationTime(YamcsServer.getTimeService(yamcsInstance).getMissionTime())
              .setGenerationTime(YamcsServer.getTimeService(yamcsInstance).getMissionTime())
              .setSource(this.linkName)
              .setType(this.linkName)
              .setMessage(String.format("Resubscribing failed. Exception info" + e.toString()))
              .setSeverity(EventSeverity.ERROR)
              .build();
      eventProducer.sendEvent(ev);

      internalLogger.warn(e.toString());
      return;
    }

    reconnectCount++;
    subStrikeCount.set(0);
  }

  private void scheduleStrikeCountThread() {
    scheduler.scheduleAtFixedRate(
        () -> {
          if (currentOPCUAStatus != OPCUAINITStatus.OPCUA_INIT_OK) {
            //            	We could be in the middle of a reconnect...
            return;
          }
          if (realtimeCount.get() > lastRealtimeCount.get()) {
            subStrikeCount.set(0);
          } else {
            subStrikeCount.getAndAdd(1);
          }

          if (subStrikeCount.intValue() > subStrikeCountThreshold) {
            org.yamcs.yarch.protobuf.Db.Event ev =
                Event.newBuilder()
                    .setGenerationTime(YamcsServer.getTimeService(yamcsInstance).getMissionTime())
                    .setGenerationTime(YamcsServer.getTimeService(yamcsInstance).getMissionTime())
                    .setSource(this.linkName)
                    .setType(this.linkName)
                    .setMessage(
                        String.format(
                            "Subscription strike count(%d) exceeded currently configured threshold(%d)",
                            subStrikeCount.intValue(), subStrikeCountThreshold))
                    .setSeverity(EventSeverity.ERROR)
                    .build();
            eventProducer.sendEvent(ev);

            // linkStatus = Status.UNAVAIL;

            //            Reconnect to realtime data
            // try {
            //   ev =
            //       Event.newBuilder()
            //
            // .setGenerationTime(YamcsServer.getTimeService(yamcsInstance).getMissionTime())
            //
            // .setGenerationTime(YamcsServer.getTimeService(yamcsInstance).getMissionTime())
            //           .setSource(this.linkName)
            //           .setType(this.linkName)
            //           .setMessage(String.format("Disconnecting"))
            //           .setSeverity(EventSeverity.ERROR)
            //           .build();
            //   eventProducer.sendEvent(ev);
            //   client.disconnect().get();
            // } catch (InterruptedException | ExecutionException e) {
            //   internalLogger.warn(e.toString());
            // }

            // try {
            //   opcuaClientConnect();
            // } catch (Exception e) {
            //   internalLogger.warn(e.toString());
            //   linkStatus = Status.FAILED;

            //   ev =
            //       Event.newBuilder()
            //
            // .setGenerationTime(YamcsServer.getTimeService(yamcsInstance).getMissionTime())
            //
            // .setGenerationTime(YamcsServer.getTimeService(yamcsInstance).getMissionTime())
            //           .setSource(this.linkName)
            //           .setType(this.linkName)
            //           .setMessage(String.format("Reconnect failed."))
            //           .setSeverity(EventSeverity.ERROR)
            //           .build();
            //   eventProducer.sendEvent(ev);
            //   notifyFailed(e);
            //   return;
            // }

            // try {
            //   ev =
            //       Event.newBuilder()
            //
            // .setGenerationTime(YamcsServer.getTimeService(yamcsInstance).getMissionTime())
            //
            // .setGenerationTime(YamcsServer.getTimeService(yamcsInstance).getMissionTime())
            //           .setSource(this.linkName)
            //           .setType(this.linkName)
            //           .setMessage(String.format("Resubscribing to events and realtime values."))
            //           .setSeverity(EventSeverity.INFO)
            //           .build();
            //   eventProducer.sendEvent(ev);
            //   subscribeToEvents(client);
            //   createOPCUASubscriptions();
            //   linkStatus = Status.OK;
            // } catch (Exception e) {

            //   ev =
            //       Event.newBuilder()
            //
            // .setGenerationTime(YamcsServer.getTimeService(yamcsInstance).getMissionTime())
            //
            // .setGenerationTime(YamcsServer.getTimeService(yamcsInstance).getMissionTime())
            //           .setSource(this.linkName)
            //           .setType(this.linkName)
            //           .setMessage(
            //               String.format("Resubscribing failed. Exception info" + e.toString()))
            //           .setSeverity(EventSeverity.ERROR)
            //           .build();
            //   eventProducer.sendEvent(ev);

            //   internalLogger.warn(e.toString());
            //   return;
            // }

            // reconnectCount++;

            // subStrikeCount.set(0);
          }

          lastRealtimeCount.set(realtimeCount.get());
        },
        1,
        subStrikeCountCheckTimeoutSecs,
        TimeUnit.SECONDS);
  }

  /**
   * Reads all attributes of all configured Value nodes and updates their corresponding PV. Useful
   * for querying data from the OPCUA server once, data such as browse names, NodeIds, etc.
   */
  private void queryAllOPCUAData() {
    Set<NodeId> nodeSet = new HashSet<NodeId>();
    /**
     * NOTE:This is super inefficient... The reason we collect these nodeIDs in a set is because
     * otherwise we will have redundant subscription(s) since there is more than 1 attribute per
     * nodeID given how nodeIDToParamsMap is designed
     */
    for (NodeIDAttrPair pair : nodeIDToParamsMap.keySet()) {
      nodeSet.add(pair.nodeID);
    }

    TupleDefinition tdef = gftdef.copy();

    List<Object> cols = new ArrayList<>(4 + nodeIDToParamsMap.keySet().size());

    tdef = gftdef.copy();
    long gentime = timeService.getMissionTime();
    cols.add(gentime);
    cols.add(parametersNamespace);
    cols.add(0);
    cols.add(gentime);

    for (NodeId nId : nodeSet) {
      UaNode node;

      try {
        node = client.getAddressSpace().getNode(nId);

        DataValue nodeClass = node.readAttribute(AttributeId.NodeClass);

        switch (NodeClass.from((int) nodeClass.getValue().getValue())) {
          case Variable:
            for (AttributeId attr : AttributeId.VARIABLE_ATTRIBUTES) {
              Parameter p = nodeIDToParamsMap.get(new NodeIDAttrPair(nId, attr));

              if (p.getParameterType() == null) {
                internalLogger.warn(
                    "{} ignored since it does not have a Parameter type",
                    p,
                    Character.toString(NameDescription.PATH_SEPARATOR));
                continue;
              }

              //            FIXME: Add leap seconds.... as config or get it from YAMCS API.

              if (node.readAttribute(attr).getValue().isNull()) {
                internalLogger.warn("{} Ignored since the data value is null", p);
                continue;
              }

              if (!useGroundTimeForQueryAllNodes) {
                gentime =
                    node.readAttribute(attr)
                        .getSourceTime()
                        .getJavaInstant()
                        .plus(37, ChronoUnit.SECONDS)
                        .toEpochMilli();
              }

              switch (p.getParameterType().getValueType()) {
                case BOOLEAN:
                  {
                    Boolean value = true;
                    if (node.readAttribute(attr).getValue().isNull()) {
                      internalLogger.warn(
                          "node {} has a Null variant. Ignoring and will not be pushed to stream.",
                          node);
                      continue;
                    } else {
                      value = (Boolean) node.readAttribute(attr).getValue().getValue();
                    }

                    tdef.addColumn(p.getQualifiedName(), DataType.PARAMETER_VALUE);
                    cols.add(getPV(p, gentime, value));
                  }
                  break;
                case DOUBLE:
                  {
                    Number value = 0;
                    if (node.readAttribute(attr).getValue().isNull()) {
                      internalLogger.warn(
                          "node {} has a Null variant. Ignoring and will not be pushed to stream.",
                          node);
                      continue;
                    } else {
                      value = (Number) node.readAttribute(attr).getValue().getValue();
                    }

                    tdef.addColumn(p.getQualifiedName(), DataType.PARAMETER_VALUE);
                    cols.add(getPV(p, gentime, value.doubleValue()));
                  }
                  break;
                case FLOAT:
                  {
                    Number value = 0;
                    if (node.readAttribute(attr).getValue().isNull()) {
                      internalLogger.warn(
                          "node {} has a Null variant. Ignoring and will not be pushed to stream.",
                          node);
                      continue;
                    } else {
                      value = (Number) node.readAttribute(attr).getValue().getValue();
                    }

                    tdef.addColumn(p.getQualifiedName(), DataType.PARAMETER_VALUE);
                    cols.add(getPV(p, gentime, value.floatValue()));
                  }
                  break;
                case SINT32:
                  {
                    Number value = 0;
                    if (node.readAttribute(attr).getValue().isNull()) {
                      internalLogger.warn(
                          "node {} has a Null variant. Ignoring and will not be pushed to stream.",
                          node);
                      continue;
                    } else {
                      value = (Number) node.readAttribute(attr).getValue().getValue();
                    }

                    tdef.addColumn(p.getQualifiedName(), DataType.PARAMETER_VALUE);
                    cols.add(getPV(p, gentime, value.intValue()));
                  }
                  break;
                case SINT64:
                  {
                    Number value = 0;
                    if (node.readAttribute(attr).getValue().isNull()) {
                      internalLogger.warn(
                          "node {} has a Null variant. Ignoring and will not be pushed to stream.",
                          node);
                      continue;
                    } else {
                      value = (Number) node.readAttribute(attr).getValue().getValue();
                    }

                    tdef.addColumn(p.getQualifiedName(), DataType.PARAMETER_VALUE);
                    cols.add(getPV(p, gentime, value.longValue()));
                  }
                  break;
                case STRING:
                  {
                    String value = "";
                    if (node.readAttribute(attr).getValue().isNull()) {
                      internalLogger.warn(
                          "node {} has a Null variant. Ignoring and will not be pushed to stream.",
                          node);
                      continue;
                    } else {
                      value = node.readAttribute(attr).getValue().getValue().toString();
                    }

                    tdef.addColumn(p.getQualifiedName(), DataType.PARAMETER_VALUE);
                    cols.add(getPV(p, gentime, value));
                  }
                  break;
                case UINT32:
                  {
                    Number value = 0;
                    if (node.readAttribute(attr).getValue().isNull()) {
                      internalLogger.warn(
                          "node {} has a Null variant. Ignoring and will not be pushed to stream.",
                          node);
                      continue;
                    } else {
                      value = (Number) node.readAttribute(attr).getValue().getValue();
                    }

                    tdef.addColumn(p.getQualifiedName(), DataType.PARAMETER_VALUE);
                    cols.add(getPV(p, gentime, value.longValue()));
                  }
                  break;
                case UINT64:
                  {
                    Number value = 0;
                    if (node.readAttribute(attr).getValue().isNull()) {
                      internalLogger.warn(
                          "node {} has a Null variant. Ignoring and will not be pushed to stream.",
                          node);
                      continue;
                    } else {
                      value = (Number) node.readAttribute(attr).getValue().getValue();
                    }

                    tdef.addColumn(p.getQualifiedName(), DataType.PARAMETER_VALUE);
                    cols.add(getPV(p, gentime, value.longValue()));
                  }
                  break;
                default:
                  break;
              }

              log.debug("Pushing {} to stream", p.toString());

              internalLogger.info(String.format("Pushing %s to stream", p.toString()));

              inCount.getAndAdd(1);
              realtimeCount.getAndAdd(1);
            }
            break;
          default:
            break;
        }

      } catch (UaException e) {
        // TODO Auto-generated catch block
        internalLogger.warn(e.toString());
        continue;
      }
    }

    pushTuple(tdef, cols);
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
      case BOOLEAN:
        return getOrCreateType(mdb, "boolean", () -> new BooleanParameterType.Builder());
      case STRING:
        return getOrCreateType(mdb, "string", () -> new StringParameterType.Builder());

      case FLOAT:
        return getOrCreateType(
            mdb, "float32", () -> new FloatParameterType.Builder().setSizeInBits(32));
      case DOUBLE:
        return getOrCreateType(
            mdb, "float64", () -> new FloatParameterType.Builder().setSizeInBits(64));
      case SINT32:
        return getOrCreateType(
            mdb,
            "sint32",
            () -> new IntegerParameterType.Builder().setSizeInBits(32).setSigned(true));
      case SINT64:
        return getOrCreateType(
            mdb,
            "sint64",
            () -> new IntegerParameterType.Builder().setSizeInBits(64).setSigned(true));
      case UINT32:
        return getOrCreateType(
            mdb,
            "uint32",
            () -> new IntegerParameterType.Builder().setSizeInBits(32).setSigned(false));
      case UINT64:
        return getOrCreateType(
            mdb,
            "uint64",
            () -> new IntegerParameterType.Builder().setSizeInBits(64).setSigned(false));
      default:
        break;
    }

    return pType;
  }

  public ParameterValue getNewPv(Parameter parameter, long time) {
    ParameterValue pv = new ParameterValue(parameter);
    pv.setAcquisitionTime(YamcsServer.getTimeService(yamcsInstance).getMissionTime());
    pv.setGenerationTime(time);
    return pv;
  }

  public ParameterValue getPV(Parameter parameter, long time, String v) {
    ParameterValue pv = getNewPv(parameter, time);
    pv.setEngValue(ValueUtility.getStringValue(v));
    pv.setRawValue(ValueUtility.getStringValue(v));
    return pv;
  }

  public ParameterValue getPV(Parameter parameter, long time, double v) {
    ParameterValue pv = getNewPv(parameter, time);
    pv.setEngValue(ValueUtility.getDoubleValue(v));
    pv.setRawValue(ValueUtility.getDoubleValue(v));
    return pv;
  }

  public ParameterValue getPV(Parameter parameter, long time, float v) {
    ParameterValue pv = getNewPv(parameter, time);
    pv.setEngValue(ValueUtility.getFloatValue(v));
    pv.setRawValue(ValueUtility.getFloatValue(v));
    return pv;
  }

  public ParameterValue getPV(Parameter parameter, long time, boolean v) {
    ParameterValue pv = getNewPv(parameter, time);
    pv.setEngValue(ValueUtility.getBooleanValue(v));
    pv.setRawValue(ValueUtility.getBooleanValue(v));
    return pv;
  }

  public ParameterValue getPV(Parameter parameter, long time, long v) {
    ParameterValue pv = getNewPv(parameter, time);
    pv.setEngValue(ValueUtility.getSint64Value(v));
    pv.setRawValue(ValueUtility.getSint64Value(v));
    return pv;
  }

  @Override
  public Status getLinkStatus() {
    return linkStatus;
  }

  @Override
  public boolean isDisabled() {
    return linkStatus == Status.DISABLED;
  }

  @Override
  public long getDataInCount() {
    return inCount.get();
  }

  @Override
  public long getDataOutCount() {
    return 0;
  }

  @Override
  public void resetCounters() {
    inCount.set(0);
  }

  /**
   * Selects first non-secured endpoint from endpoints found at discover URL. At the moment secured
   * endpoints are not supported.
   *
   * @return
   * @throws Exception
   */
  private OpcUaClient configureClient() throws Exception {

    List<EndpointDescription> endpoints = DiscoveryClient.getEndpoints(discoverURL).get();

    // At the moment, we do not support certificates.
    EndpointDescription selectedEndpoint = null;
    for (var endpoint : endpoints) {
      switch (endpoint.getSecurityMode()) {
        case Invalid:
          internalLogger.warn("Endpoint mode {} is not supported.", endpoint.getSecurityMode());
          break;
        case None:
          selectedEndpoint = endpoint;
          break;
        case Sign:
          internalLogger.warn("Endpoint mode {} is not supported.", endpoint.getSecurityMode());
          break;
        case SignAndEncrypt:
          internalLogger.warn("Endpoint mode {} is not supported.", endpoint.getSecurityMode());
          break;
      }

      if (selectedEndpoint != null) {
        break;
      }
    }

    if (selectedEndpoint == null) {
      throw new Exception("No viable endpoint found from list:" + endpoints);
    }

    OpcUaClientConfig builder = OpcUaClientConfig.builder().setEndpoint(selectedEndpoint).build();

    return OpcUaClient.create(builder);
  }

  /**
   * Adds new PV with the name of node.
   *
   * @param client
   * @param node
   */
  private void addOPCUAPV(OpcUaClient client, UaNode node) {

    if (node.getBrowseName()
        .getName()
        .contains(Character.toString(NameDescription.PATH_SEPARATOR))) {
      internalLogger.info(
          "{} ignored since it contains a {} character",
          node.getBrowseName().getName(),
          Character.toString(NameDescription.PATH_SEPARATOR));

    } else {

      /**
       * NOTE:For now we'll just flatten all the attributes instead of using an aggregate type for
       * attributes
       */
      for (AttributeId attr : AttributeId.values()) {

        ParameterType ptype = OPCUAAttrTypeToParamType(attr, node);

        String opcuaTranslatedQName = translateNodeToParamQName(client, node, attr);
        Parameter p = VariableParam.getForFullyQualifiedName(opcuaTranslatedQName);

        p.setParameterType(ptype);

        if (mdb.getParameter(p.getQualifiedName()) == null) {
          log.debug("Adding OPCUA object as parameter to mdb:{}", p.getQualifiedName());
          mdb.addParameter(p, true);
        } else {
          p = mdb.getParameter(p.getQualifiedName());
        }
        nodeIDToParamsMap.put(new NodeIDAttrPair(node.getNodeId(), attr), p);
      }
    }
  }

  /**
   * Map nodeID name to a qualified name that can be used for a YAMCS PV.
   *
   * @param client
   * @param node
   * @param attr
   * @return
   */
  private String translateNodeToParamQName(OpcUaClient client, UaNode node, AttributeId attr) {
    LocalizedText localizedDisplayName = null;
    try {

      localizedDisplayName =
          (LocalizedText) (node.readAttribute(AttributeId.DisplayName).getValue().getValue());
    } catch (UaException e) {
      internalLogger.warn(e.toString());
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
   * Browse node at nodePath relative to browseRoot.
   *
   * @param indent
   * @param client
   * @param browseRoot
   * @param nodePath in the format of "0:Root,0:Objects,2:HelloWorld,2:MyObject,2:Bar"
   * @throws Exception
   */
  private void browsePath(String indent, OpcUaClient client, NodeId startingNode, String nodePath)
      throws Exception {
    internalLogger.info("Browsing at " + startingNode);
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
      internalLogger.warn(e.toString());
    } catch (ExecutionException e) {
      internalLogger.warn(e.toString());
    }

    BrowsePathResult result = Arrays.asList(response.getResults()).get(0);
    StatusCode statusCode = result.getStatusCode();

    if (statusCode.isBad()) {
      log.warn("Bad status code:" + statusCode);
      org.yamcs.yarch.protobuf.Db.Event ev =
          Event.newBuilder()
              .setGenerationTime(YamcsServer.getTimeService(yamcsInstance).getMissionTime())
              .setGenerationTime(YamcsServer.getTimeService(yamcsInstance).getMissionTime())
              .setSource(this.linkName)
              .setType(this.linkName)
              .setMessage("Failed to find node:" + nodePath + ". Error code info:" + statusCode)
              .setSeverity(EventSeverity.ERROR)
              .build();
      eventProducer.sendEvent(ev);

      throw new Exception("Bad status code:" + statusCode);

    } else if (statusCode.isUncertain()) {
      log.warn("Uncertain status code:" + statusCode);
      return;
    }

    try {
      UaNode node =
          client
              .getAddressSpace()
              .getNode(
                  result.getTargets()[0].getTargetId().toNodeId(client.getNamespaceTable()).get());

      addOPCUAPV(client, node);
    } catch (UaException e) {
      internalLogger.warn(e.toString());
    }
  }

  private void createOPCUASubscriptions() {
    currentOPCUAStatus = OPCUAINITStatus.OPCUA_INIT_DATA_SUBSCRIPTION;
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

    ArrayList<NodeId> variableNodes = new ArrayList<NodeId>();
    for (NodeId id : nodeSet) {
      Variant nodeClass = null;
      try {
        UaNode node = client.getAddressSpace().getNode(id);

        nodeClass = node.readAttribute(AttributeId.NodeClass).getValue();

      } catch (UaException e) {
        internalLogger.warn(e.toString());
      }
      if (nodeClass != null) {
        //        try {
        switch (NodeClass.from((int) nodeClass.getValue())) {
            // As per the spec, the only thing we can subscribe to is Variables
          case Variable:
            variableNodes.add(id);
            break;
        }
      }
    }

    try {
      List<ManagedDataItem> dataItems = opcuaSubscription.createDataItems(variableNodes);
      for (var dataItem : dataItems) {
        log.debug("Status code for dataItem:{}", dataItem.getStatusCode());
        OPCUAActiveSubs.addAndGet(1);
      }
    } catch (UaException e) {
      internalLogger.warn(e.toString());
    }

    currentOPCUAStatus = OPCUAINITStatus.OPCUA_INIT_OK;
  }

  /**
   * Connects to OPCUA server and activates query all action.
   *
   * @param client
   * @throws Exception
   */
  public void connectToOPCUAServer(OpcUaClient client) throws Exception {
    internalLogger.info("Connecting to OPCUA server...");
    client.connect().get();

    if (getAction(startAction.getId()) == null) {
      addAction(startAction);
      addAction(reconnectAction);
    }
    startAction.setEnabled(true);
    reconnectAction.setEnabled(true);
  }

  /**
   * Browses the tree on the OPCUA server and maps them to YAMCS Parameters.
   *
   * @param client
   * @throws Exception
   */
  private void browseOPCUATree(OpcUaClient client) throws Exception {
    // start browsing at root folder
    internalLogger.info("Browsing OPCUA...");
    for (var p : relativeNodePaths) {
      int namespaceIndex = (int) p.rootNodeID.get("namespaceIndex");
      String identifier = (String) p.rootNodeID.get("identifier");
      IdType identifierType = IdType.valueOf((String) p.rootNodeID.get("identifierType"));

      browsePath(
          endpointURL, client, getNewNodeID(identifierType, namespaceIndex, identifier), p.path);
    }
  }

  /**
   * Get new OPCUA-compliant NodeID object that is created from NamespaceIndex and Identifier. At
   * the moment only String and Numeric node ids are supported.
   *
   * @param rootIdentifierType
   * @param NamespaceIndex
   * @param Identifier
   * @return
   */
  private NodeId getNewNodeID(IdType rootIdentifierType, int NamespaceIndex, String Identifier) {
    NodeId nodeID = null;
    switch (rootIdentifierType) {
      case Guid:
        internalLogger.warn("Guid nodeID is not supported");
        break;
      case Numeric:
        nodeID = new NodeId(NamespaceIndex, Integer.parseInt(Identifier));
        break;
      case Opaque:
        internalLogger.warn("Guid Opaque is not supported");
        break;
      case String:
        nodeID = new NodeId(NamespaceIndex, Identifier);
        break;
    }
    return nodeID;
  }

  /** Data listener for realtime OPCUA server updates. */
  private void createDataChangeListener() {
    try {
      opcuaSubscription = ManagedSubscription.create(client, publishInterval);
    } catch (UaException e) {
      internalLogger.warn(e.toString());
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
            //            FIXME: Add leap seconds.... as config or get it from YAMCS API.
            long gentime =
                values
                    .get(i)
                    .getSourceTime()
                    .getJavaInstant()
                    .plus(37, ChronoUnit.SECONDS)
                    .toEpochMilli();

            if (useGroundTimeForRealtimeData) {
              gentime = timeService.getMissionTime();
            }
            cols.add(gentime);
            cols.add(parametersNamespace);
            cols.add(0);
            long rectime = timeService.getMissionTime();
            cols.add(rectime);

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
             * <p>Another question worth answering before moving forward is to find out whether or
             * not it is concrete in the OPCUA protocol what data can change in real time and which
             * data is "static". Not sure if there is any "static" data given that clients have the
             * ability of writing to values... might be worth a test.
             */
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

            if (values.get(i).getValue() != null && values.get(i).getValue().getValue() != null) {

              switch (nodeIDToParamsMap.get(nodeAttrKey).getParameterType().getValueType()) {
                case BOOLEAN:
                  {
                    boolean value = (boolean) values.get(i).getValue().getValue();

                    tdef.addColumn(
                        nodeIDToParamsMap.get(nodeAttrKey).getQualifiedName(),
                        DataType.PARAMETER_VALUE);
                    cols.add(getPV(nodeIDToParamsMap.get(nodeAttrKey), gentime, value));
                  }
                  break;
                case DOUBLE:
                  {
                    Number value = (Number) values.get(i).getValue().getValue();

                    tdef.addColumn(
                        nodeIDToParamsMap.get(nodeAttrKey).getQualifiedName(),
                        DataType.PARAMETER_VALUE);
                    cols.add(
                        getPV(nodeIDToParamsMap.get(nodeAttrKey), gentime, value.doubleValue()));
                  }
                  break;
                case FLOAT:
                  {
                    Number value = (Number) values.get(i).getValue().getValue();

                    tdef.addColumn(
                        nodeIDToParamsMap.get(nodeAttrKey).getQualifiedName(),
                        DataType.PARAMETER_VALUE);
                    cols.add(
                        getPV(nodeIDToParamsMap.get(nodeAttrKey), gentime, value.floatValue()));
                  }
                  break;
                case SINT32:
                  {
                    Number value = (Number) values.get(i).getValue().getValue();
                    tdef.addColumn(
                        nodeIDToParamsMap.get(nodeAttrKey).getQualifiedName(),
                        DataType.PARAMETER_VALUE);
                    cols.add(getPV(nodeIDToParamsMap.get(nodeAttrKey), gentime, value.intValue()));
                  }
                  break;
                case SINT64:
                  {
                    Number value = (Number) values.get(i).getValue().getValue();

                    tdef.addColumn(
                        nodeIDToParamsMap.get(nodeAttrKey).getQualifiedName(),
                        DataType.PARAMETER_VALUE);
                    cols.add(getPV(nodeIDToParamsMap.get(nodeAttrKey), gentime, value.longValue()));
                  }
                  break;
                case STRING:
                  {
                    String value = (String) values.get(i).getValue().getValue().toString();

                    tdef.addColumn(
                        nodeIDToParamsMap.get(nodeAttrKey).getQualifiedName(),
                        DataType.PARAMETER_VALUE);
                    cols.add(getPV(nodeIDToParamsMap.get(nodeAttrKey), gentime, value));
                  }
                  break;
                case UINT32:
                  {
                    Number value = (Number) values.get(i).getValue().getValue();
                    tdef.addColumn(
                        nodeIDToParamsMap.get(nodeAttrKey).getQualifiedName(),
                        DataType.PARAMETER_VALUE);
                    cols.add(getPV(nodeIDToParamsMap.get(nodeAttrKey), gentime, value.longValue()));
                  }
                  break;
                case UINT64:
                  {
                    Number value = (Number) values.get(i).getValue().getValue();

                    tdef.addColumn(
                        nodeIDToParamsMap.get(nodeAttrKey).getQualifiedName(),
                        DataType.PARAMETER_VALUE);
                    cols.add(getPV(nodeIDToParamsMap.get(nodeAttrKey), gentime, value.longValue()));
                  }
                  break;
                default:
                  break;
              }

              pushTuple(tdef, cols);

              inCount.getAndAdd(1);
              realtimeCount.getAndAdd(1);
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
   * Get new ParameterType for the specified attribute of the node. Particularly useful for Value
   * attributes of nodes.
   *
   * @param attr
   * @param node
   * @return
   */
  private ParameterType OPCUAAttrTypeToParamType(AttributeId attr, UaNode node) {
    ParameterType pType = null;

    switch (attr) {
      case AccessLevel:
        pType = getBasicType(mdb, Type.STRING);
        break;
      case ArrayDimensions:
        pType = getBasicType(mdb, Type.STRING);
        break;
      case BrowseName:
        pType = getBasicType(mdb, Type.STRING);
        break;
      case ContainsNoLoops:
        pType = getBasicType(mdb, Type.STRING);
        break;
      case DataType:
        pType = getBasicType(mdb, Type.STRING);
        break;
      case Description:
        pType = getBasicType(mdb, Type.STRING);
        break;
      case DisplayName:
        pType = getBasicType(mdb, Type.STRING);
        break;
      case EventNotifier:
        pType = getBasicType(mdb, Type.STRING);
        break;
      case Executable:
        pType = getBasicType(mdb, Type.STRING);
        break;
      case Historizing:
        pType = getBasicType(mdb, Type.STRING);
        break;
      case InverseName:
        pType = getBasicType(mdb, Type.STRING);
        break;
      case IsAbstract:
        pType = getBasicType(mdb, Type.STRING);
        break;
      case MinimumSamplingInterval:
        pType = getBasicType(mdb, Type.STRING);
        break;
      case NodeClass:
        pType = getBasicType(mdb, Type.STRING);
        break;
      case NodeId:
        pType = getBasicType(mdb, Type.STRING);
        break;
      case Symmetric:
        pType = getBasicType(mdb, Type.STRING);
        break;
      case UserAccessLevel:
        pType = getBasicType(mdb, Type.STRING);
        break;
      case UserExecutable:
        pType = getBasicType(mdb, Type.STRING);
        break;
      case UserWriteMask:
        pType = getBasicType(mdb, Type.STRING);
        break;
      case Value:
        try {

          var value = node.readAttribute(attr).getValue();

          if (value.isNotNull()) {
            NodeId valueObjectType =
                value.getDataType().get().toNodeId(client.getNamespaceTable()).get();

            /** As per the spec:https://reference.opcfoundation.org/Core/Part6/v104/docs/5.1.2 */
            if (valueObjectType.equals(Identifiers.SByte)) {
              pType = getBasicType(mdb, Type.SINT32);
            } else if (valueObjectType.equals(Identifiers.Byte)) {
              pType = getBasicType(mdb, Type.SINT32);
            } else if (valueObjectType.equals(Identifiers.Int16)) {
              pType = getBasicType(mdb, Type.SINT32);
            } else if (valueObjectType.equals(Identifiers.UInt16)) {
              pType = getBasicType(mdb, Type.SINT32);
            } else if (valueObjectType.equals(Identifiers.Int32)) {
              pType = getBasicType(mdb, Type.SINT32);
            } else if (valueObjectType.equals(Identifiers.UInt32)) {
              pType = getBasicType(mdb, Type.UINT32);
            } else if (valueObjectType.equals(Identifiers.Int64)) {
              pType = getBasicType(mdb, Type.SINT64);
            } else if (valueObjectType.equals(Identifiers.UInt64)) {
              pType = getBasicType(mdb, Type.UINT64);
            } else if (valueObjectType.equals(Identifiers.Float)) {
              pType = getBasicType(mdb, Type.FLOAT);
            } else if (valueObjectType.equals(Identifiers.Double)) {
              pType = getBasicType(mdb, Type.DOUBLE);
            } else if (valueObjectType.equals(Identifiers.String)) {
              pType = getBasicType(mdb, Type.STRING);
            } else if (valueObjectType.equals(Identifiers.Boolean)) {
              pType = getBasicType(mdb, Type.BOOLEAN);
            }
          } else {
            pType = getBasicType(mdb, Type.STRING);
          }

        } catch (UaException e) {
          internalLogger.warn(e.toString());
        }
        break;
      case ValueRank:
        pType = getBasicType(mdb, Type.STRING);
        break;
      case WriteMask:
        pType = getBasicType(mdb, Type.STRING);
        break;
      default:
        break;
    }

    return pType;
  }

  /**
   * Subscribe to OPCUA events as per the
   * spec:https://reference.opcfoundation.org/Core/Part5/v104/docs/6.4.2
   *
   * @param client
   * @throws InterruptedException
   * @throws ExecutionException
   */
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

    monitoredItem.setEventConsumer(
        (item, vs) -> {
          internalLogger.info("Event Received from {}", item.getReadValueId().getNodeId());

          StringBuilder eventText = new StringBuilder();

          ByteString eventId;
          NodeId eventType;
          UShort eventSeverity;
          DateTime eventTime;
          LocalizedText eventMessage;

          for (int i = 0; i < vs.length; i++) {
            internalLogger.info("\tvariant[{}]: {}", i, vs[i].getValue());
          }

          eventId = (ByteString) vs[0].getValue();
          eventType = (NodeId) vs[1].getValue();
          eventSeverity = (UShort) vs[2].getValue();
          eventTime = (DateTime) vs[3].getValue();
          eventMessage = (LocalizedText) vs[4].getValue();

          //          FIXME:Map these values to YAMCS API
          eventText.append("eventId:" + eventId);
          eventText.append("\n");
          eventText.append("eventType:" + eventType);
          eventText.append("\n");
          eventText.append("eventSeverity:" + eventSeverity);
          eventText.append("\n");
          eventText.append("eventTime:" + eventTime);
          eventText.append("\n");
          eventText.append("eventMessage:" + eventMessage);
          org.yamcs.yarch.protobuf.Db.Event ev =
              Event.newBuilder()
                  .setGenerationTime(YamcsServer.getTimeService(yamcsInstance).getMissionTime())
                  .setGenerationTime(YamcsServer.getTimeService(yamcsInstance).getMissionTime())
                  .setSource(this.linkName)
                  .setType(this.linkName)
                  .setMessage(eventText.toString())
                  .setSeverity(EventSeverity.INFO)
                  .build();
          eventProducer.sendEvent(ev);
        });
  }

  @Override
  public void setupSystemParameters(SystemParametersService sysParamService) {
    super.setupSystemParameters(sysParamService);
    OPCUAInitStatusParam =
        sysParamService.createEnumeratedSystemParameter(
            linkName + "/OPCUAInitStatusParam",
            OPCUAINITStatus.class,
            "The current initialization status of OPCUA client");
    EnumeratedParameterType spLinkStatusType =
        (EnumeratedParameterType) OPCUAInitStatusParam.getParameterType();
    spLinkStatusType
        .enumValue(OPCUAINITStatus.OPCUA_INIT_CONFIG.name())
        .setDescription(
            "This link is in the configuration stage(Configuring OPCUA parameters such as certificates)");
    spLinkStatusType
        .enumValue(OPCUAINITStatus.OPCUA_INIT_TREE.name())
        .setDescription(
            "The link is parsing the OPCUA Tree and mapping them to PVs."
                + " Depending on configuration, this can take a while.");

    spLinkStatusType
        .enumValue(OPCUAINITStatus.OPCUA_INIT_TREE_FAILED.name())
        .setDescription("The initial parsing of configured nodes failed.");
    spLinkStatusType
        .enumValue(OPCUAINITStatus.OPCUA_INIT_EVENTS.name())
        .setDescription("The link is configuring and subscribing to OPCUA events");
    spLinkStatusType
        .enumValue(OPCUAINITStatus.OPCUA_INIT_DATA_SUBSCRIPTION.name())
        .setDescription(
            "The link is creating subscriptions for each node that was parsed from the tree"
                + "that has a Value attribute.");
    spLinkStatusType
        .enumValue(OPCUAINITStatus.OPCUA_INIT_ALL_DATA_QUERY.name())
        .setDescription(
            "The link is querying all attributes of all parsed nodes."
                + "This is can be configured to be done at startup.");
    spLinkStatusType
        .enumValue(OPCUAINITStatus.OPCUA_INIT_OK.name())
        .setDescription(
            "The link is done with all OPCUA initialization. It is in an usable state.");

    OPCUAActiveSubsParam =
        sysParamService.createSystemParameter(
            linkName + "/OPCUAActiveSubs",
            Type.UINT64,
            "The total number of active opcua subscriptions");

    realtimeCountParam =
        sysParamService.createSystemParameter(
            linkName + "/RealtimeCount",
            Type.UINT64,
            "The total number of realtime count(used for sub strike counts)");

    lastRealtimeCountParam =
        sysParamService.createSystemParameter(
            linkName + "/LastRealtimeCount",
            Type.UINT64,
            "The total number of realtime counts last captured(used for sub strike counts)");

    subStrikeCountThresholdParam =
        sysParamService.createSystemParameter(
            linkName + "/SubStrikeCountThreshold",
            Type.UINT64,
            "Configured strike count threshold. If current strike count exceeds this value, users will be notified via events.");

    subStrikeCountParam =
        sysParamService.createSystemParameter(
            linkName + "/SubStrikeCount", Type.UINT64, "Current subscription strike count.");

    subStrikeCountCheckTimeoutSecsParam =
        sysParamService.createSystemParameter(
            linkName + "/SubStrikeCountCheckTimeoutSecs",
            Type.UINT64,
            "Timeout(in seconds) between strike count checks.");

    reconnectCountParam =
        sysParamService.createSystemParameter(
            linkName + "/ReconnectCount",
            Type.UINT64,
            "Successful reconnect count, after sub strike count failures.");
  }

  @Override
  public List<ParameterValue> getSystemParameters() {
    long time = getCurrentTime();
    ArrayList<ParameterValue> list = new ArrayList<>();

    list.add(
        org.yamcs.parameter.SystemParametersService.getPV(
            OPCUAInitStatusParam, time, currentOPCUAStatus));
    list.add(
        org.yamcs.parameter.SystemParametersService.getPV(
            OPCUAActiveSubsParam, time, OPCUAActiveSubs.get()));

    list.add(
        org.yamcs.parameter.SystemParametersService.getPV(
            realtimeCountParam, time, realtimeCount.get()));
    list.add(
        org.yamcs.parameter.SystemParametersService.getPV(
            lastRealtimeCountParam, time, lastRealtimeCount.get()));

    list.add(
        org.yamcs.parameter.SystemParametersService.getPV(
            subStrikeCountThresholdParam, time, subStrikeCountThreshold));

    list.add(
        org.yamcs.parameter.SystemParametersService.getPV(
            subStrikeCountParam, time, subStrikeCount.get()));

    list.add(
        org.yamcs.parameter.SystemParametersService.getPV(
            subStrikeCountCheckTimeoutSecsParam, time, subStrikeCountCheckTimeoutSecs));

    list.add(
        org.yamcs.parameter.SystemParametersService.getPV(
            reconnectCountParam, time, reconnectCount));
    try {
      super.collectSystemParameters(time, list);
    } catch (Exception e) {
      log.error("Exception caught when collecting link system parameters", e);
    }
    return list;
  }
}
