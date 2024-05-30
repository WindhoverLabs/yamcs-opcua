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

import com.google.common.io.BaseEncoding;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
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
  /* Configuration Defaults */
  static final String STREAM_NAME = "opcua_params";

  private Parameter OPCUAServerStatus;

  /* Configuration Parameters */
  protected long initialDelay;
  protected long period;
  protected boolean clearBucketsAtStartup;
  protected boolean deleteFileAfterProcessing;
  protected int EVS_FILE_HDR_SUBTYPE;
  protected int DS_TOTAL_FNAME_BUFSIZE;
  boolean ignoreSpacecraftID;
  boolean ignoreProcessorID;
  private String outputFile;

  /* Internal member attributes. */
  protected FileSystemBucket csvBucket;
  protected YConfiguration packetInputStreamArgs;
  protected PacketInputStream packetInputStream;
  protected Thread thread;

  private String opcuaStreamName;

  static final String DATA_EVENT_CNAME = "data";

  private Charset charset;

  /* Constants */
  static final byte[] CFE_FS_FILE_CONTENT_ID_BYTE =
      BaseEncoding.base16().lowerCase().decode("63464531".toLowerCase());

  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

  Integer appNameMax;
  Integer eventMsgMax;

  // /yamcs/<server_id>
  private String namespace;
  private String serverId;
  XtceDb mdb;

  Stream opcuaStream;

  ParameterSink paraSink;
  private static TupleDefinition gftdef = StandardTupleDefinitions.PARAMETER.copy();

  //  NOTE:ALWAYS re-use this param as org.yamcs.parameter.ParameterRequestManager.param2RequestMap
  //  uses the object inside a map that was added to the mdb for the very fist time.
  //  If when publishing the PV, we create a new VariableParam object clients will NOT
  //  receive real-time updates as the new object VariableParam inside the new PV won't match the
  // one
  //  inside org.yamcs.parameter.ParameterRequestManager.param2RequestMap since the object hashes
  //  do not match (since VariableParam does not override its hash function).
  private VariableParam p;

  private DefaultTrustListManager trustListManager;
  private AggregateParameterType opcuaAttrsType;
  private ManagedSubscription opcuaSubscription;

  private HashMap<NodeId, VariableParam> nodeIDToParamsMap = new HashMap<NodeId, VariableParam>();

  @Override
  public Spec getSpec() {
    Spec spec = new Spec();
    Spec preprocessorSpec = new Spec();

    /* Define our configuration parameters. */
    spec.addOption("name", OptionType.STRING).withRequired(true);
    spec.addOption("class", OptionType.STRING).withRequired(true);
    spec.addOption("opcua_stream", OptionType.STRING).withRequired(true);

    return spec;
  }

  @Override
  public void init(String yamcsInstance, String serviceName, YConfiguration config) {
    super.init(yamcsInstance, serviceName, config);

    /* Local variables */
    String packetInputStreamClassName;
    this.config = config;
    /* Calidate the configuration that the user passed us. */
    try {
      config = getSpec().validate(config);
    } catch (ValidationException e) {
      log.error("Failed configuration validation.", e);
    }

    String chrname = config.getString("charset", "US-ASCII");
    try {
      charset = Charset.forName(chrname);
    } catch (UnsupportedCharsetException e) {
      throw new ConfigurationException(
          "Unsupported charset '"
              + chrname
              + "'. Please use one of "
              + Charset.availableCharsets().keySet());
    }

    YarchDatabaseInstance ydb = YarchDatabase.getInstance(yamcsInstance);

    this.opcuaStreamName = config.getString("opcua_stream");

    opcuaStream = getStream(ydb, opcuaStreamName);

    mdb = YamcsServer.getServer().getInstance(yamcsInstance).getXtceDb();

    namespace = "/instruments/tvac";

    p = VariableParam.getForFullyQualifiedName("/instruments/tvac/hello1");

    ParameterType ptype = getBasicType(mdb, Yamcs.Value.Type.SINT64, null);

    p.setQualifiedName("/instruments/tvac/hello1");
    p.setParameterType(ptype);
    mdb.addParameter(p, true);

    scheduler.scheduleAtFixedRate(
        () -> {
          publishNewPVs();
        },
        1,
        1,
        TimeUnit.SECONDS);

    try {
      runOPCUClient();
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
      return String.format("OK, received %d packets", -1);
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
    notifyStarted();
  }

  @Override
  protected void doStop() {
    if (thread != null) {
      thread.interrupt();
    }

    notifyStopped();
  }

  @Override
  public void run() {
    /* Delay the start, if configured to do so. */
    if (initialDelay > 0) {
      try {
        Thread.sleep(initialDelay);
        initialDelay = -1;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }

    /* Enter our main loop */
    while (isRunningAndEnabled()) {
      /* Iterate through all our watch keys. */

      //    	publishNewPVs();

      /* Sleep for the configured amount of time.  We normally sleep so we don't needlessly chew up resources. */
      try {
        Thread.sleep(this.period);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private void publishNewPVs() {
    TupleDefinition tdef = gftdef.copy();
    List<Object> cols = new ArrayList<>(4 + 1);
    long gentime = timeService.getMissionTime();
    cols.add(gentime);
    cols.add(namespace);
    cols.add(0);
    cols.add(gentime);

    tdef.addColumn("/instruments/tvac/hello1", DataType.PARAMETER_VALUE);

    cols.add(getPV(p, Instant.now().toEpochMilli(), new Random().nextLong()));

    Tuple t = new Tuple(tdef, cols);

    opcuaStream.emitTuple(t);
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

  private String decodeString(ByteBuffer buf, int maxLength) {
    maxLength = Math.min(maxLength, buf.remaining());
    ByteBuffer buf1 = buf.slice();
    buf1.limit(maxLength);
    int k = 0;
    while (k < maxLength) {
      if (buf1.get(k) == 0) {
        break;
      }
      k++;
    }
    buf1.limit(k);

    String r = charset.decode(buf1).toString();
    buf.position(buf.position() + maxLength);

    return r;
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
    // TODO Auto-generated method stubf

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
    return 0;
  }

  @Override
  public long getDataOutCount() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void resetCounters() {
    // TODO Auto-generated method stub

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
      opcuaSubscription = ManagedSubscription.create(client, 0);
    } catch (UaException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    opcuaSubscription.addDataChangeListener(
        (items, values) -> {
          for (int i = 0; i < items.size(); i++) {
            log.info(
                "subscription value received: item={}, value={}",
                items.get(i).getNodeId(),
                values.get(i).getValue());

            log.info(
                "Pushing new PV for param name {} which is mapped to NodeID {}",
                nodeIDToParamsMap.get(items.get(i).getNodeId()),
                items.get(i).getNodeId());

            TupleDefinition tdef = gftdef.copy();
            List<Object> cols = new ArrayList<>(4 + 1);
            long gentime = timeService.getMissionTime();
            cols.add(gentime);
            cols.add(namespace);
            cols.add(0);
            cols.add(gentime);

            tdef.addColumn(
                nodeIDToParamsMap.get(items.get(i).getNodeId()).getQualifiedName(),
                DataType.PARAMETER_VALUE);

            cols.add(
                getPV(
                    nodeIDToParamsMap.get(items.get(i).getNodeId()),
                    Instant.now().toEpochMilli(),
                    values.get(i).getValue().toString()));

            Tuple t = new Tuple(tdef, cols);

            opcuaStream.emitTuple(t);
          }
        });

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
          Parameter p =
              VariableParam.getForFullyQualifiedName(
                  qualifiedName(
                      namespace + NameDescription.PATH_SEPARATOR + rd.getBrowseName().getName(),
                      rd.getBrowseName().getName()));

          p.setParameterType(opcuaAttrsType);

          //        TODO:Add Map of node_id -> Params
          if (mdb.getParameter(p.getQualifiedName()) == null) {
            log.info("Adding OPCUA object as parameter to mdb:{}", p.getQualifiedName());
            mdb.addParameter(p, true);

            nodeIDToParamsMap.put(
                rd.getNodeId().toNodeId(client.getNamespaceTable()).get(), (VariableParam) p);

            try {
              ManagedDataItem dataItem =
                  opcuaSubscription.createDataItem(
                      rd.getNodeId().toNodeId(client.getNamespaceTable()).get());
            } catch (UaException e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
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

  public void connectToOPCUAServer(OpcUaClient client, CompletableFuture<OpcUaClient> future)
      throws Exception {
    // synchronous connect
    client.connect().get();

    // start browsing at root folder
    browseNode("", client, Identifiers.RootFolder);

    future.complete(client);
  }

  public void runOPCUAClient() {

    final CompletableFuture<OpcUaClient> future = new CompletableFuture<>();

    OpcUaClient client = null;
    try {
      client = createClient();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    try {
      connectToOPCUAServer(client, future);

      //                        client.disconnect().get();
      //      Stack.releaseSharedResources();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  public void runOPCUClient() throws Exception {

    createOPCUAAttrAggregateType();
    mdb.addParameterType(opcuaAttrsType, true);
    runOPCUAClient();
  }

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
