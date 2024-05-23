/****************************************************************************
 *
 *   Copyright (c) 2022 Windhover Labs, L.L.C. All rights reserved.
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

import com.google.common.io.BaseEncoding;


import static org.opcfoundation.ua.utils.EndpointUtil.selectByMessageSecurityMode;
import static org.opcfoundation.ua.utils.EndpointUtil.selectByProtocol;
import static org.opcfoundation.ua.utils.EndpointUtil.sortBySecurityLevel;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.opcfoundation.ua.application.Client;
import org.opcfoundation.ua.application.SessionChannel;
import org.opcfoundation.ua.builtintypes.LocalizedText;
import org.opcfoundation.ua.builtintypes.NodeId;
import org.opcfoundation.ua.cert.CertificateCheck;
import org.opcfoundation.ua.cert.DefaultCertificateValidator;
import org.opcfoundation.ua.cert.DefaultCertificateValidatorListener;
import org.opcfoundation.ua.cert.PkiDirectoryCertificateStore;
import org.opcfoundation.ua.cert.ValidationResult;
import org.opcfoundation.ua.common.ServiceResultException;
import org.opcfoundation.ua.core.ApplicationDescription;
import org.opcfoundation.ua.core.Attributes;
import org.opcfoundation.ua.core.BrowseDescription;
import org.opcfoundation.ua.core.BrowseDirection;
import org.opcfoundation.ua.core.BrowseResponse;
import org.opcfoundation.ua.core.BrowseResultMask;
import org.opcfoundation.ua.core.EndpointDescription;
import org.opcfoundation.ua.core.Identifiers;
import org.opcfoundation.ua.core.MessageSecurityMode;
import org.opcfoundation.ua.core.NodeClass;
import org.opcfoundation.ua.core.ReadRequest;
import org.opcfoundation.ua.core.ReadResponse;
import org.opcfoundation.ua.core.ReadValueId;
import org.opcfoundation.ua.core.TimestampsToReturn;
import org.opcfoundation.ua.transport.ServiceChannel;
import org.opcfoundation.ua.transport.security.Cert;
import org.opcfoundation.ua.transport.security.HttpsSecurityPolicy;
import org.opcfoundation.ua.transport.security.KeyPair;
import org.opcfoundation.ua.utils.CertificateUtils;
import org.yamcs.ConfigurationException;
import org.yamcs.Spec;
import org.yamcs.StandardTupleDefinitions;
import org.yamcs.Spec.OptionType;
import org.yamcs.TmPacket;
import org.yamcs.ValidationException;
import org.yamcs.YConfiguration;
import org.yamcs.YamcsServer;
import org.yamcs.parameter.ParameterValue;
import org.yamcs.parameter.SystemParametersProducer;
import org.yamcs.parameter.SystemParametersService;
import org.yamcs.parameter.Value;
import org.yamcs.protobuf.Yamcs;
import org.yamcs.protobuf.Yamcs.Value.Type;
import org.yamcs.tctm.AbstractTmDataLink;
import org.yamcs.tctm.CcsdsPacketInputStream;
import org.yamcs.tctm.PacketInputStream;
import org.yamcs.tctm.PacketTooLongException;
import org.yamcs.utils.ValueUtility;
import org.yamcs.utils.YObjectLoader;
import org.yamcs.xtce.AbsoluteTimeParameterType;
import org.yamcs.xtce.BaseDataType;
import org.yamcs.xtce.BinaryParameterType;
import org.yamcs.xtce.BooleanParameterType;
import org.yamcs.xtce.EnumeratedParameterType;
import org.yamcs.xtce.FloatParameterType;
import org.yamcs.xtce.IntegerParameterType;
import org.yamcs.xtce.NameDescription;
import org.yamcs.xtce.Parameter;
import org.yamcs.xtce.ParameterType;
import org.yamcs.xtce.StringParameterType;
import org.yamcs.xtce.SystemParameter;
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
import org.yamcs.yarch.protobuf.Db.Event;

public class OPCUALink extends AbstractTmDataLink
    implements Runnable, StreamSubscriber, SystemParametersProducer {
	
	  private static class MyValidationListener implements DefaultCertificateValidatorListener {

		    @Override
		    public ValidationResult onValidate(Cert certificate, ApplicationDescription applicationDescription,
		        EnumSet<CertificateCheck> passedChecks) {
		      System.out.println("Validating Server Certificate...");
		      if (passedChecks.containsAll(CertificateCheck.COMPULSORY)) {
		        System.out.println("Server Certificate is valid and trusted, accepting certificate!");
		        return ValidationResult.AcceptPermanently;
		      } else {
		        System.out.println("Certificate Details: " + certificate.getCertificate().toString());
		        System.out.println("Do you want to accept this certificate?\n" + " (A=Always, Y=Yes, this time, N=No)");
		        while (true) {
		          try {
		            char c;
		            c = Character.toLowerCase((char) System.in.read());
		            if (c == 'a') {
		              return ValidationResult.AcceptPermanently;
		            }
		            if (c == 'y') {
		              return ValidationResult.AcceptOnce;
		            }
		            if (c == 'n') {
		              return ValidationResult.Reject;
		            }
		          } catch (IOException e) {
		            System.out.println("Error reading input! Not accepting certificate.");
		            return ValidationResult.Reject;
		          }
		        }
		      }
		    }

		  }
  /* Configuration Defaults */
  static long POLLING_PERIOD_DEFAULT = 1000;
  static int INITIAL_DELAY_DEFAULT = -1;
  static boolean IGNORE_INITIAL_DEFAULT = true;
  static boolean CLEAR_BUCKETS_AT_STARTUP_DEFAULT = false;
  static boolean DELETE_FILE_AFTER_PROCESSING_DEFAULT = false;

  private boolean outOfSync = false;

  private Parameter outOfSyncParam;
  private Parameter streamEventCountParam;
  private Parameter logEventCountParam;
  private int streamEventCount;
  private int logEventCount;

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

  private String eventStreamName;

  static final String RECTIME_CNAME = "rectime";
  static final String DATA_EVENT_CNAME = "data";

  private Charset charset;

  /* Constants */
  static final byte[] CFE_FS_FILE_CONTENT_ID_BYTE =
      BaseEncoding.base16().lowerCase().decode("63464531".toLowerCase());

  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

  private ByteOrder byteOrder;

  Integer appNameMax;
  Integer eventMsgMax;
  
  // /yamcs/<server_id>
  private String namespace;
  private String serverId;
  XtceDb mdb;
  
  static final String STREAM_NAME = "sys_param";
  
  
  Stream stream;

  @Override
  public Spec getSpec() {
    Spec spec = new Spec();
    Spec preprocessorSpec = new Spec();

    /* Define our configuration parameters. */
    spec.addOption("name", OptionType.STRING).withRequired(true);
    spec.addOption("class", OptionType.STRING).withRequired(true);
    spec.addOption("stream", OptionType.STRING).withRequired(true);

    spec.addOption("packetInputStreamClassName", OptionType.STRING).withRequired(false);
    spec.addOption("packetPreprocessorClassName", OptionType.STRING).withRequired(true);
    /* Set the preprocessor argument config parameters to "allowUnknownKeys".  We don't know
    or care what
        * these parameters are.  Let the preprocessor define them. */
    preprocessorSpec.allowUnknownKeys(true);
    spec.addOption("packetPreprocessorArgs", OptionType.MAP)
        .withRequired(true)
        .withSpec(preprocessorSpec);

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

//    /* Instantiate our member objects. */
//    this.eventStreamName = this.config.getString("eventStream");
//
//    Stream stream = YarchDatabase.getInstance(yamcsInstance).getStream(eventStreamName);
//
//    stream.addSubscriber(this);
    streamEventCount = 0;

    scheduler.scheduleAtFixedRate(
        () -> {
//          this.outOfSync = this.logEventCount != this.streamEventCount;
        	publishNewPVs();
        },
        1,
        1,
        TimeUnit.SECONDS);

    /* Now get the packet input stream processor class name.  This is optional, so
     * if its not provided, use the CcsdsPacketInputStream as default. */
    if (config.containsKey("packetInputStreamClassName")) {
      packetInputStreamClassName = config.getString("packetInputStreamClassName");
      if (config.containsKey("packetInputStreamArgs")) {
        packetInputStreamArgs = config.getConfig("packetInputStreamArgs");
      } else {
        packetInputStreamArgs = YConfiguration.emptyConfig();
      }
    } else {
      packetInputStreamClassName = CcsdsPacketInputStream.class.getName();
      packetInputStreamArgs = YConfiguration.emptyConfig();
    }

    /* Now create the packet input stream process */
    try {
      packetInputStream = YObjectLoader.loadObject(packetInputStreamClassName);
    } catch (ConfigurationException e) {
      log.error("Cannot instantiate the packetInput stream", e);
      throw e;
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

    stream = ydb.getStream(STREAM_NAME);
    if (stream == null) {
        throw new ConfigurationException("Stream '" + STREAM_NAME + "' does not exist");
    }
    
    mdb = YamcsServer.getServer().getInstance(yamcsInstance).getXtceDb();
    
    namespace = "/instruments/tvac";
    
    Parameter p = new Parameter("hello1");
    
    p.setQualifiedName("/instruments/tvac/hello1");
    mdb.addParameter(p, true);
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
      return String.format("OK, received %d packets", packetCount.get());
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

      /* Sleep for the configured amount of time.  We normally sleep so we don't needlessly chew up resources. */
      try {
        Thread.sleep(this.period);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

private void publishNewPVs() {
	TupleDefinition tdef = StandardTupleDefinitions.PARAMETER.copy();
//      List<Object> cols = new ArrayList<>(4 + params.size());
     List<Object> cols = new ArrayList<>(4 + 1);
     cols.add(Instant.now().toEpochMilli());
     cols.add(namespace);
     cols.add(0);
     cols.add(Instant.now().toEpochMilli());
//      for (ParameterValue pv : params) {
//          if (pv == null) {
//              log.error("Null parameter value encountered, skipping");
//              continue;
//          }
//          String name = pv.getParameterQualifiedName();
//          int idx = tdef.getColumnIndex(name);
//          if (idx != -1) {
//              log.warn("duplicate value for {}\nfirst: {}\n second: {}", name, cols.get(idx), pv);
//              continue;
//          }
//          tdef.addColumn(name, DataType.PARAMETER_VALUE);
//          cols.add(pv);
//      }
     
     tdef.addColumn("/instruments/tvac/hello1", DataType.PARAMETER_VALUE);
     
     ParameterType ptype = getBasicType(mdb, Yamcs.Value.Type.UINT64, null);
     
     
//      VariableParam p = (VariableParam) mdb.getParameterNames().contains("/tvac/hello1");
//      boolean paramExists = mdb.getParameterNames().contains("/tvac/hello1");
     boolean paramExists = false;
     
//	  System.out.println("Adding new val before***...:" + paramExists);

     if(!paramExists) 
     {
	  VariableParam p = VariableParam.getForFullyQualifiedName("/instruments/tvac/hello1");
//    	  System.out.println("ptype:" + ptype); 
	  p.setParameterType(ptype);
//    	  mdb.addParameter(p, true);
//    	  System.out.println("Adding new val...");
	  cols.add(getPV(p,Instant.now().toEpochMilli(), 1.0));
     }
//      if (p == null) {
//          p = SystemParameter.getForFullyQualifiedName(parameterQualifiedNamed);
//          p.setParameterType(ptype);
//          addParameter(p, true);
//      } else {
//          if (p.getParameterType() != ptype) {
//              throw new IllegalArgumentException("A parameter with name " + parameterQualifiedNamed
//                      + " already exists but has a different type: " + p.getParameterType()
//                      + " The type in the request was: " + ptype);
//          }
//      }
     
     
//      cols.add(new ParameterValue());
//      cols.add(new )
     Tuple t = new Tuple(tdef, cols);
     stream.emitTuple(t);
}
  
  static private ParameterType getOrCreateType(XtceDb mdb, String name, UnitType unit,
          Supplier<ParameterType.Builder<?>> supplier) {

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
          return getOrCreateType(mdb, "binary", unit,
                  () -> new BinaryParameterType.Builder());
      case BOOLEAN:
          return getOrCreateType(mdb, "boolean", unit,
                  () -> new BooleanParameterType.Builder());
      case STRING:
          return getOrCreateType(mdb, "string", unit,
                  () -> new StringParameterType.Builder());
      case FLOAT:
          return getOrCreateType(mdb, "float32", unit,
                  () -> new FloatParameterType.Builder().setSizeInBits(32));
      case DOUBLE:
          return getOrCreateType(mdb, "float64", unit,
                  () -> new FloatParameterType.Builder().setSizeInBits(64));
      case SINT32:
          return getOrCreateType(mdb, "sint32", unit,
                  () -> new IntegerParameterType.Builder().setSizeInBits(32).setSigned(true));
      case SINT64:
          return getOrCreateType(mdb, "sint64", unit,
                  () -> new IntegerParameterType.Builder().setSizeInBits(64).setSigned(true));
      case UINT32:
          return getOrCreateType(mdb, "uint32", unit,
                  () -> new IntegerParameterType.Builder().setSizeInBits(32).setSigned(false));
      case UINT64:
          return getOrCreateType(mdb, "uint64", unit,
                  () -> new IntegerParameterType.Builder().setSizeInBits(64).setSigned(false));
      case TIMESTAMP:
          return getOrCreateType(mdb, "time", unit, () -> new AbsoluteTimeParameterType.Builder());
      case ENUMERATED:
          return getOrCreateType(mdb, "enum", unit, () -> new EnumeratedParameterType.Builder());
      default:
          throw new IllegalArgumentException(type + "is not a basic type");
      }
  }

  public TmPacket getNextPacket() {
    TmPacket pwt = null;
    while (isRunningAndEnabled()) {
      try {
        /* Get a packet from the packet input stream plugin. */
        byte[] packet = packetInputStream.readPacket();
        if (packet == null) {
          /* Something went wrong.  Return null. */
          break;
        }

        TmPacket pkt = new TmPacket(timeService.getMissionTime(), packet);
        pkt.setEarthRceptionTime(timeService.getHresMissionTime());

        /* Give the preprocessor a chance to process the packet. */
        pwt = packetPreprocessor.process(pkt);
        if (pwt != null) {
          /* We successfully processed the packet.  Break out so we can return it. */
          break;
        }
      } catch (EOFException e) {
        /* We read the EOF.  This is not an error condition, so don't throw the exception. Just
         * return a null to let the caller know we're at the end. */
        pwt = null;
        break;
      } catch (PacketTooLongException | IOException e) {
        /* Something went wrong.  Return null. */
        pwt = null;
        e.printStackTrace();
      }
    }

    return pwt;
  }

  @Override
  public void onTuple(Stream stream, Tuple tuple) {
    if (isRunningAndEnabled()) {
      Event event = (Event) tuple.getColumn("body");
      updateStats(event.getMessage().length());
      streamEventCount++;
    }
  }

  @Override
  public void setupSystemParameters(SystemParametersService sysParamCollector) {
    super.setupSystemParameters(sysParamCollector);
    outOfSyncParam =
        sysParamCollector.createSystemParameter(
            linkName + "/outOfSync",
            Yamcs.Value.Type.BOOLEAN,
            "Are the downlinked events not in sync wtih the ones from the log?");
    streamEventCountParam =
        sysParamCollector.createSystemParameter(
            linkName + "/streamEventCountParam",
            Yamcs.Value.Type.UINT64,
            "Event count in realtime event stream");
    logEventCountParam =
        sysParamCollector.createSystemParameter(
            linkName + "/logEventCountParam",
            Yamcs.Value.Type.UINT64,
            "Event count from log files");
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
    list.add(SystemParametersService.getPV(outOfSyncParam, time, outOfSync));
    list.add(SystemParametersService.getPV(streamEventCountParam, time, streamEventCount));
    list.add(SystemParametersService.getPV(logEventCountParam, time, logEventCount));
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
  
  private void initOPCUAConnection() throws ServiceResultException 
  {
//	    if (args.length == 0) {
//	        System.out.println("Usage: SampleClient [server uri]");
//	        return;
//	      }
//	      String url = args[0];

	  //	  FIXME: url should be part of config in YAML
	      String url = "";
	      System.out.print("SampleClient: Connecting to " + url + " .. ");
	      
	      System.out.println("**********************************1");

	      ////////////// CLIENT //////////////
	      // Create Client

	      // Set default key size for created certificates. The default value is also 2048,
	      // but in some cases you may want to specify a different size.
	      CertificateUtils.setKeySize(2048);

	      // Try to load an application certificate with the specified application name.
	      // In case it is not found, a new certificate is created.
	      final KeyPair pair = ExampleKeys.getCert("SampleClient");

	      // Create the client using information provided by the created certificate
	      final Client myClient = Client.createClientApplication(pair);

	      myClient.getApplication().addLocale(Locale.ENGLISH);
	      myClient.getApplication().setApplicationName(new LocalizedText("Java Sample Client", Locale.ENGLISH));
	      myClient.getApplication().setProductUri("urn:JavaSampleClient");
	      
	      System.out.println("**********************************2");

	      // Create a certificate store for handling server certificates.
	      // The constructor uses relative path "SampleClientPKI/CA" as the base directory, storing
	      // rejected certificates in folder "rejected" and trusted certificates in folder "trusted".
	      // To accept a server certificate, a rejected certificate needs to be moved from rejected to
	      // trusted folder. This can be performed by moving the certificate manually, using method
	      // addTrustedCertificate of PkiDirectoryCertificateStore or, as in this example, using a
	      // custom implementation of DefaultCertificateValidatorListener.
	      final PkiDirectoryCertificateStore myCertStore = new PkiDirectoryCertificateStore("SampleClientPKI/CA");

	      // Create a default certificate validator for validating server certificates in the certificate
	      // store.
	      final DefaultCertificateValidator myValidator = new DefaultCertificateValidator(myCertStore);

	      // Set MyValidationListener instance as the ValidatorListener. In case a certificate is not
	      // automatically accepted, user can choose to reject or accept the certificate.
	      final MyValidationListener myValidationListener = new MyValidationListener();
	      myValidator.setValidationListener(myValidationListener);

	      // Set myValidator as the validator for OpcTcp and Https
	      myClient.getApplication().getOpctcpSettings().setCertificateValidator(myValidator);
	      myClient.getApplication().getHttpsSettings().setCertificateValidator(myValidator);

	      // The HTTPS SecurityPolicies are defined separate from the endpoint securities
	      myClient.getApplication().getHttpsSettings().setHttpsSecurityPolicies(HttpsSecurityPolicy.ALL_104);

	      // The certificate to use for HTTPS
	      KeyPair myHttpsCertificate = ExampleKeys.getHttpsCert("SampleClient");
	      myClient.getApplication().getHttpsSettings().setKeyPair(myHttpsCertificate);

	      SessionChannel mySession = myClient.createSessionChannel(url);
	      // mySession.activate("username", "123");
	      mySession.activate();
	      //////////////////////////////////////

	      ///////////// EXECUTE //////////////
	      // Browse Root
	      BrowseDescription browse = new BrowseDescription();
	      browse.setNodeId(Identifiers.RootFolder);
	      browse.setBrowseDirection(BrowseDirection.Forward);
	      browse.setIncludeSubtypes(true);
	      browse.setNodeClassMask(NodeClass.Object, NodeClass.Variable);
	      browse.setResultMask(BrowseResultMask.All);
	      BrowseResponse res3 = mySession.Browse(null, null, null, browse);
	      System.out.println("**********************************3");
	      System.out.println(res3);

	      // Read Namespace Array
	      ReadResponse res5 = mySession.Read(null, null, TimestampsToReturn.Neither,
	          new ReadValueId(Identifiers.Server_NamespaceArray, Attributes.Value, null, null));
	      String[] namespaceArray = (String[]) res5.getResults()[0].getValue().getValue();
	      System.out.println("**********************************4");
	      System.out.println(Arrays.toString(namespaceArray));

	      // Read a variable (Works with NanoServer example!)
	      ReadResponse res4 = mySession.Read(null, 500.0, TimestampsToReturn.Source,
	          new ReadValueId(new NodeId(1, "Boolean"), Attributes.Value, null, null));
	      System.out.println("**********************************5");
	      System.out.println(res4);

	      // Press enter to shutdown
	      System.out.println("Enter 'x' to shutdown");
//	      while (System.in.read() != 'x') {
//	        ;
//	      }

	      ///////////// SHUTDOWN /////////////
	      mySession.close();
	      mySession.closeAsync();
	      //////////////////////////////////////
	  
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
}
