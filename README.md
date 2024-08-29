[![CI](https://github.com/WindhoverLabs/yamcs-opcua/actions/workflows/ci.yml/badge.svg)](https://github.com/WindhoverLabs/yamcs-opcua/actions/workflows/ci.yml)  [![Coverage Status](https://coveralls.io/repos/github/WindhoverLabs/yamcs-opcua/badge.svg?branch=1_minimally_functional_plugin)](https://coveralls.io/github/WindhoverLabs/yamcs-opcua?branch=1_minimally_functional_plugin)



### To Integrate with YAMCS
```yaml

  - name: tm_ocpua
    class: com.windhoverlabs.yamcs.opcua.OPCUALink
    opcuaStream: "opcua_params"
    xtceOutputFile: opcua_xtce.xml
    endpointUrl: "opc.tcp://localhost:12686/milo"
    discoveryUrl: "opc.tcp://localhost:12686/milo/discovery"
    parametersNamespace: "/instruments/tvac"
    queryAllNodesAtStartup: true # defaults to false
    publishInterval: 100
    subStrikeCountCheckTimeoutSecs: 15
    subStrikeCountThreshold: 1
    nodePaths:
      - path: "2:HelloWorld,2:MyObject,2:Bar"
        rootNodeID: #Link starts browsing at this node
          namespaceIndex: 0
          identifierType: Numeric
          identifier: "85"  #84 is Root

      - path: "2:HelloWorld,2:MyObject,2:Foo"
        rootNodeID: #Link starts browsing at this node
          namespaceIndex: 0
          identifierType: Numeric
          identifier: "85"  #84 is Root
```

### YAMCS Version 5.8.8 and Beyond

After the changes made in [yamcs 5.8.8](https://github.com/yamcs/yamcs/commit/7abba0a93013e8b4ec1020be3df592191614da33),
the namespace specified in "parametersNamespace" *must* be specified inside an XCTE file, otherwise YAMCS will not permit
writing the new PVs(specified in nodePaths key) to that namespace as it will not be a "writeable" spacesystem/namespace.
If you aren't able to add a new XTCE file to your yamcs configuration, then one way to work around this is
by prefixing the namespace specified in "parametersNamespace" with "/yamcs". The namespace shown in the  example above would be
"/yamcs/instruments/tvac". Though it is recommended tok just add a new XTCE file like the one shown below.

```xml

<?xml version="1.0" encoding="UTF-8"?>
<SpaceSystem xmlns="http://www.omg.org/spec/XTCE/20180204" name="instruments">
      <SpaceSystem name="tvac"></SpaceSystem>

</SpaceSystem>

```

Of course your xml file will look slightly different if your parametersNamespace has different names and depth.

Configure the mdb accordingly:

```yaml


mdb:    
	#Adding "writable" Due to changes in https://github.com/yamcs/yamcs/commit/9be9328690fbb305ec7cdab461f3fe0e1c77067b 
	- type: "xtce"
	  args:
	    file: "mdb/opcua.xml"
	  writable: true

```

Ensure `writable` is set to true.



###  Notes For Users

- At startup, the link will connect to the OPCUA server that is specified on the YAML config, in the format specified above.
  Users can track the status of the link by looking at the "/yamcs/pop-os/tm_ocpua/OPCUAStatusParam" PV.
  Depending on the configuration and server performance/configuration, the link may take a while to read the nodes from the server.
  In particular the link may spend a lot of time at startup on the "OPCUA_INIT_TREE" state. It is completely normal
  if it spends a lot of time on that state, just let it be .
  Once the value of "/yamcs/pop-os/tm_ocpua/OPCUAStatusParam" is set to "OPCUA_OK", it means the link is done with
  all initial setup(data subscriptions, nodes/tree browsing, YAMCS PV mapping, etc) . Again; all of this
  is highly dependent on configuration (depth of specified root nodes for example), OPCUA server implementation
  you are connecting to and even the speed of your network.
  
  
  

