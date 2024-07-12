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
  
  

