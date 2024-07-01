[![CI](https://github.com/WindhoverLabs/yamcs-opcua/actions/workflows/ci.yml/badge.svg)](https://github.com/WindhoverLabs/yamcs-opcua/actions/workflows/ci.yml)  [![Coverage Status](https://coveralls.io/repos/github/WindhoverLabs/yamcs-opcua/badge.svg?branch=1_minimally_functional_plugin)](https://coveralls.io/github/WindhoverLabs/yamcs-opcua?branch=1_minimally_functional_plugin)



### To Integrate with YAMCS
```yaml

  - name: tm_ocpua
    class: com.windhoverlabs.yamcs.opcua.OPCUALink
    # stream: tm_realtime
    opcuaStream: "opcua_params"
    # endpoint_url: "opc.tcp://localhost:4840/"
    endpointUrl: "opc.tcp://localhost:12686/milo"
    discoveryUrl: "opc.tcp://pop-os:12686/milo/discovery"
    parametersNamespace: "/instruments/tvac"
    queryAllNodesAtStartup: true # defaults to false
    
    # i 	Numeric (UInteger)
    # s 	String (String)
    # g 	Guid (Guid)
    # b 	Opaque (ByteString)

    nodePaths:
      - path: "2:HelloWorld,2:MyObject,2:Bar"
        rootNodeID: #Link starts browsing at this node
          namespaceIndex: 0
          identifierType: Numeric
          identifier: "85"  #85 is Root

      - path: "2:HelloWorld,2:MyObject,2:Foo"
        rootNodeID: #Link starts browsing at this node
          namespaceIndex: 0
          identifierType: Numeric
          identifier: "85"  #85 is Objects


    rootNodeID: #Link starts browsing at this node
      namespaceIndex: 0
      identifierType: Numeric
      identifier: "84"  #84 is Root

# 0:Root,0:Objects,2:HelloWorld,2:MyObject,2:Bar

  # - name: mavlink
  #   class: com.windhoverlabs.yamcs.mavlink.MAVLink
  #   opcua_stream: "opcua_params"
  #   parameters_namespace: "/instruments/tvac"


```