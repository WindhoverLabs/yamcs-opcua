/*
 * Copyright (c) 2021 the Eclipse Milo Authors
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 */

package com.windhoverlabs.yamcs.opcua.test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.Security;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.api.config.OpcUaClientConfig;
import org.eclipse.milo.opcua.stack.client.DiscoveryClient;
import org.eclipse.milo.opcua.stack.core.Stack;
import org.eclipse.milo.opcua.stack.core.security.DefaultTrustListManager;
import org.eclipse.milo.opcua.stack.core.types.structured.EndpointDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientExampleRunner {

  static {
    // Required for SecurityPolicy.Aes256_Sha256_RsaPss
    Security.addProvider(new BouncyCastleProvider());
  }

  private final Logger logger = LoggerFactory.getLogger(getClass());

  private final CompletableFuture<OpcUaClient> future = new CompletableFuture<>();

  private DefaultTrustListManager trustListManager;

  private final ClientExample clientExample;

  public ClientExampleRunner(ClientExample clientExample) throws Exception {
    this(clientExample, true);
  }

  public ClientExampleRunner(ClientExample clientExample, boolean serverRequired) throws Exception {
    this.clientExample = clientExample;
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
        DiscoveryClient.getEndpoints(clientExample.getEndpointUrl()).get();

    OpcUaClientConfig builder = OpcUaClientConfig.builder().setEndpoint(endpoint.get(0)).build();

    return OpcUaClient.create(builder);
  }

  public void run() {
    try {
      OpcUaClient client = createClient();

      future.whenCompleteAsync(
          (c, ex) -> {
            if (ex != null) {
              logger.error("Error running example: {}", ex.getMessage(), ex);
            }

            try {
              client.disconnect().get();
              Stack.releaseSharedResources();
            } catch (InterruptedException | ExecutionException e) {
              logger.error("Error disconnecting: {}", e.getMessage(), e);
            }

            try {
              Thread.sleep(1000);
              System.exit(0);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          });

      try {
        clientExample.run(client, future);
        future.get(15, TimeUnit.SECONDS);
      } catch (Throwable t) {
        logger.error("Error running client example: {}", t.getMessage(), t);
        future.completeExceptionally(t);
      }
    } catch (Throwable t) {
      logger.error("Error getting client: {}", t.getMessage(), t);

      future.completeExceptionally(t);

      try {
        Thread.sleep(1000);
        System.exit(0);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    try {
      Thread.sleep(999_999_999);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
