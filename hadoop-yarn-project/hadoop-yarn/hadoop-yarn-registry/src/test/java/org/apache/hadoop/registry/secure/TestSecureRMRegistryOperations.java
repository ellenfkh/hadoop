/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.registry.secure;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.PathPermissionException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.ServiceStateException;
import org.apache.hadoop.registry.client.api.RegistryConstants;
import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.registry.client.api.RegistryOperationsFactory;
import org.apache.hadoop.registry.client.api.records.ServiceRecordKey;
import org.apache.hadoop.registry.client.api.records.ApplicationServiceRecordKey;
import org.apache.hadoop.registry.client.api.records.ContainerServiceRecordKey;
import org.apache.hadoop.registry.client.api.records.CoreServiceRecordKey;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.registry.client.exceptions.InvalidRegistryKeyException;
import org.apache.hadoop.registry.client.exceptions.NoPathPermissionsException;
import org.apache.hadoop.registry.client.impl.zk.ZKPathDumper;
import org.apache.hadoop.registry.client.impl.RegistryOperationsZKClient;
import org.apache.hadoop.registry.client.impl.zk.RegistrySecurity;
import org.apache.hadoop.registry.client.impl.zk.ZookeeperConfigOptions;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.registry.server.integration.RMRegistryOperationsService;
import org.apache.hadoop.registry.server.services.ZKRegistryAdminService;
import org.apache.zookeeper.client.ZooKeeperSaslClient;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.LoginException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.List;

import static org.apache.hadoop.registry.client.api.RegistryConstants.*;

/**
 * Verify that the {@link RMRegistryOperationsService} works securely
 */
public class TestSecureRMRegistryOperations extends AbstractSecureRegistryTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestSecureRMRegistryOperations.class);
  private Configuration secureConf;
  private Configuration zkClientConf;
  private UserGroupInformation zookeeperUGI;

  @Before
  public void setupTestSecureRMRegistryOperations() throws Exception {
    startSecureZK();
    secureConf = new Configuration();
    secureConf.setBoolean(KEY_REGISTRY_SECURE, true);

    // create client conf containing the ZK quorum
    zkClientConf = new Configuration(secureZK.getConfig());
    zkClientConf.setBoolean(KEY_REGISTRY_SECURE, true);
    assertNotEmpty(zkClientConf.get(RegistryConstants.KEY_REGISTRY_ZK_QUORUM));

    // ZK is in charge
    secureConf.set(KEY_REGISTRY_SYSTEM_ACCOUNTS, "sasl:zookeeper@");
    zookeeperUGI = loginUGI(ZOOKEEPER, keytab_zk);
  }

  @After
  public void teardownTestSecureRMRegistryOperations() {
  }

  /**
   * Create the RM registry operations as the current user
   * @return the service
   * @throws LoginException
   * @throws FileNotFoundException
   */
  public RMRegistryOperationsService startRMRegistryOperations() throws
      LoginException, IOException, InterruptedException {
    // kerberos
    secureConf.set(KEY_REGISTRY_CLIENT_AUTH,
        REGISTRY_CLIENT_AUTH_KERBEROS);
    secureConf.set(KEY_REGISTRY_CLIENT_JAAS_CONTEXT, ZOOKEEPER_CLIENT_CONTEXT);

    RMRegistryOperationsService registryOperations = zookeeperUGI.doAs(
        new PrivilegedExceptionAction<RMRegistryOperationsService>() {
          @Override
          public RMRegistryOperationsService run() throws Exception {
            RMRegistryOperationsService operations
                = new RMRegistryOperationsService("rmregistry", secureZK);
            addToTeardown(operations);
            operations.init(secureConf);
            LOG.info(operations.bindingDiagnosticDetails());
            operations.start();
            return operations;
          }
        });

    return registryOperations;
  }

  /**
   * test that ZK can write as itself
   * 
   * @throws Throwable
   */
  @Test
  public void testZookeeperCanWriteUnderSystem() throws Throwable {

    RMRegistryOperationsService rmRegistryOperations =
        startRMRegistryOperations();
    RegistryOperations operations = rmRegistryOperations;
    ServiceRecordKey key = new CoreServiceRecordKey("hdfs", "system_instance");
    operations.register(key, new ServiceRecord());
    ZKPathDumper pathDumper = rmRegistryOperations.dumpPath(true);
    LOG.info(pathDumper.toString());
  }

  @Test
  public void testAnonReadAccess() throws Throwable {
    RMRegistryOperationsService rmRegistryOperations =
        startRMRegistryOperations();
    describe(LOG, "testAnonReadAccess");
    RegistryOperations operations =
        RegistryOperationsFactory.createAnonymousInstance(zkClientConf);
    addToTeardown(operations);
    operations.start();

    assertFalse("RegistrySecurity.isClientSASLEnabled()==true",
        RegistrySecurity.isClientSASLEnabled());
    operations.list(PATH_SYSTEM_SERVICES);
  }

  @Test
  public void testAnonNoWriteAccess() throws Throwable {
    RMRegistryOperationsService rmRegistryOperations =
        startRMRegistryOperations();
    describe(LOG, "testAnonNoWriteAccess");
    RegistryOperations operations =
        RegistryOperationsFactory.createAnonymousInstance(zkClientConf);
    addToTeardown(operations);
    operations.start();
    ZKPathDumper pathDumper = rmRegistryOperations.dumpPath(true);    
    ServiceRecordKey key = new CoreServiceRecordKey("hdfs", "system_instance");
    expectMkNodeFailure(operations, key);
  }


  /**
   * Expect a mknode operation to fail
   * @param operations operations instance
   * @param path path
   * @throws IOException An IO failure other than those permitted
   * @throws InvalidRegistryKeyException 
   */
  private void expectMkNodeFailure(RegistryOperations operations,
      ServiceRecordKey key) throws IOException, InvalidRegistryKeyException {
    try {
      operations.register(key, new ServiceRecord());
      fail("should have failed to create a node under " +  RegistryUtils.getPathForServiceRecordKey(key));
    } catch (PathPermissionException expected) {
      // expected
    } catch (NoPathPermissionsException expected) {
      // expected
    }
  }

  /**
   * Expect a delete operation to fail
   * @param operations operations instance
   * @param path path
   * @param recursive
   * @throws IOException An IO failure other than those permitted
   * @throws InvalidRegistryKeyException 
   */
  private void expectDeleteFailure(RegistryOperations operations,
      ServiceRecordKey key) throws IOException, InvalidRegistryKeyException {
    try {
      operations.deregister(key);
      fail("should have failed to delete the node " + RegistryUtils.getPathForServiceRecordKey(key));
    } catch (PathPermissionException expected) {
      // expected
    } catch (NoPathPermissionsException expected) {
      // expected
    }
  }

  @Test
  public void testAlicePathRestrictedAnonAccess() throws Throwable {
    RMRegistryOperationsService rmRegistryOperations =
        startRMRegistryOperations();
    String aliceHome = rmRegistryOperations.initUserRegistry(ALICE);
    describe(LOG, "Creating anonymous accessor");
    RegistryOperations anonOperations =
        RegistryOperationsFactory.createAnonymousInstance(zkClientConf);
    addToTeardown(anonOperations);
    anonOperations.start();
    anonOperations.list(aliceHome);

    ServiceRecordKey key =
        new ApplicationServiceRecordKey("alice", "hdfs", "app");

    expectMkNodeFailure(anonOperations, key);
    rmRegistryOperations.register(key, new ServiceRecord());
    anonOperations.resolve(key);
    expectDeleteFailure(anonOperations, key);
  }

  @Test
  public void testUserHomedirsPermissionsRestricted() throws Throwable {
    // test that the /users/$user permissions are restricted
    RMRegistryOperationsService rmRegistryOperations =
        startRMRegistryOperations();
    // create Alice's dir, so it should have an ACL for Alice
    final String home = rmRegistryOperations.initUserRegistry(ALICE);
    List<ACL> acls = rmRegistryOperations.zkGetACLS(home);
    ACL aliceACL = null;
    for (ACL acl : acls) {
      LOG.info(RegistrySecurity.aclToString(acl));
      Id id = acl.getId();
      if (id.getScheme().equals(ZookeeperConfigOptions.SCHEME_SASL)
          && id.getId().startsWith(ALICE)) {

        aliceACL = acl;
        break;
      }
    }
    assertNotNull(aliceACL);
    assertEquals(ZKRegistryAdminService.USER_HOMEDIR_ACL_PERMISSIONS,
        aliceACL.getPerms());
  }

  @Test(expected = ServiceStateException.class)
  public void testNoDigestAuthMissingId2() throws Throwable {
    zkClientConf.set(KEY_REGISTRY_CLIENT_AUTH, REGISTRY_CLIENT_AUTH_DIGEST);
    zkClientConf.set(KEY_REGISTRY_CLIENT_AUTHENTICATION_ID, "");
    zkClientConf.set(KEY_REGISTRY_CLIENT_AUTHENTICATION_PASSWORD, "pass");
    RegistryOperationsFactory.createInstance("DigestRegistryOperations",
        zkClientConf);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNoDigestAuthMissingPass() throws Throwable {
    RegistryOperationsFactory.createAuthenticatedInstance(zkClientConf,
        "id",
        "");
  }

  @Test(expected = ServiceStateException.class)
  public void testNoDigestAuthMissingPass2() throws Throwable {
    zkClientConf.set(KEY_REGISTRY_CLIENT_AUTH, REGISTRY_CLIENT_AUTH_DIGEST);
    zkClientConf.set(KEY_REGISTRY_CLIENT_AUTHENTICATION_ID, "id");
    zkClientConf.set(KEY_REGISTRY_CLIENT_AUTHENTICATION_PASSWORD, "");
    RegistryOperationsFactory.createInstance("DigestRegistryOperations",
        zkClientConf);
  }

}
