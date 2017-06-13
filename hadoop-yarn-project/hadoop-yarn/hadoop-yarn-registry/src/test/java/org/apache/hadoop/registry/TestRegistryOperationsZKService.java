package org.apache.hadoop.registry;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.registry.client.impl.zk.RegistryOperationsZKService;
import org.apache.hadoop.registry.server.services.AddingCompositeService;
import org.apache.hadoop.registry.server.services.MicroZookeeperService;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestRegistryOperationsZKService
    extends BaseRegistryOperationsTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestRegistryOperationsZKService.class);

  private static final AddingCompositeService servicesToTeardown =
      new AddingCompositeService("teardown");

  // static initializer guarantees it is always started
  // ahead of any @BeforeClass methods
  static {
    servicesToTeardown.init(new Configuration());
    servicesToTeardown.start();
  }

  private static MicroZookeeperService zookeeper;

  protected static void addToTeardown(Service svc) {
    servicesToTeardown.addService(svc);
  }

  @AfterClass
  public static void teardownServices() throws IOException {
    RegistryTestHelper.describe(LOG, "teardown of static services");
    servicesToTeardown.close();
  }

  @BeforeClass
  public static void setupRegistry() throws Exception {
    registry = new RegistryOperationsZKService();
    Configuration conf = createZKServer();
    ((RegistryOperationsZKService) registry).init(conf);

    ((RegistryOperationsZKService) registry).start();
  }

  @AfterClass
  public static void shutdownRegistry() throws Exception {
    ((RegistryOperationsZKService) registry).stop();

  }

  public static Configuration createZKServer() throws Exception {
    // File zkDir = new File("target/zookeeper");
    // FileUtils.deleteDirectory(zkDir);
    // RegistryTestHelper.assertTrue(zkDir.mkdirs());
    zookeeper = new MicroZookeeperService("InMemoryZKService");
    YarnConfiguration conf = new YarnConfiguration();
    // conf.set(MicroZookeeperServiceKeys.KEY_ZKSERVICE_DIR,
    // zkDir.getAbsolutePath());
    zookeeper.init(conf);
    zookeeper.start();
    addToTeardown(zookeeper);
    return conf;
  }

}
