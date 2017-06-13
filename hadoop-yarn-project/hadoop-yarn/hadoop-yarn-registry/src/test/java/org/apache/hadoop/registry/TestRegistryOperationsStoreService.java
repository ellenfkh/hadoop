package org.apache.hadoop.registry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.registry.client.impl.RegistryOperationsStoreService;
import org.junit.BeforeClass;

public class TestRegistryOperationsStoreService
    extends BaseRegistryOperationsTest {

  @BeforeClass
  public static void setupRegistry() throws Exception {
    registry = new RegistryOperationsStoreService("testStoreRegistry");
    ((RegistryOperationsStoreService) registry).serviceInit(new Configuration());
  }

}
