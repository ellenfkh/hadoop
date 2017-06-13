package org.apache.hadoop.registry;

import org.apache.hadoop.registry.client.api.records.ServiceRecordKey;
import org.apache.hadoop.registry.client.api.records.CoreServiceRecordKey;
import org.apache.hadoop.registry.client.exceptions.InvalidRecordException;
import org.apache.hadoop.registry.client.exceptions.InvalidRegistryKeyException;
import org.apache.hadoop.registry.client.exceptions.NoRecordException;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.registry.client.api.records.ApplicationServiceRecordKey;
import org.apache.hadoop.registry.client.api.records.ContainerServiceRecordKey;

import java.io.IOException;

import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public abstract class BaseRegistryOperationsTest {

  protected static RegistryOperations registry;

  /**
   * Register, resolve, and deregister a key-record pair
   * 
   * @throws InvalidRegistryKeyException
   * @throws InvalidRecordException
   * @throws IOException
   */
  @Test
  public void testRegisterResolveDeregister()
      throws InvalidRegistryKeyException, InvalidRecordException, IOException {
    ServiceRecordKey key =
        new CoreServiceRecordKey("serviceClass", "instanceName");

    // Register, resolve, check result
    registry.register(key, createRecordWithDescription("key"));
    ServiceRecord sr = registry.resolve(key);
    Assert.assertEquals(createRecordWithDescription("key"), sr);

    // Deregister and expect exceptions on resolve and re-deregister
    registry.deregister(key);

    try {
      registry.resolve(key);
      Assert.fail();
    } catch (NoRecordException e) {
      System.out.println(e);
    }

    try {
      registry.deregister(key);
      Assert.fail();
    } catch (NoRecordException e) {
      System.out.println(e);
    }

    // Reregister, resolve, check result
    sr = null;
    registry.register(key, new ServiceRecord());
    sr = registry.resolve(key);
    Assert.assertEquals(new ServiceRecord(), sr);

  }

  @Test
  public void testRegistryMultipleEntries()
      throws InvalidRegistryKeyException, InvalidRecordException, IOException {
    ServiceRecordKey key1 =
        new CoreServiceRecordKey("serviceClass", "instanceName1");
    ServiceRecordKey key2 =
        new CoreServiceRecordKey("serviceClass", "instanceName2");

    registry.register(key1, createRecordWithDescription("key1"));
    registry.register(key2, createRecordWithDescription("key2"));

    ServiceRecord sr1 = registry.resolve(key1);
    Assert.assertEquals("key1", sr1.description);

    registry.deregister(key1);
    try {
      registry.resolve(key1);
      Assert.fail();
    } catch (NoRecordException e) {
      System.out.println(e);
    }

    // Make sure that the second entry has not been deleted
    ServiceRecord sr2 = registry.resolve(key2);
    Assert.assertEquals("key2", sr2.description);
  }

  @Test
  public void testRegistryOverwrite()
      throws InvalidRecordException, InvalidRegistryKeyException, IOException {
    ServiceRecordKey key =
        new CoreServiceRecordKey("serviceClass", "instanceName");
    registry.register(key, createRecordWithDescription("key1"));

    ServiceRecord sr = registry.resolve(key);
    Assert.assertEquals("key1", sr.description);
    sr = null;

    registry.register(key, createRecordWithDescription("key2"));
    sr = registry.resolve(key);
    Assert.assertEquals("key2", sr.description);
  }

  private ServiceRecord createRecordWithDescription(String description) {
    ServiceRecord sr = new ServiceRecord();
    sr.description = description;
    return sr;
  }
}
