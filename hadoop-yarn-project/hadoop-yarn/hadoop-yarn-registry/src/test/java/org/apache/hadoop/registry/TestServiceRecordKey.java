package org.apache.hadoop.registry;

import org.apache.hadoop.registry.client.api.records.ApplicationServiceRecordKey;
import org.apache.hadoop.registry.client.api.records.ContainerServiceRecordKey;
import org.apache.hadoop.registry.client.api.records.CoreServiceRecordKey;
import org.apache.hadoop.registry.client.exceptions.InvalidRegistryKeyException;
import org.junit.Assert;
import org.junit.Test;

public class TestServiceRecordKey {

  @Test
  public void testCoreServiceRecodKey() throws InvalidRegistryKeyException {
    CoreServiceRecordKey key =
        new CoreServiceRecordKey("serviceClass", "instanceName");
    Assert.assertEquals(key.getServiceClass(), "serviceClass");
    Assert.assertEquals(key.getInstanceName(), "instanceName");

    Assert.assertEquals(key,
        new CoreServiceRecordKey("serviceClass", "instanceName"));

    Assert.assertNotEquals(key,
        new CoreServiceRecordKey("serviceClass2", "instanceName"));
    Assert.assertNotEquals(key,
        new CoreServiceRecordKey("serviceClass", "instanceName2"));

    Assert.assertNotEquals(key, new ContainerServiceRecordKey("serviceClass",
        "userName", "appId", "containerId"));
    Assert.assertNotEquals(key,
        new ApplicationServiceRecordKey("serviceClass", "userName", "appId"));
  }

  @Test
  public void testApplicationServiceRecodKey()
      throws InvalidRegistryKeyException {
    ApplicationServiceRecordKey key =
        new ApplicationServiceRecordKey("userName", "serviceClass", "appId");
    Assert.assertEquals(key.getServiceClass(), "serviceClass");
    Assert.assertEquals(key.getUsername(), "userName");
    Assert.assertEquals(key.getAppId(), "appId");

    Assert.assertEquals(key,
        new ApplicationServiceRecordKey("userName", "serviceClass", "appId"));

    Assert.assertNotEquals(key,
        new ApplicationServiceRecordKey("userName", "serviceClass2", "appId"));
    Assert.assertNotEquals(key,
        new ApplicationServiceRecordKey("userName2", "serviceClass", "appId"));
    Assert.assertNotEquals(key,
        new ApplicationServiceRecordKey("userName", "serviceClass", "appId2"));

    Assert.assertNotEquals(key, new ContainerServiceRecordKey("userName",
        "serviceClass", "appId", "containerId"));
    Assert.assertNotEquals(key,
        new CoreServiceRecordKey("serviceClass", "instanceName"));

  }

  @Test
  public void testContainerServiceRecodKey()
      throws InvalidRegistryKeyException {
    ContainerServiceRecordKey key = new ContainerServiceRecordKey("userName",
        "serviceClass", "appId", "containerId");
    Assert.assertEquals(key.getServiceClass(), "serviceClass");
    Assert.assertEquals(key.getUsername(), "userName");
    Assert.assertEquals(key.getAppId(), "appId");
    Assert.assertEquals(key.getContainerId(), "containerId");

    Assert.assertEquals(key, new ContainerServiceRecordKey("userName",
        "serviceClass", "appId", "containerId"));

    Assert.assertNotEquals(key, new ContainerServiceRecordKey("userName",
        "serviceClass2", "appId", "containerId"));
    Assert.assertNotEquals(key, new ContainerServiceRecordKey("userName2",
        "serviceClass", "appId", "containerId"));
    Assert.assertNotEquals(key, new ContainerServiceRecordKey("userName",
        "serviceClass", "appId2", "containerId2"));
    Assert.assertNotEquals(key, new ContainerServiceRecordKey("userName",
        "serviceClass", "appId", "containerId2"));

    Assert.assertNotEquals(key,
        new ApplicationServiceRecordKey("userName", "serviceClass", "appId"));
    Assert.assertNotEquals(key,
        new CoreServiceRecordKey("serviceClass", "instanceName"));

  }

  /*
   * Make sure an empty or null field cannot be passed in to constructor. Equals
   * and hash don't do null/empty check, so be very pedantic.
   */

  @Test
  public void testCoreServiceRecordKeyBadConstructor()
      throws InvalidRegistryKeyException {
    // Bad instance name
    try {
      new CoreServiceRecordKey("serviceClass", null);
      Assert.fail("Instance name was null; should throw exception");
    } catch (InvalidRegistryKeyException e) {
    }
    try {
      new CoreServiceRecordKey("serviceClass", "");
      Assert.fail("Instance name was empty; should throw exception");
    } catch (InvalidRegistryKeyException e) {
    }

    // Bad service class
    try {
      new CoreServiceRecordKey(null, "instanceName");
      Assert.fail("Service class was null; should throw exception");
    } catch (InvalidRegistryKeyException e) {
    }
    try {
      new CoreServiceRecordKey("", "instanceName");
      Assert.fail("Service class was empty; should throw exception");
    } catch (InvalidRegistryKeyException e) {
    }
  }

  @Test
  public void testApplicationServiceRecordKeyBadConstructor()
      throws InvalidRegistryKeyException {
    // Bad appId
    try {
      new ApplicationServiceRecordKey("username", "serviceClass", null);
      Assert.fail("ApplicationId was null; should throw exception");
    } catch (InvalidRegistryKeyException e) {
    }
    try {
      new ApplicationServiceRecordKey("username", "serviceClass", "");
      Assert.fail("ApplicationId was empty; should throw exception");
    } catch (InvalidRegistryKeyException e) {
    }

    // Bad username
    try {
      new ApplicationServiceRecordKey(null, "serviceClass", "appId");
      Assert.fail("Username was null; should throw exception");
    } catch (InvalidRegistryKeyException e) {
    }
    try {
      new ApplicationServiceRecordKey("", "serviceClass", "appId");
      Assert.fail("Username was empty; should throw exception");
    } catch (InvalidRegistryKeyException e) {
    }

    // Bad service class
    try {
      new ApplicationServiceRecordKey("username", null, "appId");
      Assert.fail("Service class was null; should throw exception");
    } catch (InvalidRegistryKeyException e) {
    }
    try {
      new ApplicationServiceRecordKey("username", "", "appId");
      Assert.fail("Service class was empty; should throw exception");
    } catch (InvalidRegistryKeyException e) {
    }
  }

  @Test
  public void testContainerServiceRecordKeyBadConstructor()
      throws InvalidRegistryKeyException {
    // Bad containerId
    try {
      new ContainerServiceRecordKey("username", "serviceClass", "appId", null);
      Assert.fail("ContainerId was null; should throw exception");
    } catch (InvalidRegistryKeyException e) {
    }
    try {
      new ContainerServiceRecordKey("username", "serviceClass", "appId", "");
      Assert.fail("ContainerId was empty; should throw exception");
    } catch (InvalidRegistryKeyException e) {
    }

    // Bad appId
    try {
      new ContainerServiceRecordKey("username", "serviceClass", null,
          "ContainerId");
      Assert.fail("ApplicationId was null; should throw exception");
    } catch (InvalidRegistryKeyException e) {
    }
    try {
      new ContainerServiceRecordKey("username", "serviceClass", "",
          "containerId");
      Assert.fail("ApplicationId was empty; should throw exception");
    } catch (InvalidRegistryKeyException e) {
    }

    // Bad username
    try {
      new ContainerServiceRecordKey(null, "serviceClass", "appId",
          "ContainerId");
      Assert.fail("Username was null; should throw exception");
    } catch (InvalidRegistryKeyException e) {
    }
    try {
      new ContainerServiceRecordKey("", "serviceClass", "appId", "ContainerId");
      Assert.fail("Username was empty; should throw exception");
    } catch (InvalidRegistryKeyException e) {
    }

    // Bad service class
    try {
      new ContainerServiceRecordKey("username", null, "appId", "ContainerId");
      Assert.fail("Service class was null; should throw exception");
    } catch (InvalidRegistryKeyException e) {
    }
    try {
      new ContainerServiceRecordKey("username", "", "appId", "ContainerId");
      Assert.fail("Service class was empty; should throw exception");
    } catch (InvalidRegistryKeyException e) {
    }
  }

}
