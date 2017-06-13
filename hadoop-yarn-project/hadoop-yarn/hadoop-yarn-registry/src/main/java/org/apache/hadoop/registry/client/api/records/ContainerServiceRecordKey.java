package org.apache.hadoop.registry.client.api.records;

import org.apache.hadoop.registry.client.exceptions.InvalidRegistryKeyException;

public class ContainerServiceRecordKey implements ServiceRecordKey {
  private final String appId;
  private final String containerId;
  private String serviceClass;
  private final String username;

  public ContainerServiceRecordKey(String username, String serviceClass,
      String appId, String containerId) throws InvalidRegistryKeyException {
    this.serviceClass = serviceClass;
    this.username = username;
    this.appId = appId;
    this.containerId = containerId;

    if (!validate()) {
      throw new InvalidRegistryKeyException(this.toString());
    }
  }

  @Override
  public boolean validate() {
    if (this.serviceClass == null || this.serviceClass.isEmpty())
      return false;
    if (this.username == null || this.username.isEmpty())
      return false;
    if (this.appId == null || this.appId.isEmpty())
      return false;
    if (this.containerId == null || this.containerId.isEmpty())
      return false;
    return true;
  }

  public String getAppId() {
    return appId;
  }

  public String getContainerId() {
    return containerId;
  }

  public String getServiceClass() {
    return serviceClass;
  }

  public String getUsername() {
    return username;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    ContainerServiceRecordKey other = (ContainerServiceRecordKey) obj;
    if (!appId.equals(other.appId))
      return false;
    if (!containerId.equals(other.containerId))
      return false;
    if (!serviceClass.equals(other.serviceClass))
      return false;
    if (!username.equals(other.username))
      return false;
    return true;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + appId.hashCode();
    result = prime * result + containerId.hashCode();
    result = prime * result + serviceClass.hashCode();
    result = prime * result + username.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return String.format(
        "ContainerServiceRecordKey [serviceClass=%s, username=%s, appId=%s, containerId=%s]",
        serviceClass, username, appId, containerId);
  }
}
