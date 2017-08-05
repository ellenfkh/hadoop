package org.apache.hadoop.registry.api;

import org.apache.hadoop.registry.client.exceptions.InvalidRegistryKeyException;

public class CoreServiceRecordKey implements ServiceRecordKey {
  private String instanceName;
  private String serviceClass;

  public CoreServiceRecordKey(String serviceClass, String instanceName)
      throws InvalidRegistryKeyException {
    this.serviceClass = serviceClass;
    this.instanceName = instanceName;

    if (!validate()) {
      throw new InvalidRegistryKeyException(this.toString());
    }
  }

  @Override
  public boolean validate() {
    if (this.serviceClass == null || this.serviceClass.isEmpty())
      return false;
    if (this.instanceName == null || this.instanceName.isEmpty())
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + instanceName.hashCode();
    result = prime * result + serviceClass.hashCode();
    return result;
  }

  public String getServiceClass() {
    return serviceClass;
  }

  public String getInstanceName() {
    return instanceName;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    CoreServiceRecordKey other = (CoreServiceRecordKey) obj;
    if (!instanceName.equals(other.instanceName))
      return false;
    if (!serviceClass.equals(other.serviceClass))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return String.format(
        "CoreServiceRecordKey [serviceClass=%s, instanceName=%s]", serviceClass,
        instanceName);
  }

}
