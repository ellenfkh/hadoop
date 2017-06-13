package org.apache.hadoop.registry.client.api.records;

public interface ServiceRecordKey {

  public boolean validate();

  public String toString();

  @Override
  public boolean equals(Object obj);

  @Override
  public int hashCode();

}
