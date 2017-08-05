package org.apache.hadoop.registry.api;

public interface ServiceRecordKey {

  public boolean validate();

  public String toString();

  @Override
  public boolean equals(Object obj);

  @Override
  public int hashCode();

}
