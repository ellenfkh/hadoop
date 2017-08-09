package org.apache.hadoop.registry.api;

public class ZKServiceRecordKey implements ServiceRecordKey {

  private String path;

  public ZKServiceRecordKey(String path) {
    this.path = path;
  }

  @Override
  public boolean validate() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public String toString() {
    return this.path;
  }

}
