package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;

/**
 * Simple class representing an execution type request.
 */
@XmlRootElement(name = "ExecutionTypeRequest")
@XmlAccessorType(XmlAccessType.FIELD)
public class ExecutionTypeRequestInfo {
  @XmlElement(name = "executionType")
  private String executionType;
  @XmlElement(name = "enforceExecutionType")
  private boolean enforceExecutionType;

  public ExecutionTypeRequestInfo() {
  }

  public ExecutionTypeRequestInfo(ExecutionTypeRequest executionTypeRequest) {
    executionType = executionTypeRequest.getExecutionType().name();
    enforceExecutionType = executionTypeRequest.getEnforceExecutionType();
  }

  public ExecutionType getExecutionType() {
    return ExecutionType.valueOf(executionType);
  }

  public void setExecutionType(ExecutionType executionType) {
    this.executionType = executionType.name();
  }

  public boolean getEnforceExecutionType() {
    return enforceExecutionType;
  }

  public void setEnforceExecutionType(boolean enforceExecutionType) {
    this.enforceExecutionType = enforceExecutionType;
  }
}
