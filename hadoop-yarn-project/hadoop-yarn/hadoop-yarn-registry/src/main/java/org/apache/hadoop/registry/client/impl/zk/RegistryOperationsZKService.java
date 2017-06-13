package org.apache.hadoop.registry.client.impl.zk;

import static org.apache.hadoop.registry.client.impl.zk.ZookeeperConfigOptions.SCHEME_SASL;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.fs.PathPermissionException;
import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.registry.client.api.records.ApplicationServiceRecordKey;
import org.apache.hadoop.registry.client.api.records.ContainerServiceRecordKey;
import org.apache.hadoop.registry.client.api.records.CoreServiceRecordKey;
import org.apache.hadoop.registry.client.api.records.ServiceRecordKey;
import org.apache.hadoop.registry.client.binding.RegistryPathUtils;
import org.apache.hadoop.registry.client.binding.RegistryTypeUtils;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.registry.client.exceptions.InvalidPathnameException;
import org.apache.hadoop.registry.client.exceptions.InvalidRecordException;
import org.apache.hadoop.registry.client.exceptions.InvalidRegistryKeyException;
import org.apache.hadoop.registry.client.exceptions.NoPathPermissionsException;
import org.apache.hadoop.registry.client.exceptions.NoRecordException;
import org.apache.hadoop.registry.client.types.RegistryPathStatus;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang.NotImplementedException;
import org.apache.curator.utils.ZKPaths;

import com.google.common.base.Preconditions;

public class RegistryOperationsZKService extends CuratorService
    implements RegistryOperations {
  private static final Logger LOG =
      LoggerFactory.getLogger(RegistryOperationsZKService.class);

  private final RegistryUtils.ServiceRecordMarshal serviceRecordMarshal =
      new RegistryUtils.ServiceRecordMarshal();

  public RegistryOperationsZKService(String name) {
    this(name, null);
  }

  public RegistryOperationsZKService() {
    this("RegistryOperationsZKService");
  }

  public RegistryOperationsZKService(String name,
      RegistryBindingSource bindingSource) {
    super(name, bindingSource);
  }

  /**
   * Get the aggregate set of ACLs the client should use to create directories
   * 
   * @return the ACL list
   */
  public List<ACL> getClientAcls() {
    return getRegistrySecurity().getClientACLs();
  }

  /**
   * Validate a path
   * 
   * @param path path to validate
   * @throws InvalidPathnameException if a path is considered invalid
   */
  protected void validatePath(String path) throws InvalidPathnameException {
    // currently no checks are performed
  }

  @Override
  public void register(ServiceRecordKey key, ServiceRecord record)
      throws InvalidRecordException, InvalidRegistryKeyException, IOException {
    String path = RegistryUtils.getPathForServiceRecordKey(key);
    Preconditions.checkArgument(record != null, "null record");
    validatePath(path);
    // validate the record before putting it
    RegistryTypeUtils.validateServiceRecord(path, record);
    LOG.info("Bound at {} : {}", path, record);

    CreateMode mode = CreateMode.PERSISTENT;
    byte[] bytes = serviceRecordMarshal.toBytes(record);

    // overwrite by default
    // FIXME maybe overwrite setting can be part of key
    zkMkParentPath(path, getClientAcls());
    zkMkPath(path, mode, true, getClientAcls());
    zkSet(path, mode, bytes, getClientAcls(), true);
  }

  @Override
  public ServiceRecord resolve(ServiceRecordKey key)
      throws NoRecordException, IOException, InvalidRegistryKeyException {
    String path = RegistryUtils.getPathForServiceRecordKey(key);
    byte[] bytes;
    try {
      bytes = zkRead(path);
    } catch (Exception e) {
      throw new NoRecordException(path, e.getMessage());
    }
    ServiceRecord record =
        serviceRecordMarshal.fromBytes(path, bytes, ServiceRecord.RECORD_TYPE);
    RegistryTypeUtils.validateServiceRecord(path, record);
    return record;
  }

  @Override
  public void deregister(ServiceRecordKey key)
      throws NoRecordException, IOException, InvalidRegistryKeyException {
    String path = RegistryUtils.getPathForServiceRecordKey(key);
    validatePath(path);
    if (!zkPathExists(path)) {
      throw new NoRecordException(path, "Path to delete does not exist.");
    }
    zkDelete(path, true, null);

  }

  @Private
  public boolean exists(String path) throws IOException {
    validatePath(path);
    return zkPathExists(path);
  }

  @Private
  public RegistryPathStatus stat(String path) throws IOException {
    validatePath(path);
    Stat stat = zkStat(path);

    String name = RegistryPathUtils.lastPathEntry(path);
    RegistryPathStatus status = new RegistryPathStatus(name, stat.getCtime(),
        stat.getDataLength(), stat.getNumChildren());
    if (LOG.isDebugEnabled()) {
      LOG.debug("Stat {} => {}", path, status);
    }
    return status;
  }

  @Private
  public List<String> list(String path) throws IOException {

    return zkList(path);
  }

  @Override
  public boolean mknode(String string, boolean createPatrents)
      throws PathPermissionException, NoPathPermissionsException,
      NotImplementedException {
    throw new NotImplementedException();
  }

  @Override
  public void delete(String path, boolean recursive)
      throws PathPermissionException, NoPathPermissionsException,
      NotImplementedException {
    throw new NotImplementedException();

  }

}
