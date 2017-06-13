package org.apache.hadoop.registry.client.impl;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathPermissionException;
import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.registry.client.api.records.ServiceRecordKey;
import org.apache.hadoop.registry.client.binding.RegistryTypeUtils;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.registry.client.exceptions.InvalidRegistryKeyException;
import org.apache.hadoop.registry.client.exceptions.NoPathPermissionsException;
import org.apache.hadoop.registry.client.exceptions.NoRecordException;
import org.apache.hadoop.registry.client.types.RegistryPathStatus;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegistryOperationsStoreService extends AbstractService
    implements RegistryOperations {

  private FileSystem fs;
  private static final Logger LOG =
      LoggerFactory.getLogger(RegistryOperationsStoreService.class);
  private final RegistryUtils.ServiceRecordMarshal serviceRecordMarshal =
      new RegistryUtils.ServiceRecordMarshal();

  public RegistryOperationsStoreService(String name) {
    super(name);
  }

  /**
   * Create a unique path to store this key
   * 
   * @throws InvalidRegistryKeyException
   */
  private Path getPathForServiceRecordKey(ServiceRecordKey key)
      throws InvalidRegistryKeyException {
    if (!key.validate()) {
      throw new InvalidRegistryKeyException(key.toString());
    }

    // If everything goes in the same dir, just name it for the hashcode
    // FIXME maybe should use a better human-readable path...
    // FIXME make base path configurable
    return new Path(
        Paths.get("registry", Integer.toString(key.hashCode())).toUri());
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    try {
      this.fs = FileSystem.get(conf);
      LOG.info("Initialized Yarn-registry with Filesystem "
          + fs.getClass().getCanonicalName());
    } catch (IOException e) {
      LOG.error("Failed to get FileSystem for registry", e);
      throw new YarnRuntimeException(e);
    }
    super.serviceInit(conf);
  }

  @Override
  public void register(ServiceRecordKey key, ServiceRecord record)
      throws IOException, InvalidRegistryKeyException {
    Path path = getPathForServiceRecordKey(key);

    FSDataOutputStream stream = fs.create(path);
    byte[] bytes = serviceRecordMarshal.toBytes(record);
    stream.write(bytes);
    stream.close();
    LOG.info("Bound record to path " + path);

  }

  @Override
  public ServiceRecord resolve(ServiceRecordKey key)
      throws IOException, InvalidRegistryKeyException {
    Path path = getPathForServiceRecordKey(key);
    Long size = (long) 0;
    try {
      size = fs.getFileStatus(path).getLen();
    } catch (FileNotFoundException e) {
      throw new NoRecordException(path.toString(), e.getMessage());
    }

    byte[] bytes = new byte[size.intValue()];

    FSDataInputStream instream = fs.open(path);
    instream.read(bytes);
    instream.close();

    // FIXME this only uses the path for logging purposes, see if it can be
    // removed...
    ServiceRecord record =
        serviceRecordMarshal.fromBytes(path.toString(), bytes);
    RegistryTypeUtils.validateServiceRecord(path.toString(), record);
    return record;

  }

  @Override
  public void deregister(ServiceRecordKey key)
      throws IOException, InvalidRegistryKeyException {
    Path path = getPathForServiceRecordKey(key);
    if (!fs.exists(path)) {
      throw new NoRecordException(path.toString(),
          "Path to delete does not exist.");
    }
    fs.delete(path, false);
  }

  @Override
  public RegistryPathStatus stat(String path) {
    throw new NotImplementedException();
  }

  @Override
  public boolean mknode(String string, boolean createPatrents)
      throws PathPermissionException, NoPathPermissionsException {
    throw new NotImplementedException();
  }

  @Override
  public void delete(String path, boolean recursive)
      throws PathPermissionException, NoPathPermissionsException {
    throw new NotImplementedException();

  }

  @Override
  public List<String> list(String path) {
    throw new NotImplementedException();
  }
}
