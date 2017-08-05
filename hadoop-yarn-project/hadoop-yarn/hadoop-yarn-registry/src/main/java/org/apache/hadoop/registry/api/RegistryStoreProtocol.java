package org.apache.hadoop.registry.api;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.fs.PathPermissionException;
import org.apache.hadoop.registry.client.exceptions.InvalidRecordException;
import org.apache.hadoop.registry.client.exceptions.InvalidRegistryKeyException;
import org.apache.hadoop.registry.client.exceptions.NoPathPermissionsException;
import org.apache.hadoop.registry.client.exceptions.NoRecordException;
import org.apache.hadoop.registry.client.types.RegistryPathStatus;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.service.Service;

public interface RegistryStoreProtocol extends Service {

	/**
	 * Register the given Service Record into the registry.
	 * 
	 * @throws IOException
	 */
	public void register(ServiceRecordKey key, ServiceRecord record)
			throws InvalidRecordException, InvalidRegistryKeyException, IOException;

	/**
	 * Look up the service record matching the fields specified in the lookup
	 * key. Must be an exact match, or will throw NoRecordException.
	 * 
	 * @throws IOException
	 * @throws InvalidRegistryKeyException
	 */
	public ServiceRecord resolve(ServiceRecordKey key)
			throws NoRecordException, IOException, InvalidRegistryKeyException;

	/**
	 * Delete the service record matching the fields specified in the lookup
	 * key. Must be an exact match, or will throw NoRecordException.
	 * 
	 * @throws IOException
	 * @throws InvalidRegistryKeyException
	 */
	public void deregister(ServiceRecordKey key) throws NoRecordException, IOException, InvalidRegistryKeyException;
}
