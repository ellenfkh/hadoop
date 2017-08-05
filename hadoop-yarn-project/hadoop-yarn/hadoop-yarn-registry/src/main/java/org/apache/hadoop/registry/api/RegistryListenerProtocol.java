package org.apache.hadoop.registry.api;

public interface RegistryListenerProtocol {
	
	  /**
	   * Registers a listener to path related events.
	   *
	   * @param listener the listener.
	   * @return a handle allowing for the management of the listener.
	   * @throws Exception if registration fails due to error.
	   */
	  public ListenerHandle registerPathListener(final PathListener listener)
	      throws Exception;
	  
	  /**
	   * Create the tree cache that monitors the registry for node addition, update,
	   * and deletion.
	   *
	   * @throws Exception if any issue arises during monitoring.
	   */
	  public void monitorRegistryEntries() throws Exception;

}