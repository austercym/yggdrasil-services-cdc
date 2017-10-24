package com.orwellg.yggdrasil.party.config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfig;

/**
 * Topology configuration factory.
 * @author c.friaszapater
 *
 */
public class TopologyConfigWithLdapFactory {

	private final static Logger LOG = LogManager.getLogger(TopologyConfigWithLdapFactory.class);

	protected static TopologyConfigWithLdap topologyConfig;
	
	protected static synchronized void initTopologyConfig(String propertiesFile) {
		
		if (topologyConfig == null) {
			if (propertiesFile != null) {
				LOG.info("Initializing topology with propertiesFile {}", propertiesFile);
				topologyConfig = new TopologyConfigWithLdap(propertiesFile);
			} else {
				LOG.info("Initializing topology with propertiesFile DEFAULT_PROPERTIES_FILE");
				topologyConfig = new TopologyConfigWithLdap();
			}
			try {
				topologyConfig.start();
			} catch (Exception e) {
				LOG.error("Topology configuration params cannot be started. The system will work with default parameters. Message: {}",  e.getMessage(),  e);
			}
		}		
	}
	
	/**
	 * Loads config from propertiesFile and zookeeper.<br/>
	 * Use it for testing or with a topology-specific properties file name.
	 * @param propertiesFile can be null, in which case DEFAULT_PROPERTIES_FILE will be used.
	 * @return TopologyConfig initialized with propertiesFile (a properties file with at least "zookeeper.host" property).
	 * @see TopologyConfig
	 */
	public static synchronized TopologyConfigWithLdap getTopologyConfig(String propertiesFile) {
		initTopologyConfig(propertiesFile);
		return topologyConfig;
	}

	/**
	 * Loads config from TopologyConfig.DEFAULT_PROPERTIES_FILE and zookeeper.<br/>
	 * This is the usual way to instantiate TopologyConfig.
	 * @return TopologyConfig initialized with TopologyConfig.DEFAULT_PROPERTIES_FILE (a properties file with at least "zookeeper.host" property).
	 * @see TopologyConfig
	 */
	public static synchronized TopologyConfigWithLdap getTopologyConfig() {
		initTopologyConfig(null);
		return topologyConfig;
	}
	
	/**
	 * Set topologyConfig ready for a new initialization in getTopologyConfig().<br/>
	 * Useful for testing.
	 */
	public static void resetTopologyConfig() {
		topologyConfig = null;
	}
}
