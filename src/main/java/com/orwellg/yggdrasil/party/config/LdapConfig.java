package com.orwellg.yggdrasil.party.config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.netflix.config.DynamicPropertyFactory;
import com.netflix.config.DynamicStringProperty;
import com.orwellg.umbrella.commons.beans.config.zookeeper.ZkConfigurationParams;
import com.orwellg.umbrella.commons.utils.config.ZookeeperUtils;

public class LdapConfig extends ZkConfigurationParams {

	private final static Logger LOG = LogManager.getLogger(LdapConfig.class);

	// public final static String DEFAULT_PROPERTIES_FILE =
	// "yggdrasil-scylla.properties";

	public static final String DEFAULT_SUB_BRANCH = "/yggdrasil/ldap";

	public static final String ZK_SUB_BRANCH_KEY = "zookeeper.ldap.config.subbranch";

	// zookeeper property names where the configuration may be stored
	public static final String URL_PROP_NAME = "yggdrasil.ldap.url";
	public static final String ADMIN_DN_ZKPROP_NAME = "yggdrasil.ldap.admin.dn";
	public static final String ADMIN_PWD_ZKPROP_NAME  = "yggdrasil.ldap.admin.pwd";
	public static final String USERS_GROUP_DN_ZKPROP_NAME  = "yggdrasil.ldap.usersgroup.dn";
	
	protected LdapParams ldapParams;

	// public LdapConfig() {
	// this(DEFAULT_PROPERTIES_FILE);
	// }
	
	public LdapConfig(String propertiesFileName) {
		LOG.info("Getting properties of {} : [{}, {}]", propertiesFileName, ZK_SUB_BRANCH_KEY, DEFAULT_SUB_BRANCH);
		super.setPropertiesFile(propertiesFileName);
		super.setApplicationRootConfig(ZK_SUB_BRANCH_KEY, DEFAULT_SUB_BRANCH);
	}

	public LdapParams getLdapParams() {
		return ldapParams;
	}

	@Override
	protected void loadParameters() {

		DynamicPropertyFactory dynamicPropertyFactory = null;
		try {
			dynamicPropertyFactory = ZookeeperUtils.getDynamicPropertyFactory();
		} catch (Exception e) {
			LOG.error("Error when try get the dynamic property factory from Zookeeper. Message: {}", e.getMessage(), e);
		}

		if (dynamicPropertyFactory != null) {
			LOG.debug("Loading parameters from zookeeper if they exist, using defaults if not.");
			DynamicStringProperty url = dynamicPropertyFactory.getStringProperty(URL_PROP_NAME, LdapParams.URL_DEFAULT);
			DynamicStringProperty adminDn = dynamicPropertyFactory.getStringProperty(ADMIN_DN_ZKPROP_NAME, LdapParams.ADMIN_DN_DEFAULT);
			DynamicStringProperty adminPwd = dynamicPropertyFactory.getStringProperty(ADMIN_PWD_ZKPROP_NAME, LdapParams.ADMIN_PWD_DEFAULT);
			DynamicStringProperty usersGroupDn = dynamicPropertyFactory.getStringProperty(USERS_GROUP_DN_ZKPROP_NAME, LdapParams.USERS_GROUP_DN_DEFAULT);
			ldapParams = new LdapParams(url, adminDn, adminPwd, usersGroupDn);

		}
	}
}
