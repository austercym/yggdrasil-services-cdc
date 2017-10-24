package com.orwellg.yggdrasil.party.config;

import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfig;

public class TopologyConfigWithLdap extends TopologyConfig {

	protected LdapConfig ldapConfig;

	public TopologyConfigWithLdap() {
		this(DEFAULT_PROPERTIES_FILE);
	}

	public TopologyConfigWithLdap(String propertiesFile) {
		super(propertiesFile);
		ldapConfig = new LdapConfig(propertiesFile);
	}

	// No need to override this, as there are no properties to be loaded in this class apart from LdapConfig, that are loaded on start().
//	@Override
//	protected void loadParameters() {
//		super.loadParameters();
//	}

	@Override
	public void start() throws Exception {
		super.start();
		ldapConfig.start();
	}

	@Override
	public void close() {
		super.close();
		ldapConfig.close();
	}

	public LdapConfig getLdapConfig() {
		return ldapConfig;
	}

	public void setLdapConfig(LdapConfig ldapConfig) {
		this.ldapConfig = ldapConfig;
	}

}
