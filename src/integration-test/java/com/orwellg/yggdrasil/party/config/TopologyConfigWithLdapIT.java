package com.orwellg.yggdrasil.party.config;

import static org.junit.Assert.assertEquals;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfigFactory;
import com.orwellg.umbrella.commons.utils.uniqueid.UniqueIDGenerator;
import com.orwellg.umbrella.commons.utils.zookeeper.ZooKeeperHelper;

import zookeeperjunit.ZKFactory;
import zookeeperjunit.ZKInstance;

public class TopologyConfigWithLdapIT {

    public final static Logger LOG = LogManager.getLogger(TopologyConfigWithLdapIT.class);
	
    private CuratorFramework client;

	protected String zookeeperHosts;

    private static final Duration duration = Duration.ofSeconds(90);
	private final ZKInstance zkInstance = ZKFactory.apply()
//			 .withMaxClientConnections(20)
			 .withPort(6969)
//			 .withRootDir(new File("some-other-path"))
			 .create();	

	@Before
	public void start() throws TimeoutException, Throwable {
		// Starts a ZooKeeper server
		zkInstance.start().result(duration);

		zookeeperHosts = zkInstance.connectString().get();
		
		TopologyConfigFactory.resetTopologyConfig();
		
		LOG.info("Curator connecting to zookeeper {}...", zookeeperHosts);
		client = CuratorFrameworkFactory.newClient(zookeeperHosts, new ExponentialBackoffRetry(1000, 3));
		client.start();
		LOG.info("...Curator connected.");
	}
	
	@After
	public void stop() throws TimeoutException, InterruptedException {
		// Closing the curator client would be nice to avoid some exceptions in the log, but it would raise the guava compatibility issue:
//		client.close();
		
		// Stops the ZooKeeper instance and also deletes any data files.
		// This makes sure no state is kept between test cases.
		zkInstance.destroy().ready(duration);

        TopologyConfigWithLdapFactory.resetTopologyConfig();
	}


	@Test
	public void testLoadConfigParamsZk() throws Exception {
		// Scenario: load zookeeper connection from properties, find nothing configured in zookeeper, then use defaults for the rest of params
		
		// Given <topo>.properties (eg: get-lasttransactionlog.properties) with:
		// # Zookeeper conection
		// zookeeper.host=...
		String propertiesFile = "topo.properties";
		
		// And default zookeeper path constants:
		// ZkConfigurationParams.ZK_BASE_ROOT_PATH = "/com/orwellg"
		// TopologyConfig.DEFAULT_SUB_BRANCH          = "/yggdrasil/topologies-defaults"
		// MariaDBConfig.DEFAULT_SUB_BRANCH      = "/yggdrasil/mariadb"
		// ScyllaConfig.DEFAULT_SUB_BRANCH      = "/yggdrasil/scylla"
		
		// And zookeeper with params set
		ZooKeeperHelper zk = new ZooKeeperHelper(client);
		
		// #uniqueid must be set anyway in zookeeper:
		// create /com/orwellg/unique-id-generator/cluster-suffix IPAGO
		String uniqueIdClusterSuffix = "IPAGO";
		zk.setZkProp(UniqueIDGenerator.CLUSTER_SUFFIX_ZNODE, uniqueIdClusterSuffix);
		
		// ldap properties in zookeeper:
		String url = "ldap://hostname:389";
		zk.setZkProp("/com/orwellg/yggdrasil/topologies-defaults/yggdrasil.ldap.url", url);
		String adminDn = "cn=admin,dc=amazonaws,dc=com";
		zk.setZkProp("/com/orwellg/yggdrasil/topologies-defaults/yggdrasil.ldap.admin.dn", adminDn);
		String adminpwd = "1234";
		zk.setZkProp("/com/orwellg/yggdrasil/topologies-defaults/yggdrasil.ldap.admin.pwd", adminpwd);
		String usersGroupDn = "cn=users,dc=amazonaws,dc=com";
		zk.setZkProp("/com/orwellg/yggdrasil/topologies-defaults/yggdrasil.ldap.usersgroup.dn", usersGroupDn);

		// When loaded (reads propertiesFile and zookeeper)
		TopologyConfigWihLdap conf = TopologyConfigWithLdapFactory.getTopologyConfig(propertiesFile);
		assertEquals(propertiesFile, conf.propertiesFile);

		////////////
		// Then zookeeperHosts in TopologyConfig:
		assertEquals(zookeeperHosts, conf.getZookeeperConnection());
		LOG.info("zookeeperHosts = {}", zookeeperHosts);
		////////////
		// And LDAP PROPERTIES in TopologyConfig.LdapConfig:
		assertEquals(url, conf.getLdapConfig().getLdapParams().getUrl());
		assertEquals(adminDn, conf.getLdapConfig().getLdapParams().getAdminDn());
		assertEquals(adminpwd, conf.getLdapConfig().getLdapParams().getAdminPwd());
		assertEquals(usersGroupDn, conf.getLdapConfig().getLdapParams().getUsersGroupDn());
	}
	
	@Test
	public void testLoadConfigParamsDefault() throws Exception {
		// Scenario: load zookeeper connection from properties, find nothing configured in zookeeper, then use defaults for the rest of params
		
		// Given <topo>.properties (eg: get-lasttransactionlog.properties) with:
		// # Zookeeper conection
		// zookeeper.host=...
		String propertiesFile = "topo.properties";
		
		// And default zookeeper path constants:
		// ZkConfigurationParams.ZK_BASE_ROOT_PATH = "/com/orwellg"
		// TopologyConfig.DEFAULT_SUB_BRANCH          = "/yggdrasil/topologies-defaults"
		// MariaDBConfig.DEFAULT_SUB_BRANCH      = "/yggdrasil/mariadb"
		// ScyllaConfig.DEFAULT_SUB_BRANCH      = "/yggdrasil/scylla"
		
		// And zookeeper with params set
		ZooKeeperHelper zk = new ZooKeeperHelper(client);
		
		// #uniqueid must be set anyway in zookeeper:
		// create /com/orwellg/unique-id-generator/cluster-suffix IPAGO
		String uniqueIdClusterSuffix = "IPAGO";
		zk.setZkProp(UniqueIDGenerator.CLUSTER_SUFFIX_ZNODE, uniqueIdClusterSuffix);

		// When loaded (reads propertiesFile and zookeeper)
		TopologyConfigWihLdap conf = TopologyConfigWithLdapFactory.getTopologyConfig(propertiesFile);
		assertEquals(propertiesFile, conf.propertiesFile);

		////////////
		// Then zookeeperHosts in TopologyConfig:
		assertEquals(zookeeperHosts, conf.getZookeeperConnection());
		LOG.info("zookeeperHosts = {}", zookeeperHosts);
		////////////
		// And LDAP PROPERTIES in TopologyConfig.LdapConfig:
		assertEquals("ldap://ec2-35-176-201-54.eu-west-2.compute.amazonaws.com:389", conf.getLdapConfig().getLdapParams().getUrl());
		assertEquals("cn=admin,dc=ec2-35-176-201-54,dc=eu-west-2,dc=compute,dc=amazonaws,dc=com", conf.getLdapConfig().getLdapParams().getAdminDn());
		assertEquals("Password123$", conf.getLdapConfig().getLdapParams().getAdminPwd());
		assertEquals("ou=Users,dc=ec2-35-176-201-54,dc=eu-west-2,dc=compute,dc=amazonaws,dc=com", conf.getLdapConfig().getLdapParams().getUsersGroupDn());
	}

}
