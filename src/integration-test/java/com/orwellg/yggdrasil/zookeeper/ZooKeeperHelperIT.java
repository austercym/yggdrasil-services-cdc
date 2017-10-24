package com.orwellg.yggdrasil.zookeeper;

import static org.junit.Assert.fail;

import java.util.concurrent.TimeoutException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.orwellg.umbrella.commons.utils.uniqueid.UniqueIDGenerator;
import com.orwellg.umbrella.commons.utils.zookeeper.ZooKeeperHelper;

/**
 * Test ZooKeeperHelper with zookeeper (curator-test TestingServer).
 * @author c.friaszapater
 *
 */
public class ZooKeeperHelperIT {

	protected TestingServer zkInstance;

	protected CuratorFramework client;
	
	protected String zookeeperHosts;

	public final static Logger LOG = LogManager.getLogger(ZooKeeperHelperIT.class);
	
	@Before
	public void start() throws TimeoutException, Throwable {
		LOG.info("start");
		// Starts a ZooKeeper server
		// zkInstance.start().result(duration);
		zkInstance = new TestingServer(6969);
		LOG.info("Creating zookeeper with tempDir = {}", zkInstance.getTempDirectory());
//		zkInstance.start();

		zookeeperHosts = zkInstance.getConnectString();
		
		LOG.info("Curator connecting to zookeeper {}...", zookeeperHosts);
		client = CuratorFrameworkFactory.newClient(zookeeperHosts, new ExponentialBackoffRetry(1000, 3));
		client.start();
//		client = ZookeeperUtils.getStartedZKClient(conf.getZookeeperPath(), conf.getZookeeperConnection());
		LOG.info("...Curator connected.");
	}

	@After
	public void stop() throws Exception {
		LOG.info("stop");
		if (client != null) {
			client.close();
		}
		
		// Stops the ZooKeeper instance and also deletes any data files.
		// This makes sure no state is kept between test cases.
		// zkInstance.destroy().ready(duration);
		zkInstance.stop();
		zkInstance.close();
	}
	
	@Test
	public void test() throws Exception {
		LOG.info("test1");
		ZooKeeperHelper zk = new ZooKeeperHelper(client);
	
		String uniqueIdClusterSuffix = "IPAGO";
		zk.setZkProp(UniqueIDGenerator.CLUSTER_SUFFIX_ZNODE, uniqueIdClusterSuffix);
		
		zk.printAllProps();
		
		Assert.assertEquals(uniqueIdClusterSuffix, new String(zk.getZkProp(UniqueIDGenerator.CLUSTER_SUFFIX_ZNODE)));
	}

	@Test
	public void test2() throws Exception {
		LOG.info("test2");
		ZooKeeperHelper zk = new ZooKeeperHelper(client);
	
		zk.printAllProps();

		try {
			zk.getZkProp(UniqueIDGenerator.CLUSTER_SUFFIX_ZNODE);
			fail(String.format("Node %s should not exist, as it is a fresh zookeeper server", UniqueIDGenerator.CLUSTER_SUFFIX_ZNODE));
		} catch (KeeperException.NoNodeException ex) {
			LOG.info("Node {} not found, as expected: {}", UniqueIDGenerator.CLUSTER_SUFFIX_ZNODE, ex.getMessage());
		}
	}
}
