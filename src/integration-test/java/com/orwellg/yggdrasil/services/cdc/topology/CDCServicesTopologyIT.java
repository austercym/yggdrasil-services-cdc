package com.orwellg.yggdrasil.services.cdc.topology;

import static org.junit.Assert.assertEquals;

import java.sql.SQLException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.LocalCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.datastax.driver.core.Session;
import com.orwellg.umbrella.commons.config.params.ScyllaParams;
import com.orwellg.umbrella.commons.repositories.scylla.ScyllaDbHelper;
import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfig;
import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfigFactory;
import com.orwellg.umbrella.commons.utils.scylla.ScyllaManager;
import com.orwellg.umbrella.commons.utils.uniqueid.UniqueIDGenerator;
import com.orwellg.umbrella.commons.utils.zookeeper.ZooKeeperHelper;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.waiting.HealthChecks;

public class CDCServicesTopologyIT {

	protected String scyllaNodes = "localhost:9042";
	protected String scyllaKeyspace = ScyllaParams.DEFAULT_SCYLA_KEYSPACE_CUSTOMER_PRODUCT_DB;

    @Rule
    public DockerComposeRule docker = DockerComposeRule.builder()
            .file("src/integration-test/resources/docker-compose.yml")
            .waitingForService("kafka", HealthChecks.toHaveAllPortsOpen())
            .waitingForService("scylla", HealthChecks.toHaveAllPortsOpen())
            .build();

    public final static Logger LOG = LogManager.getLogger(CDCServicesTopologyIT.class);

	protected CuratorFramework client;
	
	protected LocalCluster cluster;

	@Before
	public void setUp() throws Exception {

		// Set for tests: zookeeper property /com/orwellg/unique-id-generator/cluster-suffix = IPAGO
		String zookeeperHost = "localhost:2181";
		client = CuratorFrameworkFactory.newClient(zookeeperHost, new ExponentialBackoffRetry(1000, 3));
		client.start();
		ZooKeeperHelper zk = new ZooKeeperHelper(client);
		
		zk.printAllProps();
		
		// #uniqueid must be set anyway in zookeeper:
		// create /com/orwellg/unique-id-generator/cluster-suffix IPAGO
		String uniqueIdClusterSuffix = "IPAGO";
		//zk.setZkProp(UniqueIDGenerator.CLUSTER_SUFFIX_ZNODE, uniqueIdClusterSuffix);
		
		// #scylla:
		// create /com/orwellg/yggdrasil/scylla/yggdrassil.scylla.node.list scylla-node1:9042,scylla-node2:9042,scylla-node3:9042
		zk.setZkProp("/com/orwellg/yggdrasil/scylla/yggdrassil.scylla.node.list", scyllaNodes);
	    // create /com/orwellg/yggdrasil/scylla/yggdrassil.scylla.keyspace.customer.product Customer_Product_DB
		zk.setZkProp("/com/orwellg/yggdrasil/scylla/yggdrassil.scylla.keyspace.customer.product", scyllaKeyspace);
		
		zk.printAllProps();
		
		TopologyConfigFactory.resetTopologyConfig();
		TopologyConfig config = (TopologyConfig) TopologyConfigFactory.getTopologyConfig();
		assertEquals(zookeeperHost, config.getZookeeperConnection());
		assertEquals(scyllaNodes, TopologyConfigFactory.getTopologyConfig().getScyllaConfig().getScyllaParams().getNodeList());
		assertEquals(scyllaKeyspace, TopologyConfigFactory.getTopologyConfig().getScyllaConfig().getScyllaParams().getKeyspace());

		// Pause here as sometimes the connection can not be made.
		Thread.sleep(5000);

		Session session = ScyllaManager.getInstance(scyllaNodes).getCluster().connect();
//		Session session = ScyllaManager.getInstance(scyllaNodes).getSession(scyllaKeyspace);
		ScyllaDbHelper scyllaDbHelper = new ScyllaDbHelper(session);
		scyllaDbHelper.createDbSchema("/DataModel/ScyllaDB/scylla_obs_datamodel.cql", ";", ScyllaParams.DEFAULT_SCYLA_KEYSPACE_CUSTOMER_PRODUCT_DB, scyllaKeyspace);
		
		LOG.info("LocalCluster setting up...");
		cluster = new LocalCluster();
		LOG.info("...LocalCluster set up.");
		
		LOG.info("Loading topology in LocalCluster...");
		CDCServicesTopology.loadTopologyInStorm(cluster);
		LOG.info("...topology loaded.");
	}
    
	@After
	public void stop() throws Exception {
		// Don't call the close() methods to avoid guava incompatibility issue:
		// Close the curator client
//		if (client != null) {
//			client.close();
//		}
		
//		TopologyConfigFactory.resetTopologyConfig();
//		assertEquals(CuratorFrameworkState.STOPPED, client.getState());
//		
//		if (cluster != null) {
//			cluster.shutdown();
//		}
	}
	
	/**
	 * Load topology in storm and then test it. May take several minutes to finish.
	 * 
	 * @throws SQLException
	 */
	@Test 
	public void testRequestTopology() throws Exception {
		CDCServicesRequestSender rs = new CDCServicesRequestSender(ScyllaManager.getInstance(scyllaNodes).getSession(scyllaKeyspace));

		// When requests sent to topology
		// Then requests are processed correctly
		rs.requestManyCreateToTopologyAndWaitResponse(1);

		cluster.shutdown();

//		ZookeeperUtils.close();
	}

}
