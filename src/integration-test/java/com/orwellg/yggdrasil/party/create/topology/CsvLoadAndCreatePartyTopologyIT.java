package com.orwellg.yggdrasil.party.create.topology;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.gson.Gson;
import com.orwellg.umbrella.commons.repositories.h2.H2DbHelper;
import com.orwellg.umbrella.commons.repositories.mariadb.impl.PartyDAO;
import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfig;
import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfigFactory;
import com.orwellg.umbrella.commons.utils.uniqueid.UniqueIDGenerator;
import com.orwellg.umbrella.commons.utils.zookeeper.ZooKeeperHelper;
import com.orwellg.yggdrasil.party.create.csv.PartyCsvLoader;
import com.orwellg.yggdrasil.party.dao.MariaDbManager;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.waiting.HealthChecks;

public class CsvLoadAndCreatePartyTopologyIT {

    @Rule
    public DockerComposeRule docker = DockerComposeRule.builder()
            .file("src/integration-test/resources/docker-compose.yml")
            .waitingForService("kafka", HealthChecks.toHaveAllPortsOpen())
            .build();

	protected static final String JDBC_CONN = "jdbc:h2:mem:test;MODE=MySQL;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE";
	private static final String DATABASE_SQL = "/DataModel/MariaDB/mariadb_obs_datamodel.sql";
	private static final String DELIMITER = ";";

    public final static Logger LOG = LogManager.getLogger(CsvLoadAndCreatePartyTopologyIT.class);

	protected PartyDAO partyDAO;
	protected UniqueIDGenerator idGen;
	protected Gson gson = new Gson();

	protected CuratorFramework client;
	
	protected LocalCluster cluster;

	@Before
	public void setUp() throws Exception {
		// Start H2 db server in-memory
		Connection connection = DriverManager.getConnection(JDBC_CONN);
		
		// Create schema
		H2DbHelper h2 = new H2DbHelper();
		h2.createDbSchema(connection, DATABASE_SQL, DELIMITER);
		
		// Set for tests: zookeeper property /com/orwellg/unique-id-generator/cluster-suffix = IPAGO
		String zookeeperHost = "localhost:2181";
		client = CuratorFrameworkFactory.newClient(zookeeperHost, new ExponentialBackoffRetry(1000, 3));
		client.start();

		ZooKeeperHelper zk = new ZooKeeperHelper(client);
		
		zk.printAllProps();
		
		// #uniqueid must be set anyway in zookeeper:
		// create /com/orwellg/unique-id-generator/cluster-suffix IPAGO
		String uniqueIdClusterSuffix = "IPAGO";
		zk.setZkProp(UniqueIDGenerator.CLUSTER_SUFFIX_ZNODE, uniqueIdClusterSuffix);
		
		zk.setZkProp("/com/orwellg/yggdrasil/mariadb/yggdrassil.mariadb.url", JDBC_CONN);
		zk.setZkProp("/com/orwellg/yggdrasil/mariadb/yggdrassil.mariadb.user", "");
		zk.setZkProp("/com/orwellg/yggdrasil/mariadb/yggdrassil.mariadb.password", "");

		// Workaround until we have exactly-once semantics - avoid duplicate executions in test by having just 1 kafka spout:
		zk.setZkProp("/com/orwellg/yggdrasil/topologies-defaults/kafka-spout-hints", "1");
		
		zk.printAllProps();
		
		TopologyConfigFactory.resetTopologyConfig();
		TopologyConfig config = (TopologyConfig) TopologyConfigFactory.getTopologyConfig();
		assertEquals(zookeeperHost, config.getZookeeperConnection());
		TopologyConfigFactory.resetTopologyConfig();
		TopologyConfigFactory.getTopologyConfig();
		assertEquals(zookeeperHost, TopologyConfigFactory.getTopologyConfig().getZookeeperConnection());
		assertEquals(JDBC_CONN, TopologyConfigFactory.getTopologyConfig().getMariaDBConfig().getMariaDBParams().getUrl());
		
		MariaDbManager mariaDbManager = MariaDbManager.getInstance();
		
		assertEquals(JDBC_CONN, MariaDbManager.getUrl());
		
		partyDAO = new PartyDAO(mariaDbManager.getConnection());
		idGen = new UniqueIDGenerator();

		LOG.info("LocalCluster setting up...");
		cluster = new LocalCluster();
		LOG.info("...LocalCluster set up.");
	}
    
	@After
	public void stop() throws Exception {
		// Close the curator client
		if (client != null) {
			client.close();
		}
		
		TopologyConfigFactory.resetTopologyConfig();
		assertEquals(CuratorFrameworkState.STOPPED, client.getState());
		
		if (cluster != null) {
			cluster.shutdown();
		}
	}
	
	/**
	 * Load topology in storm and then test it. May take several minutes to finish
	 * (see requestCreateToTopologyAndWaitResponse()).
	 * 
	 * @throws SQLException
	 */
	@Test 
	public void testLoadCsvAndRequestToTopology() throws Exception {
		LOG.info("CsvLoadAndCreatePartyTopologyIT.testLoadCsvAndRequestToTopology START");
		
		Config conf = new Config();
		conf.setDebug(false);
		TopologyConfig config = TopologyConfigFactory.getTopologyConfig();
		conf.setMaxTaskParallelism(config.getTopologyMaxTaskParallelism());
		conf.setNumWorkers(config.getTopologyNumWorkers());
		conf.setMessageTimeoutSecs(120);
		CreatePartyTopology.loadTopologyInStorm(cluster, conf);

		String bootstrapserver = config.getKafkaBootstrapHosts();
		// Do not match eventName in responses
		String eventToExpect = null;
		PartyCsvLoader csvLoader = new PartyCsvLoader(PartyCsvLoader.CSV_PARTY_PERSONAL_FILENAME, bootstrapserver, eventToExpect);
		csvLoader.loadCsvAndSendToKafka();
		
//		ZookeeperUtils.close();
		LOG.info("CsvLoadAndCreatePartyTopologyIT.testLoadCsvAndRequestToTopology END");
	}

}
