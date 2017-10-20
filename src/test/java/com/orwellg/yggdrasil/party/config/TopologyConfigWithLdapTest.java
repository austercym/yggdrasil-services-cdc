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

import com.orwellg.umbrella.commons.beans.config.kafka.PublisherKafkaConfiguration;
import com.orwellg.umbrella.commons.beans.config.kafka.SubscriberKafkaConfiguration;
import com.orwellg.umbrella.commons.config.params.MariaDBParams;
import com.orwellg.umbrella.commons.config.params.ScyllaParams;
import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfig;
import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfigFactory;
import com.orwellg.umbrella.commons.utils.uniqueid.UniqueIDGenerator;
import com.orwellg.umbrella.commons.utils.zookeeper.ZooKeeperHelper;

import zookeeperjunit.ZKFactory;
import zookeeperjunit.ZKInstance;

// TODO
public class TopologyConfigWithLdapTest {

    public final static Logger LOG = LogManager.getLogger(TopologyConfigWithLdapTest.class);
	
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

        TopologyConfigFactory.resetTopologyConfig();
	}

	@Test
	public void testLoadConfigParamsUsualCase() throws Exception {
		// Scenario (usual case): load zookeeper connection from properties, some params from zookeeper, and some by default
		
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
		String bootstrapHosts = "hdf-node1:6667,hdf-node4:6667,hdf-node5:6667";
		String uniqueIdClusterSuffix = "IPAGO";
		String scyllaNodes = "scylla-node1:9042,scylla-node2:9042,scylla-node3:9042";
		String scyllaKeyspace = "ipagoo";

		ZooKeeperHelper zk = new ZooKeeperHelper(client);
		
		// #kafkaBootstrapServers:
		// create /com/orwellg/yggdrasil/my-topology/kafka-bootstrap-host hdf-node1:6667,hdf-node4:6667,hdf-node5:6667
		zk.setZkProp("/com/orwellg/yggdrasil/my-topology/kafka-bootstrap-host", bootstrapHosts);
		assertEquals(bootstrapHosts, new String(client.getData().forPath("/com/orwellg/yggdrasil/my-topology/kafka-bootstrap-host")));
		
		// #topology configuration
		Integer numWorkers = 4;
		zk.setZkProp("/com/orwellg/yggdrasil/my-topology/topology-num-workers", numWorkers.toString());
		Integer maxParalelism = 30;
		zk.setZkProp("/com/orwellg/yggdrasil/my-topology/topology-max-task-paralelism", maxParalelism.toString());
		
		// #uniqueid:
		// create /com/orwellg/unique-id-generator/cluster-suffix IPAGO
		zk.setZkProp(UniqueIDGenerator.CLUSTER_SUFFIX_ZNODE, uniqueIdClusterSuffix);
		
		// #mariadb:
		// create /com/orwellg/yggdrasil/mariadb/yggdrassil.mariadb.host 217.182.88.190
		String mariaHost = "217.182.88.190";
		zk.setZkProp("/com/orwellg/yggdrasil/mariadb/yggdrassil.mariadb.host", mariaHost);
		// create /com/orwellg/yggdrasil/mariadb/yggdrassil.mariadb.port 3306
		String mariaPort = "3306";
		zk.setZkProp("/com/orwellg/yggdrasil/mariadb/yggdrassil.mariadb.port", mariaPort);
		// create /com/orwellg/yggdrasil/mariadb/yggdrassil.mariadb.dbname IPAGOO_Customer_DB_TEST
		String mariaDbName = "IPAGOO_Customer_DB_TEST";
		zk.setZkProp("/com/orwellg/yggdrasil/mariadb/yggdrassil.mariadb.dbname", mariaDbName);
		// create /com/orwellg/yggdrasil/mariadb/yggdrassil.mariadb.user root
		String mariaUser = "root";
		zk.setZkProp("/com/orwellg/yggdrasil/mariadb/yggdrassil.mariadb.user", mariaUser);
		// create /com/orwellg/yggdrasil/mariadb/yggdrassil.mariadb.password Tempo.99
		String mariaPwd = "Tempo.99";
		zk.setZkProp("/com/orwellg/yggdrasil/mariadb/yggdrassil.mariadb.password", mariaPwd);
		
		// #scylla:
		// create /com/orwellg/yggdrasil/scylla/yggdrassil.scylla.node.list scylla-node1:9042,scylla-node2:9042,scylla-node3:9042
		zk.setZkProp("/com/orwellg/yggdrasil/scylla/yggdrassil.scylla.node.list", scyllaNodes);
	    // create /com/orwellg/yggdrasil/scylla/yggdrassil.scylla.keyspace ipagoo
		zk.setZkProp("/com/orwellg/yggdrasil/scylla/yggdrassil.scylla.keyspace", scyllaKeyspace);
		
		// When loaded (reads propertiesFile and zookeeper)
		TopologyConfig conf = TopologyConfigFactory.getTopologyConfig(propertiesFile);
		assertEquals(propertiesFile, conf.propertiesFile);

		////////////
		// Then zookeeperHosts in TopologyConfig:
		assertEquals(zookeeperHosts, conf.getZookeeperConnection());
		LOG.info("zookeeperHosts = {}", zookeeperHosts);
		
		////////////
		// And applicationRootPath (set by "zookeeper.topology.config.subbranch=/yggdrasil/my-topology" in topo.properties file):
		assertEquals("zookeeper.topology.config.subbranch", conf.applicationRootPathKey);
		assertEquals("/yggdrasil/my-topology", conf.getApplicationRootPath());
		
		////////////
		// And TOPOLOGY PROPERTIES in TopologyConfig:
		assertEquals(maxParalelism, conf.getTopologyMaxTaskParallelism());
		assertEquals(numWorkers, conf.getTopologyNumWorkers());
		
		////////////
		// And KAFKA PROPERTIES in TopologyConfig:
		
		// kafkaBootstrapServers = value read from zookeeper
		assertEquals(bootstrapHosts, conf.getKafkaBootstrapHosts());
		// KafkaSpoutWrapper (subscriber) properties as kafkaSubscriberSpoutConfig in TopologyConfig:
//			zookeeper:
//			   host: ${zookeeper.host}
//			bootstrap:
//			   host: ${kafka.bootstrap.host}
//			topic:
//			   name: 
//			      - lasttransactionlog.get.event.1
//			   commitInterval: 3         
//			application:
//			   id: get-lasttransactionlog
//			   name: get-lasttransactionlog
//			configuration:
//			   autoOffsetReset: earliest
		SubscriberKafkaConfiguration subsConf = conf.getKafkaSubscriberSpoutConfig();
		assertEquals(zookeeperHosts, subsConf.getZookeeper().getHost());
		assertEquals(bootstrapHosts, subsConf.getBootstrap().getHost());
		// subscriber.topic property in topo.properties
		assertEquals("topology.command.request.1", subsConf.getTopic().getName().get(0));
		// default value. Cannot be set for now in .properties or zookeeper. Can be overriden with yaml
		assertEquals(3, subsConf.getTopic().getCommitInterval().intValue());
		// from topo.properties "application.id"
		assertEquals("command-topology", subsConf.getApplication().getId());
		// from topo.properties "application.id"
		assertEquals("command-topology", subsConf.getApplication().getName());
		// default value. Cannot be set for now in .properties or zookeeper. Can be overriden with yaml
		assertEquals("earliest", subsConf.getConfiguration().getAutoOffsetReset());

		// SubscriberKafkaConfiguration optionally loadable by yaml, overriding only the properties present in the yaml file
		subsConf = conf.getKafkaSubscriberSpoutConfig("subscriber-minimal.yaml");
		assertEquals(zookeeperHosts, subsConf.getZookeeper().getHost());
		assertEquals(bootstrapHosts, subsConf.getBootstrap().getHost());
		// overriden
		assertEquals("readfromyaml.get.event.1", subsConf.getTopic().getName().get(0));
		// overriden
		assertEquals(5, subsConf.getTopic().getCommitInterval().intValue());
		// from topo.properties (not overriden)
		assertEquals("command-topology", subsConf.getApplication().getId());
		// from topo.properties (not overriden)
		assertEquals("command-topology", subsConf.getApplication().getName());
		// overriden
		assertEquals("earliest", subsConf.getConfiguration().getAutoOffsetReset());
		
		// KafkaBoltWrapper (publisher-result) properties as kafkaPublisherResultBoltConfig in TopologyConfig:
//			zookeeper:
//				   host: ${zookeeper.host}
//				bootstrap:
//				   host: ${kafka.bootstrap.host}
//				topic:
//				    name: 
//				       - lasttransactionlog.get.result.1
//				    commitInterval: 3                         
//				application:
//				   id: get-lasttransactionlog
//				   name: get-lasttransactionlog
//				configuration:
//				    retries: 0
//				    acks: 1
		PublisherKafkaConfiguration pubConf = conf.getKafkaPublisherBoltConfig();
		assertEquals(zookeeperHosts, pubConf.getZookeeper().getHost());
		assertEquals(bootstrapHosts, pubConf.getBootstrap().getHost());
		// publisher.topic property in topo.properties
		assertEquals("topology.command.result.1", pubConf.getTopic().getName().get(0));
		// default value. Cannot be set for now in .properties or zookeeper. Can be overriden with yaml
		assertEquals(3, pubConf.getTopic().getCommitInterval().intValue());
		// from topo.properties "application.id"
		assertEquals("command-topology", pubConf.getApplication().getId());
		// from topo.properties "application.id"
		assertEquals("command-topology", pubConf.getApplication().getName());
		// default value. Cannot be set for now in .properties or zookeeper. Can be overriden with yaml
		assertEquals(0, pubConf.getConfiguration().getRetries().intValue());
		// default value. Cannot be set for now in .properties or zookeeper. Can be overriden with yaml
		assertEquals("1", pubConf.getConfiguration().getAcks());

		pubConf = conf.getKafkaPublisherBoltConfig("publisher-minimal.yaml");
		assertEquals(zookeeperHosts, pubConf.getZookeeper().getHost());
		assertEquals(bootstrapHosts, pubConf.getBootstrap().getHost());
		// overriden
		assertEquals("readfromyaml.get.result.1", pubConf.getTopic().getName().get(0));
		// default value. Cannot be set for now in .properties or zookeeper. Can be overriden with yaml
		assertEquals(3, pubConf.getTopic().getCommitInterval().intValue());
		// from topo.properties "application.id"
		assertEquals("command-topology", pubConf.getApplication().getId());
		// from topo.properties "application.id"
		assertEquals("command-topology", pubConf.getApplication().getName());
		// overriden
		assertEquals(2, pubConf.getConfiguration().getRetries().intValue());
		// overriden
		assertEquals("0", pubConf.getConfiguration().getAcks());
		
		// KafkaBoltWrapper (publisher-error) properties as kafkaPublisherErrorBoltConfig in TopologyConfig:
		PublisherKafkaConfiguration pubErrConf = conf.getKafkaPublisherErrorBoltConfig();
		assertEquals(zookeeperHosts, pubErrConf.getZookeeper().getHost());
		assertEquals(bootstrapHosts, pubErrConf.getBootstrap().getHost());
		// error.topic property in topo.properties
		assertEquals("topology.command.error.1", pubErrConf.getTopic().getName().get(0));
		// default value. Cannot be set for now in .properties or zookeeper. Can be overriden with yaml
		assertEquals(3, pubErrConf.getTopic().getCommitInterval().intValue());
		// from topo.properties "application.id"
		assertEquals("command-topology", pubErrConf.getApplication().getId());
		// from topo.properties "application.id"
		assertEquals("command-topology", pubErrConf.getApplication().getName());
		// default value. Cannot be set for now in .properties or zookeeper. Can be overriden with yaml
		assertEquals(0, pubErrConf.getConfiguration().getRetries().intValue());
		// default value. Cannot be set for now in .properties or zookeeper. Can be overriden with yaml
		assertEquals("1", pubErrConf.getConfiguration().getAcks());

		pubErrConf = conf.getKafkaPublisherErrorBoltConfig("publisher-error-minimal.yaml");
		assertEquals(zookeeperHosts, pubErrConf.getZookeeper().getHost());
		assertEquals(bootstrapHosts, pubErrConf.getBootstrap().getHost());
		// overriden
		assertEquals("readfromyaml.get.error.1", pubErrConf.getTopic().getName().get(0));
		// default value. Cannot be set for now in .properties or zookeeper. Can be overriden with yaml
		assertEquals(3, pubErrConf.getTopic().getCommitInterval().intValue());
		// from topo.properties "application.id"
		assertEquals("command-topology", pubErrConf.getApplication().getId());
		// from topo.properties "application.id"
		assertEquals("command-topology", pubErrConf.getApplication().getName());
		// overriden
		assertEquals(2, pubErrConf.getConfiguration().getRetries().intValue());
		// overriden
		assertEquals("0", pubErrConf.getConfiguration().getAcks());
		
		////////////
		// And UNIQUEID PROPERTIES in UniqueIdGenerator:
		UniqueIDGenerator idGen = new UniqueIDGenerator(conf.getZookeeperConnection());
		assertEquals(zookeeperHosts, idGen.getZookeeperAddress());
		assertEquals(uniqueIdClusterSuffix, idGen.getClusterSuffixFromZk());
		
		////////////
		// And MARIADB PROPERTIES in TopologyConfig.MariaDBConfig:
		assertEquals(zookeeperHosts, conf.getMariaDBConfig().getZookeeperConnection());
		assertEquals(mariaHost, conf.getMariaDBConfig().getMariaDBParams().getHost());
		assertEquals(mariaPort, conf.getMariaDBConfig().getMariaDBParams().getPort());
		assertEquals(mariaDbName, conf.getMariaDBConfig().getMariaDBParams().getDbName());
		assertEquals(mariaUser, conf.getMariaDBConfig().getMariaDBParams().getUser());
		assertEquals(mariaPwd, conf.getMariaDBConfig().getMariaDBParams().getPassword());
		
		////////////
		// And SCYLLADB PROPERTIES in TopologyConfig.ScyllaConfig:
		assertEquals(scyllaNodes, conf.getScyllaConfig().getScyllaParams().getNodeList());
		assertEquals(scyllaKeyspace, conf.getScyllaConfig().getScyllaParams().getKeyspace());
		assertEquals(zookeeperHosts, conf.getScyllaConfig().getZookeeperConnection());
	}

	@Test
	public void testLoadConfigParamsDefault() throws Exception {
		// Scenario: load zookeeper connection from properties, find nothing configured in zookeeper, then use defaults for the rest of params
		
		// Given <topo>.properties (eg: get-lasttransactionlog.properties) with:
		// # Zookeeper conection
		// zookeeper.host=...
		String propertiesFile = "topo-minimal.properties";
		
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
		TopologyConfig conf = TopologyConfigFactory.getTopologyConfig(propertiesFile);
		assertEquals(propertiesFile, conf.propertiesFile);

		////////////
		// Then zookeeperHosts in TopologyConfig:
		assertEquals(zookeeperHosts, conf.getZookeeperConnection());
		LOG.info("zookeeperHosts = {}", zookeeperHosts);
		
		////////////
		// And applicationRootPathKey default:
		assertEquals("zookeeper.topology.config.subbranch", conf.applicationRootPathKey);
		assertEquals(TopologyConfig.DEFAULT_SUB_BRANCH, conf.getApplicationRootPath());
		
		////////////
		// And TOPOLOGY PROPERTIES default:
		assertEquals(TopologyConfig.DEFAULT_TOPOLOGY_MAX_TASK_PARALLELISM, conf.getTopologyMaxTaskParallelism());
		assertEquals(TopologyConfig.DEFAULT_TOPOLOGY_NUM_WORKERS, conf.getTopologyNumWorkers());
		
		////////////
		// And KAFKA PROPERTIES in TopologyConfig:
		
		// kafkaBootstrapServers default
		String bootstrapHosts = TopologyConfig.DEFAULT_KAFKA_BOOTSTRAP_HOSTS;
		assertEquals(bootstrapHosts, conf.getKafkaBootstrapHosts());
		// KafkaSpoutWrapper (subscriber) properties as kafkaSubscriberSpoutConfig in TopologyConfig:
//			zookeeper:
//			   host: ${zookeeper.host}
//			bootstrap:
//			   host: ${kafka.bootstrap.host}
//			topic:
//			   name: 
//			      - lasttransactionlog.get.event.1
//			   commitInterval: 3         
//			application:
//			   id: get-lasttransactionlog
//			   name: get-lasttransactionlog
//			configuration:
//			   autoOffsetReset: earliest
		SubscriberKafkaConfiguration subsConf = conf.getKafkaSubscriberSpoutConfig();
		assertEquals(zookeeperHosts, subsConf.getZookeeper().getHost());
		assertEquals(bootstrapHosts, subsConf.getBootstrap().getHost());
		// subscriber.topic property in topo.properties
		assertEquals("topology.command.request.1", subsConf.getTopic().getName().get(0));
		// default value. Cannot be set for now in .properties or zookeeper. Can be overriden with yaml
		assertEquals(3, subsConf.getTopic().getCommitInterval().intValue());
		// from topo.properties "application.id"
		assertEquals("command-topology", subsConf.getApplication().getId());
		// from topo.properties "application.id"
		assertEquals("command-topology", subsConf.getApplication().getName());
		// default value. Cannot be set for now in .properties or zookeeper. Can be overriden with yaml
		assertEquals("earliest", subsConf.getConfiguration().getAutoOffsetReset());

		// SubscriberKafkaConfiguration optionally loadable by yaml, overriding only the properties present in the yaml file
		subsConf = conf.getKafkaSubscriberSpoutConfig("subscriber-minimal.yaml");
		assertEquals(zookeeperHosts, subsConf.getZookeeper().getHost());
		assertEquals(bootstrapHosts, subsConf.getBootstrap().getHost());
		// overriden
		assertEquals("readfromyaml.get.event.1", subsConf.getTopic().getName().get(0));
		// overriden
		assertEquals(5, subsConf.getTopic().getCommitInterval().intValue());
		// from topo.properties (not overriden)
		assertEquals("command-topology", subsConf.getApplication().getId());
		// from topo.properties (not overriden)
		assertEquals("command-topology", subsConf.getApplication().getName());
		// overriden
		assertEquals("earliest", subsConf.getConfiguration().getAutoOffsetReset());
		
		// KafkaBoltWrapper (publisher-result) properties as kafkaPublisherResultBoltConfig in TopologyConfig:
//			zookeeper:
//				   host: ${zookeeper.host}
//				bootstrap:
//				   host: ${kafka.bootstrap.host}
//				topic:
//				    name: 
//				       - lasttransactionlog.get.result.1
//				    commitInterval: 3                         
//				application:
//				   id: get-lasttransactionlog
//				   name: get-lasttransactionlog
//				configuration:
//				    retries: 0
//				    acks: 1
		PublisherKafkaConfiguration pubConf = conf.getKafkaPublisherBoltConfig();
		assertEquals(zookeeperHosts, pubConf.getZookeeper().getHost());
		assertEquals(bootstrapHosts, pubConf.getBootstrap().getHost());
		// publisher.topic property in topo.properties
		assertEquals("topology.command.result.1", pubConf.getTopic().getName().get(0));
		// default value. Cannot be set for now in .properties or zookeeper. Can be overriden with yaml
		assertEquals(3, pubConf.getTopic().getCommitInterval().intValue());
		// from topo.properties "application.id"
		assertEquals("command-topology", pubConf.getApplication().getId());
		// from topo.properties "application.id"
		assertEquals("command-topology", pubConf.getApplication().getName());
		// default value. Cannot be set for now in .properties or zookeeper. Can be overriden with yaml
		assertEquals(0, pubConf.getConfiguration().getRetries().intValue());
		// default value. Cannot be set for now in .properties or zookeeper. Can be overriden with yaml
		assertEquals("1", pubConf.getConfiguration().getAcks());

		pubConf = conf.getKafkaPublisherBoltConfig("publisher-minimal.yaml");
		assertEquals(zookeeperHosts, pubConf.getZookeeper().getHost());
		assertEquals(bootstrapHosts, pubConf.getBootstrap().getHost());
		// overriden
		assertEquals("readfromyaml.get.result.1", pubConf.getTopic().getName().get(0));
		// default value. Cannot be set for now in .properties or zookeeper. Can be overriden with yaml
		assertEquals(3, pubConf.getTopic().getCommitInterval().intValue());
		// from topo.properties "application.id"
		assertEquals("command-topology", pubConf.getApplication().getId());
		// from topo.properties "application.id"
		assertEquals("command-topology", pubConf.getApplication().getName());
		// overriden
		assertEquals(2, pubConf.getConfiguration().getRetries().intValue());
		// overriden
		assertEquals("0", pubConf.getConfiguration().getAcks());
		
		// KafkaBoltWrapper (publisher-error) properties as kafkaPublisherErrorBoltConfig in TopologyConfig:
		PublisherKafkaConfiguration pubErrConf = conf.getKafkaPublisherErrorBoltConfig();
		assertEquals(zookeeperHosts, pubErrConf.getZookeeper().getHost());
		assertEquals(bootstrapHosts, pubErrConf.getBootstrap().getHost());
		// error.topic property in topo.properties
		assertEquals("topology.command.error.1", pubErrConf.getTopic().getName().get(0));
		// default value. Cannot be set for now in .properties or zookeeper. Can be overriden with yaml
		assertEquals(3, pubErrConf.getTopic().getCommitInterval().intValue());
		// from topo.properties "application.id"
		assertEquals("command-topology", pubErrConf.getApplication().getId());
		// from topo.properties "application.id"
		assertEquals("command-topology", pubErrConf.getApplication().getName());
		// default value. Cannot be set for now in .properties or zookeeper. Can be overriden with yaml
		assertEquals(0, pubErrConf.getConfiguration().getRetries().intValue());
		// default value. Cannot be set for now in .properties or zookeeper. Can be overriden with yaml
		assertEquals("1", pubErrConf.getConfiguration().getAcks());

		pubErrConf = conf.getKafkaPublisherErrorBoltConfig("publisher-error-minimal.yaml");
		assertEquals(zookeeperHosts, pubErrConf.getZookeeper().getHost());
		assertEquals(bootstrapHosts, pubErrConf.getBootstrap().getHost());
		// overriden
		assertEquals("readfromyaml.get.error.1", pubErrConf.getTopic().getName().get(0));
		// default value. Cannot be set for now in .properties or zookeeper. Can be overriden with yaml
		assertEquals(3, pubErrConf.getTopic().getCommitInterval().intValue());
		// from topo.properties "application.id"
		assertEquals("command-topology", pubErrConf.getApplication().getId());
		// from topo.properties "application.id"
		assertEquals("command-topology", pubErrConf.getApplication().getName());
		// overriden
		assertEquals(2, pubErrConf.getConfiguration().getRetries().intValue());
		// overriden
		assertEquals("0", pubErrConf.getConfiguration().getAcks());
		
		////////////
		// And UNIQUEID PROPERTIES in UniqueIdGenerator:
		UniqueIDGenerator idGen = new UniqueIDGenerator(conf.getZookeeperConnection());
		assertEquals(zookeeperHosts, idGen.getZookeeperAddress());
		assertEquals(UniqueIDGenerator.CLUSTER_SUFFIX_DEFAULT, idGen.getClusterSuffixFromZk());
		
		////////////
		// And MARIADB PROPERTIES in TopologyConfig.MariaDBConfig by default:
		assertEquals(zookeeperHosts, conf.getMariaDBConfig().getZookeeperConnection());
		assertEquals(MariaDBParams.DEFAULT_MARIADB_HOST, conf.getMariaDBConfig().getMariaDBParams().getHost());
		assertEquals(MariaDBParams.DEFAULT_MARIADB_PORT, conf.getMariaDBConfig().getMariaDBParams().getPort());
		assertEquals(MariaDBParams.DEFAULT_MARIADB_DBNAME, conf.getMariaDBConfig().getMariaDBParams().getDbName());
		assertEquals(MariaDBParams.DEFAULT_MARIADB_USER, conf.getMariaDBConfig().getMariaDBParams().getUser());
		assertEquals(MariaDBParams.DEFAULT_MARIADB_PASSWORD, conf.getMariaDBConfig().getMariaDBParams().getPassword());
		
		////////////
		// And SCYLLADB PROPERTIES in TopologyConfig.ScyllaConfig by default:
		assertEquals(ScyllaParams.DEFAULT_SCYLA_NODE_LIST, conf.getScyllaConfig().getScyllaParams().getNodeList());
		assertEquals(ScyllaParams.DEFAULT_SCYLA_KEYSPACE, conf.getScyllaConfig().getScyllaParams().getKeyspace());
		assertEquals(zookeeperHosts, conf.getScyllaConfig().getZookeeperConnection());
	}

}
