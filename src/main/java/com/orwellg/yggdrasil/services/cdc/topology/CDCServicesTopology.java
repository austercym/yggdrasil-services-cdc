package com.orwellg.yggdrasil.services.cdc.topology;

import java.util.Arrays;

import com.orwellg.yggdrasil.services.cdc.topology.bolts.CDCServicesByContractIdBolt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;

import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfig;
import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfigFactory;
import com.orwellg.umbrella.commons.storm.topology.TopologyFactory;
import com.orwellg.umbrella.commons.storm.topology.component.spout.KafkaSpout;
import com.orwellg.umbrella.commons.storm.topology.generic.bolt.GBolt;
import com.orwellg.umbrella.commons.storm.topology.generic.bolt.GRichBolt;
import com.orwellg.umbrella.commons.storm.topology.generic.grouping.ShuffleGrouping;
import com.orwellg.umbrella.commons.storm.topology.generic.spout.GSpout;
import com.orwellg.umbrella.commons.storm.wrapper.kafka.KafkaSpoutWrapper;
import com.orwellg.umbrella.commons.utils.config.ZookeeperUtils;
import com.orwellg.yggdrasil.services.cdc.topology.bolts.CDCServicesBolt;
import com.orwellg.yggdrasil.services.cdc.topology.bolts.KafkaChangeRecordProcessBolt;

/**
 * Storm topology to process Services actions read from kafka topic. Topology
 * summary:
 * <li>KafkaSpoutWrapper
 * 
 * <li>ServicesKafkaEventProcessBolt
 * <li>GenerateUniqueIDBolt
 * <li>CreateServicesBolt
 * <li>ServicesFinalProcessBolt
 * 
 * <li>KafkaEventGeneratorBolt
 * <li>KafkaBoltWrapper
 * 
 * @author c.friaszapater
 *
 */
public class CDCServicesTopology {

	private static final String TOPOLOGY_NAME = "yggdrasil-services-cdc";

	private static final String KAFKA_EVENT_READER_COMPONENT_ID = "cdc-services-kafka-event-reader";
	private static final String KAFKA_EVENT_SUCCESS_PROCESS_COMPONENT_ID = "cdc-services-kafka-event-success-process";
	private static final String CDC_SERVICES_COMPONENT_ID = "cdc-services-action";
	private static final String CDC_SERVICES_BY_CONTRACT_ID_COMPONENT_ID = "cdc-services-by-contract-id-action";

	private final static Logger LOG = LogManager.getLogger(CDCServicesTopology.class);

	/**
	 * Set up Services topology and load it into storm, then keep it up (sleeping) for
	 * more than 1h (if it's in LocalCluster).
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		boolean local = false;
		if (args.length >= 1 && args[0].equals("local")) {
			LOG.info("*********** Local parameter received, will work with LocalCluster ************");
			local = true;
		}

		if (local) {
			LOG.info("Creating local cluster...");
			LocalCluster cluster = new LocalCluster();
			LOG.info("...Local cluster created. Loading topology...");
			loadTopologyInStorm(cluster);
			LOG.info("...topology loaded, awaiting kafka messages...");
			Thread.sleep(6000000);
			cluster.shutdown();
			ZookeeperUtils.close();
		} else {
			loadTopologyInStorm();
		}

	}

	public static void loadTopologyInStorm() throws Exception {
		loadTopologyInStorm(null);
	}

	public static void loadTopologyInStorm(LocalCluster localCluster) throws Exception {
		loadTopologyInStorm(localCluster, null);
	}

	/**
	 * Set up Services topology and load into storm.<br/>
	 * It may take some 2min to execute synchronously, then another some 2min to
	 * completely initialize storm asynchronously.<br/>
	 * Pre: kafka+zookeeper servers up in addresses as defined in subscriber.yaml
	 * and publisher-result.yaml.
	 * 
	 * @param localCluster
	 *            null to submit to remote cluster.
	 */
	public static void loadTopologyInStorm(LocalCluster localCluster, Config conf) throws Exception {
		LOG.info("Creating {} topology...", TOPOLOGY_NAME);

		// Read configuration params from topology.properties and zookeeper
		TopologyConfig config = TopologyConfigFactory.getTopologyConfig();
		
		// Create the spout that read the events from Kafka
		Integer kafkaSpoutHints = config.getKafkaSpoutHints();
		LOG.info("kafkaSpoutHints = {}", kafkaSpoutHints);
		GSpout kafkaEventReader = new GSpout(KAFKA_EVENT_READER_COMPONENT_ID,
				new KafkaSpoutWrapper(config.getKafkaSubscriberSpoutConfig(), String.class, String.class).getKafkaSpout(), kafkaSpoutHints);

		// Parse the events and we send it to the rest of the topology
		GBolt<?> kafkaEventProcess = new GRichBolt(KAFKA_EVENT_SUCCESS_PROCESS_COMPONENT_ID,
				new KafkaChangeRecordProcessBolt(), config.getEventProcessHints());
		kafkaEventProcess
				.addGrouping(new ShuffleGrouping(KAFKA_EVENT_READER_COMPONENT_ID, KafkaSpout.EVENT_SUCCESS_STREAM));

		////////
		// Action bolts:

		// CDC bolt
		GBolt<?> actionBolt = new GRichBolt(CDC_SERVICES_COMPONENT_ID, new CDCServicesBolt(), config.getActionBoltHints());
		// Link to the former bolt
		actionBolt.addGrouping(new ShuffleGrouping(KAFKA_EVENT_SUCCESS_PROCESS_COMPONENT_ID));

		// CDC bolt
		GBolt<?> servicesByContractIdActionBolt = new GRichBolt(CDC_SERVICES_BY_CONTRACT_ID_COMPONENT_ID, new CDCServicesByContractIdBolt(), config.getActionBoltHints());
		// Link to the former bolt
		servicesByContractIdActionBolt.addGrouping(new ShuffleGrouping(KAFKA_EVENT_SUCCESS_PROCESS_COMPONENT_ID));

		// Build the topology
		StormTopology topology = TopologyFactory.generateTopology(kafkaEventReader,
				Arrays.asList(
						new GBolt[] { kafkaEventProcess, actionBolt, servicesByContractIdActionBolt }));

		LOG.info("Services Topology created, submitting it to storm...");

		// Create the basic config and upload the topology
		if (conf == null) {
			conf = new Config();
			conf.setDebug(false);
			conf.setMaxTaskParallelism(config.getTopologyMaxTaskParallelism());
			conf.setNumWorkers(config.getTopologyNumWorkers());
		}

		if (localCluster != null) {
			// LocalCluster cluster = new LocalCluster();
			localCluster.submitTopology(TOPOLOGY_NAME, conf, topology);
			LOG.info("{} Topology submitted to storm (LocalCluster).", TOPOLOGY_NAME);
		} else {
			StormSubmitter.submitTopology(TOPOLOGY_NAME, conf, topology);
			LOG.info("{} Topology submitted to storm (StormSubmitter).", TOPOLOGY_NAME);
		}
	}
}
