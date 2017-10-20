package com.orwellg.yggdrasil.party.create.topology;

import java.util.Arrays;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;

import com.orwellg.onboarding.party.topology.bolts.actions.GenerateUniqueIdBoltParty;
import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfig;
import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfigFactory;
import com.orwellg.umbrella.commons.storm.topology.TopologyFactory;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.EventErrorBolt;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.KafkaEventGeneratorBolt;
import com.orwellg.umbrella.commons.storm.topology.component.spout.KafkaSpout;
import com.orwellg.umbrella.commons.storm.topology.generic.bolt.GBolt;
import com.orwellg.umbrella.commons.storm.topology.generic.bolt.GRichBolt;
import com.orwellg.umbrella.commons.storm.topology.generic.grouping.ShuffleGrouping;
import com.orwellg.umbrella.commons.storm.topology.generic.spout.GSpout;
import com.orwellg.umbrella.commons.storm.wrapper.kafka.KafkaBoltWrapper;
import com.orwellg.umbrella.commons.storm.wrapper.kafka.KafkaSpoutWrapper;
import com.orwellg.umbrella.commons.utils.config.ZookeeperUtils;
import com.orwellg.yggdrasil.party.create.topology.bolts.PartyJoinCreateAndLdapBolt;
import com.orwellg.yggdrasil.party.create.topology.bolts.PartyKafkaEventProcessBolt;

/**
 * Storm topology to process Party actions read from kafka topic. Topology
 * summary:
 * <li>KafkaSpoutWrapper
 * 
 * <li>PartyKafkaEventProcessBolt
 * <li>GenerateUniqueIDBolt
 * <li>CreatePartyBolt
 * <li>PartyFinalProcessBolt
 * 
 * <li>KafkaEventGeneratorBolt
 * <li>KafkaBoltWrapper
 * 
 * @author c.friaszapater
 *
 */
public class CreatePartyTopology {

	public static final String TOPOLOGY_NAME = "create-party";

	public static final String KAFKA_EVENT_PRODUCER_COMPONENT_ID = "kafka-event-producer";
	public static final String KAFKA_EVENT_GENERATOR_COMPONENT_ID = "kafka-event-generator";
	public static final String PARTY_FINAL_PROCESS_COMPONENT_ID = "party-final-process";
	public static final String GENERATE_UNIQUEID_COMPONENT_ID = "generate-uniqueid";
	public static final String CREATE_PARTY_COMPONENT_ID = "create-party";
	public static final String REGISTER_PARTY_LDAP_COMPONENT_ID = "register-party-ldap";
	public static final String JOIN_COMPONENT_ID = "join-party";

	public static final String KAFKA_ERROR_PRODUCER_COMPONENT_ID = "kafka-error-producer";
	public static final String KAFKA_EVENT_ERROR_PROCESS_COMPONENT_ID = "kafka-event-error-process";
	public static final String KAFKA_EVENT_SUCCESS_PROCESS_COMPONENT_ID = "kafka-event-success-process";
	public static final String KAFKA_EVENT_READER_COMPONENT_ID = "kafka-event-reader";

	private final static Logger LOG = LogManager.getLogger(CreatePartyTopology.class);

	/**
	 * Set up Party topology and load it into storm, then keep it up (sleeping) for
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

	/**
	 * Set up Party topology and load into storm.<br/>
	 * It may take some 2min to execute synchronously, then another some 2min to
	 * completely initialize storm asynchronously.<br/>
	 * Pre: kafka+zookeeper servers up in addresses as defined in subscriber.yaml
	 * and publisher-result.yaml.
	 * 
	 * @param localCluster
	 *            null to submit to remote cluster.
	 */
	public static void loadTopologyInStorm(LocalCluster localCluster) throws Exception {
		LOG.info("Creating {} topology...", TOPOLOGY_NAME);

		// Read configuration params from topology.properties and zookeeper
		TopologyConfig config = TopologyConfigFactory.getTopologyConfig();

		// Create the spout that read the events from Kafka
		GSpout kafkaEventReader = new GSpout(KAFKA_EVENT_READER_COMPONENT_ID,
				new KafkaSpoutWrapper(config.getKafkaSubscriberSpoutConfig(), String.class, String.class).getKafkaSpout(), config.getKafkaSpoutHints());

		// Parse the events and we send it to the rest of the topology
		GBolt<?> kafkaEventProcess = new GRichBolt(KAFKA_EVENT_SUCCESS_PROCESS_COMPONENT_ID,
				new PartyKafkaEventProcessBolt(), config.getEventProcessHints());
		kafkaEventProcess
				.addGrouping(new ShuffleGrouping(KAFKA_EVENT_READER_COMPONENT_ID, KafkaSpout.EVENT_SUCCESS_STREAM));

		////////
		// Action bolts
		
		// Obtain Unique ID bolt
		GBolt<?> generateUniqueIDBolt = new GRichBolt(GENERATE_UNIQUEID_COMPONENT_ID, new GenerateUniqueIdBoltParty(config.getZookeeperConnection()), 
				config.getActionBoltHints());
		// Link to the former bolt
		generateUniqueIDBolt.addGrouping(new ShuffleGrouping(KAFKA_EVENT_SUCCESS_PROCESS_COMPONENT_ID));

		//
		// Party action bolts (in parallel after GenerateUniqueIdBolt):

		GBolt<?> partyJoinBolt = new GRichBolt(JOIN_COMPONENT_ID, new PartyJoinCreateAndLdapBolt(JOIN_COMPONENT_ID), 10);
		partyJoinBolt.addGrouping(new ShuffleGrouping(GENERATE_UNIQUEID_COMPONENT_ID));
		
		
		////////
		// Event generator
		GBolt<?> kafkaEventGeneratorBolt = new GRichBolt(KAFKA_EVENT_GENERATOR_COMPONENT_ID, new KafkaEventGeneratorBolt(),
				config.getActionBoltHints());
		kafkaEventGeneratorBolt.addGrouping(new ShuffleGrouping(JOIN_COMPONENT_ID));

		////////
		// Final process bolts
		// Send a event with the result
		KafkaBoltWrapper kafkaPublisherBoltWrapper = new KafkaBoltWrapper(config.getKafkaPublisherBoltConfig(), String.class, String.class);
		GBolt<?> kafkaEventProducer = new GRichBolt(KAFKA_EVENT_PRODUCER_COMPONENT_ID,
				kafkaPublisherBoltWrapper.getKafkaBolt(), config.getEventResponseHints());
		kafkaEventProducer.addGrouping(new ShuffleGrouping(KAFKA_EVENT_GENERATOR_COMPONENT_ID));

		/////////
		// Error processing bolts
		GBolt<?> kafkaEventError = new GRichBolt(KAFKA_EVENT_ERROR_PROCESS_COMPONENT_ID, new EventErrorBolt(), config.getEventErrorHints());
		kafkaEventError
				.addGrouping(new ShuffleGrouping(KAFKA_EVENT_READER_COMPONENT_ID, KafkaSpout.EVENT_ERROR_STREAM));
		// GBolt for send errors of events to kafka
		KafkaBoltWrapper kafkaErrorBoltWrapper = new KafkaBoltWrapper(config.getKafkaPublisherErrorBoltConfig(), String.class, String.class);
		GBolt<?> kafkaErrorProducer = new GRichBolt(KAFKA_ERROR_PRODUCER_COMPONENT_ID,
				kafkaErrorBoltWrapper.getKafkaBolt(), config.getEventErrorHints());
		kafkaErrorProducer.addGrouping(new ShuffleGrouping(KAFKA_EVENT_ERROR_PROCESS_COMPONENT_ID));

		
		// Build the topology
		StormTopology topology = TopologyFactory.generateTopology(kafkaEventReader,
				Arrays.asList(
						new GBolt[] { kafkaEventProcess, kafkaEventError, kafkaErrorProducer, generateUniqueIDBolt,
								partyJoinBolt, kafkaEventGeneratorBolt, kafkaEventProducer }));

		LOG.info("Party Topology created, submitting it to storm...");

		// Create the basic config and upload the topology
		Config conf = new Config();
		conf.setDebug(false);
		conf.setMaxTaskParallelism(30);
		conf.setNumWorkers(4);

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
