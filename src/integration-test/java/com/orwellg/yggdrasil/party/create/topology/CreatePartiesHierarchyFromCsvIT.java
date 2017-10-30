// TODO Uncomment when NEXG-323 is addressed:
//package com.orwellg.yggdrasil.party.create.topology;
//
//import static org.junit.Assert.assertEquals;
//
//import java.io.IOException;
//import java.io.InputStream;
//import java.io.InputStreamReader;
//import java.io.Reader;
//import java.sql.Connection;
//import java.sql.DriverManager;
//import java.sql.SQLException;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Map;
//import java.util.UUID;
//import java.util.concurrent.ThreadLocalRandom;
//
//import org.apache.commons.codec.binary.Base64;
//import org.apache.curator.framework.CuratorFramework;
//import org.apache.curator.framework.CuratorFrameworkFactory;
//import org.apache.curator.framework.imps.CuratorFrameworkState;
//import org.apache.curator.retry.ExponentialBackoffRetry;
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.apache.kafka.clients.consumer.ConsumerRecords;
//import org.apache.kafka.clients.consumer.KafkaConsumer;
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
//import org.apache.storm.LocalCluster;
//import org.junit.After;
//import org.junit.Assert;
//import org.junit.Before;
//import org.junit.Rule;
//import org.junit.Test;
//
//import com.google.gson.Gson;
//import com.orwellg.umbrella.avro.types.event.Event;
//import com.orwellg.umbrella.avro.types.party.PartyIdType;
//import com.orwellg.umbrella.avro.types.party.PartyType;
//import com.orwellg.umbrella.commons.repositories.h2.H2DbHelper;
//import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfig;
//import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfigFactory;
//import com.orwellg.umbrella.commons.types.party.Party;
//import com.orwellg.umbrella.commons.types.utils.avro.RawMessageUtils;
//import com.orwellg.umbrella.commons.utils.enums.PartyEvents;
//import com.orwellg.umbrella.commons.utils.uniqueid.UniqueIDGenerator;
//import com.orwellg.umbrella.commons.utils.zookeeper.ZooKeeperHelper;
//import com.orwellg.yggdrasil.party.bo.PartyBO;
//import com.orwellg.yggdrasil.party.create.csv.PartyCsvLoader;
//import com.orwellg.yggdrasil.party.dao.MariaDbManager;
//import com.orwellg.yggdrasil.party.dao.PartyDAO;
//import com.palantir.docker.compose.DockerComposeRule;
//import com.palantir.docker.compose.connection.waiting.HealthChecks;
//
//public class CreatePartiesHierarchyFromCsvIT {
//	
//    @Rule
//    public DockerComposeRule docker = DockerComposeRule.builder()
//            .file("src/integration-test/resources/docker-compose.yml")
//            .waitingForService("kafka", HealthChecks.toHaveAllPortsOpen())
//            .build();
//
//	protected static final String JDBC_CONN = "jdbc:h2:mem:test;MODE=MySQL;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE";
//	private static final String DATABASE_SQL = "/DataModel/MariaDB/mariadb_obs_datamodel.sql";
//	private static final String DELIMITER = ";";
//
//    public final static Logger LOG = LogManager.getLogger(CreatePartiesHierarchyFromCsvIT.class);
//
//	protected PartyDAO partyDAO;
//	protected PartyBO partyBO;
//	protected PartyCsvLoader partyCsvLoader;
//
//	protected CuratorFramework client;
//	
//	protected LocalCluster cluster;
//
//	@Before
//	public void setUp() throws Exception {
//		// Start H2 db server in-memory
//		Connection connection = DriverManager.getConnection(JDBC_CONN);
//		
//		// Create schema
//		H2DbHelper h2 = new H2DbHelper();
//		h2.createDbSchema(connection, DATABASE_SQL, DELIMITER);
//		
//		// Set for tests: zookeeper property /com/orwellg/unique-id-generator/cluster-suffix = IPAGO
//		String zookeeperHost = "localhost:2181";
//		client = CuratorFrameworkFactory.newClient(zookeeperHost, new ExponentialBackoffRetry(1000, 3));
//		client.start();
//
//		ZooKeeperHelper zk = new ZooKeeperHelper(client);
//		
//		zk.printAllProps();
//		
//		// #uniqueid must be set anyway in zookeeper:
//		// create /com/orwellg/unique-id-generator/cluster-suffix IPAGO
//		String uniqueIdClusterSuffix = "IPAGO";
//		zk.setZkProp(UniqueIDGenerator.CLUSTER_SUFFIX_ZNODE, uniqueIdClusterSuffix);
//		
//		zk.setZkProp("/com/orwellg/yggdrasil/mariadb/yggdrassil.mariadb.url", JDBC_CONN);
//		zk.setZkProp("/com/orwellg/yggdrasil/mariadb/yggdrassil.mariadb.user", "");
//		zk.setZkProp("/com/orwellg/yggdrasil/mariadb/yggdrassil.mariadb.password", "");
//		
//		zk.printAllProps();
//		
//		TopologyConfigFactory.resetTopologyConfig();
//		TopologyConfig config = (TopologyConfig) TopologyConfigFactory.getTopologyConfig();
//		assertEquals(zookeeperHost, config.getZookeeperConnection());
//		assertEquals(JDBC_CONN, TopologyConfigFactory.getTopologyConfig().getMariaDBConfig().getMariaDBParams().getUrl());
//		
//		MariaDbManager mariaDbManager = MariaDbManager.getInstance();
//		
//		assertEquals(JDBC_CONN, MariaDbManager.getUrl());
//		
//		partyDAO = new PartyDAO(mariaDbManager.getConnection());
//		partyBO = new PartyBO(mariaDbManager.getConnection());
//		partyCsvLoader = new PartyCsvLoader();
//
//		LOG.info("LocalCluster setting up...");
//		cluster = new LocalCluster();
//		LOG.info("...LocalCluster set up.");
//	}
//	
//	@After
//	public void stop() throws Exception {
//		// Close the curator client
//		if (client != null) {
//			client.close();
//		}
//		
//		TopologyConfigFactory.resetTopologyConfig();
//		assertEquals(CuratorFrameworkState.STOPPED, client.getState());
//		
//		if (cluster != null) {
//			cluster.shutdown();
//		}
//	}
//	
//	@Test
//	public void testLoadPartiesCsvToKafkaToMariadb() throws Exception {
//		// Given CSV file
//		// and kafka+zookeeper running
//		// and mariadb running
//		// and deployed topology CreatePartyTopology in local storm cluster
//		// When load CSV 
//		// and send events into topic "party.action.event.1"
//		// Then Parties created in mariadb
//		// and responses in topic "party.result.event.1"
//		
//		// Set for tests: zookeeper property /com/orwellg/unique-id-generator/cluster-suffix = IPAGO
//		TopologyConfig config = TopologyConfigFactory.getTopologyConfig();
//		String bootstrapServer = config.getKafkaBootstrapHosts();
//
//		// Load topology in local storm cluster
//		LOG.info("Loading Party topology...");
//		CreatePartyTopology.loadTopologyInStorm(cluster);
//		LOG.info("...Party topology loaded.");
//
//		// And given subscribed consumer
//		KafkaConsumer<String, String> consumer = partyCsvLoader.makeConsumer(bootstrapServer);
//		String responseTopic = config.getKafkaPublisherBoltConfig().getTopic().getName().get(0);
//		consumer.subscribe(Arrays.asList(responseTopic));
//		
//		int maxRetries = 120;
//		int retries = 0;
//		int interval = 1000;
//		String parentKey = this.getClass().getSimpleName();
//		String eventName = PartyEvents.CREATE_PARTY.getEventName();
//
//		// Send an isolated event and check response
//		{
//			PartyType iso = new PartyType();
//			String processId = "" + ThreadLocalRandom.current().nextLong(1000000);
//			String uuid = UUID.randomUUID().toString();
//			String eventKeyId = "EVENT-" + uuid;
//			LOG.debug("eventKeyId = {}", eventKeyId);
//			iso.setId(new PartyIdType(processId+"ISOLA"));
////			String serializedIso = RawMessageUtils.encodeToString(PartyType.SCHEMA$, iso);
//			String serializedIso = new Gson().toJson(iso);
//			Event isoEvent = partyCsvLoader.generateEvent(parentKey, processId, eventKeyId, eventName, serializedIso);
//			
//			List<Event> isoEvents = new ArrayList<>();
//			isoEvents.add(isoEvent);
//			
//			// Send events to kafka topic "party.action.event.1"
//			partyCsvLoader.sendToKafkaParty(bootstrapServer, isoEvents );
//			
//			LOG.info("***** Waiting response for eventKeyId = {}.", eventKeyId);
//			PartyIdType createdPartyID = waitAndConsume(eventKeyId, maxRetries, retries, interval, consumer);
//
//			Assert.assertNotNull(String.format("Result kafka message should be received for eventKeyId = %s.", eventKeyId), createdPartyID);
//			LOG.info("***** CORRECT Response for eventKeyId = {} found, createdPartyID = {}.", eventKeyId, createdPartyID);
//
//			// Check if party exists in DB
//			Party resP = partyBO.getById(createdPartyID.getId());
//			Assert.assertNotNull(resP);
//		}
//		
//
//		/////////////////////
//		// Load and send events for CSV file with PersonalParties
//		
//		{
//			// Given CSV file with PersonalParties
//			InputStream csvStream = this.getClass().getClassLoader().getResourceAsStream(PartyCsvLoader.CSV_PARTY_PERSONAL_FILENAME);
//			Reader in = new InputStreamReader(csvStream);
//			int expectedElementsNum = 99;
//	
//			loadSendAndWaitResponse(bootstrapServer, consumer, maxRetries, interval, parentKey, eventName, in,
//					expectedElementsNum, PartyEvents.CREATE_PARTY_COMPLETE.getEventName());
//		}
////		consumer.close();
//		
//		/////////////////////
//		// Load and send events for CSV file with NonPersonalParties
//		{
//			// Given CSV file with PersonalParties
//			InputStream csvStream = this.getClass().getClassLoader().getResourceAsStream(PartyCsvLoader.CSV_PARTY_NONPERSONAL_FILENAME);
//			Reader in = new InputStreamReader(csvStream);
//			int expectedElementsNum = 100;
//	
//			loadSendAndWaitResponse(bootstrapServer, consumer, maxRetries, interval, parentKey, eventName, in,
//					expectedElementsNum, PartyEvents.CREATE_PARTY_COMPLETE.getEventName());
//		}
//		
//		// In case we needed multi-topology integration tests -> add topology id as key to TopologyConfigFactory methods?
//		// Anyway, we could just pre-create the needed data to test IPRCsvLoader+IPRTopology, so we don't need multi-topology tests:
////		/////////////////////
////		// Load and send events for CSV file with IPRs
////		
////		LOG.info("Loading CreateIPR topology...");
////		CreateIPRTopology.loadTopologyInStorm(cluster);
////		LOG.info("...CreateIPR topology loaded.");
////		
////		// Load csv and send events to topic "ipr.create.event.1"
////		IPRCsvLoader iprCsvLoader = new IPRCsvLoader();
////		String topic = "ipr.create.event.1";
////		iprCsvLoader.csvToKafka(IPRCsvLoader.CSV_IPR_FILENAME, bootstrapServer, topic);
////		
////		LOG.info("Sleeping for {}s...", 120);
////		Thread.sleep(120*1000);
//
//		LOG.info("END");
//	}
//
//	protected PartyIdType waitAndConsume(String eventKeyId, int maxRetries, int retries,
//			int interval, KafkaConsumer<String, String> consumer) {
//		PartyIdType createdPartyID = null;
//		while (createdPartyID == null && retries < maxRetries) {
//			ConsumerRecords<String, String> records = consumer.poll(100);
//			for (ConsumerRecord<String, String> record : records) {
//				LOG.info("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
//				Event responseEvent = RawMessageUtils.decode(Event.SCHEMA$, Base64.decodeBase64(record.value()));
//				LOG.info("responseEvent = {}", responseEvent);
//				String eventDataStr = responseEvent.getEvent().getData().toString();
//				LOG.info("responseEvent data with parentKey {} = {}", responseEvent.getEvent().getParentKey().toString(), eventDataStr);
//				// Check if eventKeyId of the request matches the parent of the response
//				if (eventKeyId.equals(responseEvent.getEvent().getParentKey().toString()) && responseEvent.getEvent().getName().equals(PartyEvents.CREATE_PARTY_COMPLETE.getEventName())) {
//					LOG.info("responseEvent data  found for eventKeyId {} = {}", eventKeyId, eventDataStr);
////					PartyType pt = RawMessageUtils.decodeFromString(PartyType.SCHEMA$, eventDataStr);
//					PartyType pt = new Gson().fromJson(eventDataStr, PartyType.class);
//					createdPartyID = pt.getId();
//				}
//			}
//			if (createdPartyID == null) {
//				retries++;
//				LOG.info(
//						"Response for eventKeyId = {} not found yet, retry {} after sleeping {}ms...",
//						 eventKeyId, retries, interval);
//				try {
//					Thread.sleep(interval);
//				} catch (InterruptedException e) {
//					e.printStackTrace();
//				}
//			}
//		}
//		return createdPartyID;
//	}
//
//	public void loadSendAndWaitResponse(String bootstrapServer, KafkaConsumer<String, String> consumer,
//			int maxRetries, int interval, String parentKey, String eventName, Reader in,
//			int expectedElementsNum, String eventToExpect) throws IOException, SQLException {
//		// Load CSV
//		List<PartyType> parties = partyCsvLoader.loadCsvParty(in);
//
//		Assert.assertEquals(expectedElementsNum, parties.size());
//		
//		// Generate events
//		List<Event> events = partyCsvLoader.generateEventList(parentKey, eventName, parties);
//		
//		Assert.assertEquals(expectedElementsNum, events.size());
//		
//		// Send events to kafka topic "party.action.event.1"
//		partyCsvLoader.sendToKafkaParty(bootstrapServer, events);
//		
//		
//		// Wait for result responses to all events
//		// Check if PartyTopology returns kafka PartyCreated result event
//		LOG.info("Checking response to all events...");
//		Map<String, Event> matchedResponseEvents = partyCsvLoader.waitAndConsumeAllResponses(events, maxRetries, interval, consumer, eventToExpect);
////			Assert.assertNotNull(String.format("Result kafka message should be received for eventKeyId = %s.", eventKeyId), createdPartyID);
//		
//		Assert.assertEquals(String.format("There should be %s events matched, matchedResponseEvents = {}", events.size(), matchedResponseEvents),
//				events.size(), matchedResponseEvents.size());
//
//		for (Iterator<Event> i2 = matchedResponseEvents.values().iterator(); i2.hasNext();) {
//			Event responseEvent = i2.next();
////			PartyType pt = RawMessageUtils.decodeFromString(PartyType.SCHEMA$, responseEvent.getEvent().getData());
//			PartyType pt = new Gson().fromJson(responseEvent.getEvent().getData(), PartyType.class);
//			String createdPartyID = pt.getId().getId();
//			// Check if party exists in DB
//			Party resP = partyBO.getById(createdPartyID);
//			Assert.assertNotNull(resP);
//			// TODO Assert.assertTrue(resP.getParty().getPersonalDetails() != null || resP.getParty().getNonPersonalDetails());
//		}
//	}
//}
