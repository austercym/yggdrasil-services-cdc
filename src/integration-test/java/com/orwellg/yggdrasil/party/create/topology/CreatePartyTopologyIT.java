package com.orwellg.yggdrasil.party.create.topology;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import javax.naming.directory.DirContext;

import org.apache.commons.codec.binary.Base64;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.LocalCluster;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.gson.Gson;
import com.orwellg.umbrella.avro.types.event.EntityIdentifierType;
import com.orwellg.umbrella.avro.types.event.Event;
import com.orwellg.umbrella.avro.types.event.EventType;
import com.orwellg.umbrella.avro.types.event.ProcessIdentifierType;
import com.orwellg.umbrella.avro.types.party.PartyIdType;
import com.orwellg.umbrella.avro.types.party.PartyPersonalDetailsType;
import com.orwellg.umbrella.avro.types.party.PartyType;
import com.orwellg.umbrella.avro.types.party.personal.PPEmploymentDetailType;
import com.orwellg.umbrella.avro.types.party.personal.PPEmploymentDetails;
import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfigFactory;
import com.orwellg.umbrella.commons.types.party.Party;
import com.orwellg.umbrella.commons.types.utils.avro.RawMessageUtils;
import com.orwellg.umbrella.commons.utils.constants.Constants;
import com.orwellg.umbrella.commons.utils.enums.PartyEvents;
import com.orwellg.umbrella.commons.utils.uniqueid.UniqueIDGenerator;
import com.orwellg.umbrella.commons.utils.zookeeper.ZooKeeperHelper;
import com.orwellg.yggdrasil.h2.H2DbHelper;
import com.orwellg.yggdrasil.party.bo.PartyBO;
import com.orwellg.yggdrasil.party.config.LdapParams;
import com.orwellg.yggdrasil.party.config.TopologyConfigWithLdap;
import com.orwellg.yggdrasil.party.config.TopologyConfigWithLdapFactory;
import com.orwellg.yggdrasil.party.dao.MariaDbManager;
import com.orwellg.yggdrasil.party.dao.PartyDAO;
import com.orwellg.yggdrasil.party.ldap.LdapUtil;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.waiting.HealthChecks;

public class CreatePartyTopologyIT {

    @Rule
    public DockerComposeRule docker = DockerComposeRule.builder()
            .file("src/integration-test/resources/docker-compose.yml")
            .waitingForService("kafka", HealthChecks.toHaveAllPortsOpen())
            .build();

	protected static final String JDBC_CONN = "jdbc:h2:mem:test;MODE=MySQL;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE";
	private static final String DATABASE_SQL = "/DataModel/MariaDB/mariadb_obs_datamodel.sql";
	private static final String DELIMITER = ";";
    
    public final static Logger LOG = LogManager.getLogger(CreatePartyTopologyIT.class);

	protected PartyDAO partyDAO;
	protected PartyBO partyBO;
	
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
//		client = ZookeeperUtils.getStartedZKClient(config.getZookeeperPath(), config.getZookeeperConnection());
//		client.blockUntilConnected();

		ZooKeeperHelper zk = new ZooKeeperHelper(client);
		
		zk.printAllProps();
		
		// #uniqueid must be set anyway in zookeeper:
		// create /com/orwellg/unique-id-generator/cluster-suffix IPAGO
		String uniqueIdClusterSuffix = "IPAGO";
		zk.setZkProp(UniqueIDGenerator.CLUSTER_SUFFIX_ZNODE, uniqueIdClusterSuffix);
		
		zk.setZkProp("/com/orwellg/yggdrasil/topologies-defaults/yggdrasil.ldap.url", LdapParams.URL_DEFAULT);
		zk.setZkProp("/com/orwellg/yggdrasil/topologies-defaults/yggdrasil.ldap.admin.dn", LdapParams.ADMIN_DN_DEFAULT);
		zk.setZkProp("/com/orwellg/yggdrasil/topologies-defaults/yggdrasil.ldap.admin.pwd", LdapParams.ADMIN_PWD_DEFAULT);
		zk.setZkProp("/com/orwellg/yggdrasil/topologies-defaults/yggdrasil.ldap.usersgroup.dn", LdapParams.USERS_GROUP_DN_DEFAULT);

		zk.setZkProp("/com/orwellg/yggdrasil/mariadb/yggdrassil.mariadb.url", JDBC_CONN);
		zk.setZkProp("/com/orwellg/yggdrasil/mariadb/yggdrassil.mariadb.user", "");
		zk.setZkProp("/com/orwellg/yggdrasil/mariadb/yggdrassil.mariadb.password", "");
		
		zk.printAllProps();
		
		TopologyConfigWithLdapFactory.resetTopologyConfig();
		TopologyConfigWithLdap config = (TopologyConfigWithLdap) TopologyConfigWithLdapFactory.getTopologyConfig();
		assertEquals(zookeeperHost, config.getZookeeperConnection());
		TopologyConfigFactory.resetTopologyConfig();
		TopologyConfigFactory.getTopologyConfig();
		assertEquals(zookeeperHost, TopologyConfigFactory.getTopologyConfig().getZookeeperConnection());
		assertEquals(JDBC_CONN, TopologyConfigFactory.getTopologyConfig().getMariaDBConfig().getMariaDBParams().getUrl());
		
		MariaDbManager mariaDbManager = MariaDbManager.getInstance();
		
		assertEquals(JDBC_CONN, MariaDbManager.getUrl());
		
		partyDAO = new PartyDAO(mariaDbManager.getConnection());
		partyBO = new PartyBO(mariaDbManager.getConnection());

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
		
		TopologyConfigWithLdapFactory.resetTopologyConfig();
		TopologyConfigFactory.resetTopologyConfig();
		assertEquals(CuratorFrameworkState.STOPPED, client.getState());
		
		if (cluster != null) {
			cluster.shutdown();
		}
	}
	
	/**
	 * Load topology in storm and then test it. May take several minutes to finish
	 * (see requestCreatePartyToTopologyAndWaitResponse()).
	 * 
	 * @throws SQLException
	 */
	@Test 
	public void testSetUpAndExecPartyTopology() throws Exception {
		// Set for tests: zookeeper property /com/orwellg/unique-id-generator/cluster-suffix = IPAGO
		TopologyConfigWithLdapFactory.getTopologyConfig();

		// Load topology in local storm cluster
		CreatePartyTopology.loadTopologyInStorm(cluster);

		// When request then get response and created element
		requestCreatePartyToTopologyAndWaitResponse();

		cluster.shutdown();

//		ZookeeperUtils.close();
	}

	/**
	 * Test an already loaded topology by sending a CreateParty event to kafka
	 * topic, and wait for PartyCreated response.<br/>
	 * Pre: party topology already loaded in storm.
	 * 
	 * @throws SQLException
	 */
	protected void requestCreatePartyToTopologyAndWaitResponse() throws Exception {

		// Request CreateParty writing in kafka

		// Generate CreateParty event with Party value to be created
		String parentKey = "PartyTopologyTest";
		String processId = "" + ThreadLocalRandom.current().nextLong(1000000);
		String uuid = UUID.randomUUID().toString();
		String eventKeyId = "EVENT-" + uuid;
		LOG.info("eventKeyId = {}", eventKeyId);

		// Generate CreateParty event with Party as eventData
		String eventName = PartyEvents.CREATE_PARTY.getEventName();
		// Send Party as eventData
		// PartyType partyType = PartyType.newBuilder().build();
		String partyNameToCreate = "PartyChunga" + processId;
		String partyUsername = "PartyChunga" + processId + "@greatparty.co.uk";
		String partyPassword = "mlkw0914!!·$%&";
		PartyType partyType = generatePartyTypeForCreation(partyNameToCreate, partyUsername, partyPassword);
//		String serializedPartyType = RawMessageUtils.encodeToString(PartyType.SCHEMA$, partyType);
		String serializedPartyType = new Gson().toJson(partyType);
		LOG.info("serializedPartyType = {}", serializedPartyType);
		Event event = generateEvent(parentKey, processId, eventKeyId, eventName, serializedPartyType);
		// Serialize event
		String base64Event = Base64.encodeBase64String(RawMessageUtils.encode(Event.SCHEMA$, event).array());

		// Write CreateParty event to request kafka topic.
		TopologyConfigWithLdap config = (TopologyConfigWithLdap) TopologyConfigWithLdapFactory.getTopologyConfig();
		String bootstrapServer = config.getKafkaBootstrapHosts();
		String topic = config.getKafkaSubscriberSpoutConfig().getTopic().getName().get(0);
		Producer<String, String> producer = makeProducer(bootstrapServer);
		LOG.info("Sending event {} to topic {}...", event, topic);
		producer.send(new ProducerRecord<String, String>(topic, base64Event));
		LOG.info("Event {} sent to topic {}.", event, topic);
		producer.close();

		// Check results

		int maxRetries = 120;
		int retries = 0;
		int interval = 1;

		// Check if PartyTopology returns kafka PartyCreated result event
		LOG.info("Checking if Party topology has returned to kafka topic...");
		KafkaConsumer<String, String> consumer = makeConsumer(bootstrapServer);
		String responseTopic = config.getKafkaPublisherBoltConfig().getTopic().getName().get(0);
		consumer.subscribe(Arrays.asList(responseTopic));
		PartyIdType createdPartyID = waitAndConsume(parentKey, processId, eventKeyId, maxRetries, retries, interval,
				consumer);
		Assert.assertNotNull("Result kafka message should be received.", createdPartyID);
		LOG.info("CORRECT Response for key = {}, processId = {}, eventKeyId = {} found, createdPartyID = {}.", parentKey,
				processId, eventKeyId, createdPartyID);
		consumer.close();

		// Check if PartyTopology has processed event (eg: Party created in
		// mariadb)
		LOG.info("Checking if Party topology has inserted Party in database with Id = {} and name = {}...",
				createdPartyID.getId(), partyNameToCreate);
		// Wait until Party inserted in db, or timeout
		// Check every 1 sec, max 60 sec
		Party resParty = waitAndGetById(maxRetries, interval, createdPartyID);
		Assert.assertNotNull(resParty);
		Assert.assertEquals("Created party name in db should be the requested one.", partyNameToCreate,
				resParty.getParty().getFirstName());
		Assert.assertNull("Created party username in db should not be persisted.", resParty.getParty().getUsername());
		Assert.assertNull("Created party password in db should not be persisted.", resParty.getParty().getPassword());
		Assert.assertNotNull(resParty.getParty().getPersonalDetails());
		Assert.assertEquals(partyType.getPersonalDetails().getEmploymentDetails(), resParty.getParty().getPersonalDetails().getEmploymentDetails());
		LOG.info("CORRECT Party with Id = {} found in db.", createdPartyID.getId());

		// Connect to ldap and check if party was created with username.
		// Get (and init if they weren't before) party-storm application-wide params. Tries to connect to zookeeper:
		// LdapUtil specific params
		LdapParams ldapParams = config.getLdapConfig().getLdapParams();
		LdapUtil ldapUtil = new LdapUtil(ldapParams);
		DirContext userById = ldapUtil.getUserById(createdPartyID.getId());
		Assert.assertEquals(String.format("cn=%s,%s", createdPartyID.getId(), LdapParams.USERS_GROUP_DN_DEFAULT),
				userById.getNameInNamespace());
		LOG.info("CORRECT userById found in LDAP = {}", userById.getNameInNamespace());
		// Remove created user in ldap for housekeeping:
		ldapUtil.removeUser(createdPartyID.getId());
	}

	protected Party waitAndGetById(int maxRetries, int interval, PartyIdType createdPartyID)
			throws SQLException {
		int retries;
		retries = 0;
		Party p = null;
		while (p == null && retries < maxRetries) {
			// p = partyDAO.getById(partyIdToCreate);
			p = partyBO.getById(createdPartyID.getId());

			if (p == null) {
				retries++;
				LOG.info("Party with Id = {} not found yet, retry {} after sleeping {}s...", createdPartyID.getId(),
						retries, interval);
				try {
					Thread.sleep(interval * 1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		return p;
	}

	protected PartyIdType waitAndConsume(String key, String processId, String eventKeyId, int maxRetries, int retries,
			int interval, KafkaConsumer<String, String> consumer) {
		PartyIdType createdPartyID = null;
		while (createdPartyID == null && retries < maxRetries) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records) {
				System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
				// Deserialize responseEvent (decodeBase64 + decode avro):
				// KafkaEventGenerator does:
				// new Values(event.getEvent().getKey(),
				// Base64.encodeBase64String(RawMessageUtils.encode(Event.SCHEMA$,
				// event).array()))
				Event responseEvent = RawMessageUtils.decode(Event.SCHEMA$, Base64.decodeBase64(record.value()));
				LOG.info("responseEvent = {}", responseEvent);
				String eventDataStr = responseEvent.getEvent().getData().toString();
				LOG.info("responseEvent data = {}", eventDataStr);
				// Check if eventKeyId of the request matches the parent of the response
				if (eventKeyId.equals(responseEvent.getEvent().getParentKey().toString())) {
//					PartyType pt = RawMessageUtils.decodeFromString(PartyType.SCHEMA$, eventDataStr);
					LOG.info("received serializedPartyType = {}", eventDataStr);
					PartyType pt = new Gson().fromJson(eventDataStr, PartyType.class);
					LOG.info("deserialized PartyType = {}", pt);
					createdPartyID = pt.getId();
				}
			}
			if (createdPartyID == null) {
				retries++;
				LOG.info(
						"Response for key = {}, processId = {}, eventKeyId = {} not found yet, retry {} after sleeping {}s...",
						key, processId, eventKeyId, retries, interval);
				try {
					Thread.sleep(interval * 1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		return createdPartyID;
	}

	protected KafkaConsumer<String, String> makeConsumer(String bootstrapServer) {
		Properties propsC = new Properties();
		propsC.put("bootstrap.servers", bootstrapServer);
		propsC.put("acks", "all");
		propsC.put("retries", 0);
		propsC.put("batch.size", 16384);
		propsC.put("linger.ms", 1);
		propsC.put("buffer.memory", 33554432);
		propsC.put("group.id", "test");
		propsC.put("enable.auto.commit", "true");
		propsC.put("auto.commit.interval.ms", "1000");
		propsC.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		propsC.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(propsC);
		return consumer;
	}

	protected Producer<String, String> makeProducer(String bootstrapServer) {
		// Using kafka-clients library:
		// https://kafka.apache.org/0110/javadoc/index.html?org/apache/kafka/clients/producer/KafkaProducer.html
		Properties propsP = new Properties();
		propsP.put("bootstrap.servers", bootstrapServer);
		// props.put("bootstrap.servers", "172.31.17.121:6667");
		propsP.put("acks", "all");
		propsP.put("retries", 0);
		propsP.put("batch.size", 16384);
		propsP.put("linger.ms", 1);
		propsP.put("buffer.memory", 33554432);
		propsP.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		propsP.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		Producer<String, String> producer = new KafkaProducer<>(propsP);
		return producer;
	}

	/**
	 * Create a PartyType to send to PartyTopology along with a CreateParty event.
	 * 
	 * @param name
	 *            name of the Party to be created.
	 * @return
	 */
	private PartyType generatePartyTypeForCreation(String name, String username, String password) {
		PartyType pt = new PartyType();
		// Do not set ID here, then the topology will create and return it:
		pt.setId(null);
		pt.setFirstName(name);
//		// These fields must be not null per avro schema (see PartyType.avsc):
//		pt.setAddresses(new ArrayList<AddressType>());
//		pt.setIds(new ArrayList<KeyValue>());
//		pt.setChannels(new ArrayList<PartyChannelType>());
//		pt.setContracts(new ArrayList<PartyContractType>());
//		pt.setQuestionaires(new ArrayList<PartyQuestionaireType>());
//		pt.setNotifications(new ArrayList<NotificationPreferencesType>());
		pt.setUsername(username);
		pt.setPassword(password);
		
		PartyPersonalDetailsType persDet = new PartyPersonalDetailsType();
		PPEmploymentDetailType empDet = new PPEmploymentDetailType();
		empDet.setJobTitle("job title");
		persDet.setEmploymentDetails(new PPEmploymentDetails(empDet));
		pt.setPersonalDetails(persDet);
		
		return pt;
	}

	/**
	 * Based on KafkaEventGeneratorBolt.generateEvent(), modified for this test.
	 * 
	 * @param parentKey
	 * @param processId
	 * @param eventKeyId
	 * @param eventName
	 * @param data
	 * @return
	 */
	private Event generateEvent(String parentKey, String processId, String eventKeyId, String eventName, String serializedData) {

		String logPreffix = String.format("[Key: %s][ProcessId: %s]: ", parentKey, processId);

		LOG.info("{} Generating event {} for PartyTopology", logPreffix, eventName);

		// Create the event type
		EventType eventType = new EventType();
		eventType.setName(eventName);
		eventType.setVersion(Constants.getDefaultEventVersion());
		eventType.setParentKey(parentKey);
		eventType.setKey(eventKeyId);
		eventType.setSource(this.getClass().getSimpleName());
		SimpleDateFormat format = new SimpleDateFormat(Constants.getDefaultEventTimestampFormat());
		eventType.setTimestamp(format.format(new Date()));

		eventType.setData(serializedData);

		ProcessIdentifierType processIdentifier = new ProcessIdentifierType();
		processIdentifier.setUuid(processId);

		EntityIdentifierType entityIdentifier = new EntityIdentifierType();
		entityIdentifier.setEntity(Constants.IPAGOO_ENTITY);
		entityIdentifier.setBrand(Constants.IPAGOO_BRAND);

		// Create the corresponden event
		Event event = new Event();
		event.setEvent(eventType);
		event.setProcessIdentifier(processIdentifier);
		event.setEntityIdentifier(entityIdentifier);

		LOG.info("{}Event generated correctly: {}", logPreffix, event);

		return event;
	}

}
