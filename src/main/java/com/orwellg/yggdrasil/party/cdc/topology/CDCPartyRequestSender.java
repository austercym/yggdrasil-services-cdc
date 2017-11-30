package com.orwellg.yggdrasil.party.cdc.topology;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lable.oss.uniqueid.GeneratorException;

import com.datastax.driver.core.Session;
import com.google.gson.Gson;
import com.orwellg.umbrella.avro.types.cdc.CDCPartyChangeRecord;
import com.orwellg.umbrella.avro.types.cdc.EVENT_TYPES;
import com.orwellg.umbrella.commons.config.params.ScyllaParams;
import com.orwellg.umbrella.commons.repositories.scylla.PartyRepository;
import com.orwellg.umbrella.commons.repositories.scylla.impl.PartyRepositoryImpl;
import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfig;
import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfigFactory;
import com.orwellg.umbrella.commons.types.scylla.entities.Party;
import com.orwellg.umbrella.commons.utils.scylla.ScyllaManager;
import com.orwellg.umbrella.commons.utils.uniqueid.UniqueIDGenerator;
import com.orwellg.umbrella.commons.utils.uniqueid.UniqueIDGeneratorLocal;

public class CDCPartyRequestSender {

	protected PartyRepository partyDao;
	protected UniqueIDGenerator idGen;
	protected Gson gson = new Gson();
    
    public final static Logger LOG = LogManager.getLogger(CDCPartyRequestSender.class);

    public CDCPartyRequestSender(Session ses) {
		partyDao = new PartyRepositoryImpl(ses);
		idGen = new UniqueIDGeneratorLocal();
	}

	/**
	 * Test an already loaded topology by sending a Create event to kafka
	 * topic, and wait for Created response.<br/>
	 * Pre: topology already loaded in storm.
	 */
	public void requestManyCreateToTopologyAndWaitResponse(int numElements) throws Exception {
		TopologyConfig config = TopologyConfigFactory.getTopologyConfig();
		String bootstrapServer = config.getKafkaBootstrapHosts();

		// Given producer
		String requestTopic = config.getKafkaSubscriberSpoutConfig().getTopic().getName().get(0);
		Producer<String, String> producer = makeProducer(bootstrapServer);
		
		// Generate ChangeRecords
		List<CDCPartyChangeRecord> events = generateChangeRecordElements(numElements);
		
		// Send ChangeRecords
		sendRequestEventsToKafka(events, producer, requestTopic);
		
		// Check all elements inserted in DB
		int maxRetries = 120;
		int interval = 1000;
		List<Party> processedElements = waitAndGetInDbAllElements(events, maxRetries, interval);
		if (events.size() != processedElements.size()) {
			throw new RuntimeException("All elements should be inserted in scylla DB.");
		}
	}

	/**
	 * Test an already loaded topology by sending a Create event to kafka
	 * topic, and wait for Created response.<br/>
	 * Pre: topology already loaded in storm.
	 */
	public void requestToTopologyAndCauseError() throws Exception {
		TopologyConfig config = TopologyConfigFactory.getTopologyConfig();
		String bootstrapServer = config.getKafkaBootstrapHosts();

		// Given producer
		String requestTopic = config.getKafkaSubscriberSpoutConfig().getTopic().getName().get(0);
		Producer<String, String> producer = makeProducer(bootstrapServer);
		
		// Send ChangeRecords
		sendRequestEventToKafkaToCauseError(producer, requestTopic);
	}

	public static void main(String[] args) throws Exception {
		ScyllaParams scyllaParams = TopologyConfigFactory.getTopologyConfig().getScyllaConfig().getScyllaParams();
		CDCPartyRequestSender rs = new CDCPartyRequestSender(ScyllaManager.getInstance(scyllaParams.getNodeList()).getSession(scyllaParams.getKeyspace()));
		// Test cause error
		rs.requestToTopologyAndCauseError();
		// And correct events
		rs.requestManyCreateToTopologyAndWaitResponse(Integer.valueOf(args[0]));
	}

	private void sendRequestEventsToKafka(List<CDCPartyChangeRecord> events, Producer<String, String> producer, String topic) {
		for (Iterator<CDCPartyChangeRecord> iterator = events.iterator(); iterator.hasNext();) {
			CDCPartyChangeRecord changeRecord = iterator.next();
			String json = gson.toJson(changeRecord);

			// Write CreateParty event to kafka topic.
			LOG.debug("Sending changeRecord {} to topic {}...", json, topic);
			producer.send(new ProducerRecord<String, String>(topic, json));
			LOG.debug("ChangeRecord {} sent to topic {}.", json, topic);
		}
		LOG.info("{} events sent to topic {}.", events.size(), topic);
	}

	private void sendRequestEventToKafkaToCauseError(Producer<String, String> producer, String topic) {
		String json = "bad json to cause error";

		// Write event to kafka topic.
		LOG.debug("Sending changeRecord {} to topic {}...", json, topic);
		producer.send(new ProducerRecord<String, String>(topic, json));
		LOG.debug("ChangeRecord {} sent to topic {}.", json, topic);
		LOG.info("{} events sent to topic {}.", 1, topic);
	}
	
	protected List<CDCPartyChangeRecord> generateChangeRecordElements(int numElements) throws IOException, GeneratorException {
		ArrayList<CDCPartyChangeRecord> l = new ArrayList<>(numElements);
		for (int i = 0; i < numElements; i++) {
			String id = idGen.generateLocalUniqueIDStr();

			CDCPartyChangeRecord element = generateChangeRecordElement(i, id);
			
			l.add(element);
		}
		return l;
	}

	private CDCPartyChangeRecord generateChangeRecordElement(int eventNumber, String id) {
		CDCPartyChangeRecord element = new CDCPartyChangeRecord();
		
		element.setSequence(eventNumber);
		element.setEventNumber(eventNumber);
		element.setTimestamp((int) System.currentTimeMillis());
		element.setEventType(EVENT_TYPES.insert);
		
		element.setPartyID(id);
		element.setFirstName("PartyByCDCRequestSender");
		return element;
	}

	private Producer<String, String> makeProducer(String bootstrapServer) {
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

	protected Party waitAndGetById(int maxRetries, int interval, String elementID)
			throws SQLException {
		int retries;
		retries = 0;
		com.orwellg.umbrella.commons.types.scylla.entities.Party p = null;
		while (p == null && retries < maxRetries) {
			p = partyDao.getParty(elementID);

			if (p == null) {
				retries++;
				LOG.info("Element with Id = {} not found yet, retry {} after sleeping {}s...", elementID,
						retries, interval);
				try {
					Thread.sleep(interval);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		return p;
	}

	protected List<Party> waitAndGetInDbAllElements(List<CDCPartyChangeRecord> originEvents, int maxRetries, int interval) throws SQLException {
		List<String> ids = new ArrayList<>();
		for (Iterator<CDCPartyChangeRecord> iterator = originEvents.iterator(); iterator.hasNext();) {
			CDCPartyChangeRecord originEvent = iterator.next();
			ids.add(originEvent.getPartyID());
		}
		
		// Get from db until all found or maxRetries
		int retries = 0;
		List<Party> elementsFound = new ArrayList<>();
		for (Iterator<String> iterator = ids.iterator(); iterator.hasNext();) {
			String id = (String) iterator.next();
			
			LOG.info("getting from DB id = {}, elementsFound = {} of {}...", id, elementsFound.size(), ids.size());
			Party e = null;
			while (e == null && retries < maxRetries) {
				e = waitAndGetById(maxRetries, interval, id);

				if (e == null) {
					retries++;
					LOG.info("element with id = {} not found, retry {}...", id, retries);
					try {
						Thread.sleep(interval);
					} catch (InterruptedException ex) {
						ex.printStackTrace();
					}
				} else {
					elementsFound.add(e);
				}
			}
		}
		
		return elementsFound;
	}
}
