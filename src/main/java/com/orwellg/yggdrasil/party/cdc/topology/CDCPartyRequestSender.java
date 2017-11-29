package com.orwellg.yggdrasil.party.cdc.topology;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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
	 * 
	 * @throws SQLException
	 */
	public void requestManyCreateToTopologyAndWaitResponse(boolean failIfWrong, int numElements) throws Exception {
		TopologyConfig config = TopologyConfigFactory.getTopologyConfig();
		String bootstrapServer = config.getKafkaBootstrapHosts();

		// Given producer
		String requestTopic = config.getKafkaSubscriberSpoutConfig().getTopic().getName().get(0);
		Producer<String, String> producer = makeProducer(bootstrapServer);
		
		// And given subscribed consumer
		KafkaConsumer<String, String> consumer = makeConsumer(bootstrapServer);
		String responseTopic = config.getKafkaPublisherBoltConfig().getTopic().getName().get(0);
		consumer.subscribe(Arrays.asList(responseTopic));
		
		// Generate ChangeRecords
		List<CDCPartyChangeRecord> events = generateElementsToCreate(numElements);
		
		// Send ChangeRecords
		sendRequestEventsToKafka(events, producer, requestTopic);
		
		boolean failed = false;
		// Wait all responses
		int maxRetries = 120;
		int interval = 1000;
		if (failIfWrong) {
			Map<String, CDCPartyChangeRecord> matchedResponseEvents = waitAndConsumeAllResponses(events, maxRetries, interval, consumer);
			if (events.size() != matchedResponseEvents.size()) {
				String msg = String.format("There should be %s events matched, matchedResponseEvents = %s", 
						events.size(), matchedResponseEvents.size());
				if (failIfWrong) {
					throw new RuntimeException(msg);
				}
				LOG.error(msg);
				failed = true;
			} 
			if (!failed) {
				LOG.info("All responses received: {}", events.size());
			}
		}
	
	
		// Check all elements inserted in DB
		List<Party> processedElements = waitAndGetInDbAllElements(events, maxRetries, interval);
		if (processedElements.size() != events.size()) {
			failed = true;
		}
		if (!failed) {
			LOG.info("All elements found inserted in DB: {}", events.size());
		}
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

	protected List<CDCPartyChangeRecord> generateElementsToCreate(int numElements) throws IOException, GeneratorException {
		ArrayList<CDCPartyChangeRecord> l = new ArrayList<>(numElements);
		for (int i = 0; i < numElements; i++) {
			CDCPartyChangeRecord element = new CDCPartyChangeRecord();
			
			element.setSequence(i);
			element.setEventNumber(i);
			element.setTimestamp((int) System.currentTimeMillis());
			element.setEventType(EVENT_TYPES.insert);
			
			element.setPartyID(idGen.generateLocalUniqueIDStr());
			element.setFirstName("PartyByCDCRequestSender");
			
			l.add(element);
		}
		return l;
	}

	private KafkaConsumer<String, String> makeConsumer(String bootstrapServer) {
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

	protected Map<String, CDCPartyChangeRecord> waitAndConsumeAllResponses(List<CDCPartyChangeRecord> originEvents, int maxRetries, int interval,
			KafkaConsumer<String, String> consumer) {
		
		// parentKeys are originEvents keys
		Set<String> parentKeys = new HashSet<>();
		for (Iterator<CDCPartyChangeRecord> iterator = originEvents.iterator(); iterator.hasNext();) {
			CDCPartyChangeRecord originEvent = iterator.next();
			parentKeys.add(getChangeRecordKey(originEvent));
		}
		
		// parentKey, event
		Map<String, CDCPartyChangeRecord> parentKeyAndResponseEvents = new HashMap<>();
		
		// Consume responses until all matched or maxRetries
		int retries = 0;
		int matchedResponses = 0;
		while (matchedResponses < parentKeys.size() && retries < maxRetries) {
			LOG.info("consumer polling topic {}, matchedResponses = {} of {}...", consumer.subscription(), matchedResponses, parentKeys.size());
			ConsumerRecords<String, String> records = consumer.poll(100);
			if (records.isEmpty()) {
				retries++;
				LOG.info("empty records polled, retry {}...", retries);
				try {
					Thread.sleep(interval);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			for (ConsumerRecord<String, String> record : records) {
				CDCPartyChangeRecord responseEvent = gson.fromJson(record.value(), CDCPartyChangeRecord.class);
				LOG.info("responseEvent data with parentKey {} found = {}", getChangeRecordKey(responseEvent), responseEvent);
				// Check if eventKeyId of the request matches the parent of the response
				boolean parentKeyMatch = parentKeys.contains(getChangeRecordKey(responseEvent));
				if (parentKeyMatch) {
					parentKeyAndResponseEvents.put(getChangeRecordKey(responseEvent), responseEvent);
					LOG.info("responseEvent data with parentKey {} matched with responseEvent = {}.", 
							getChangeRecordKey(responseEvent),
							responseEvent);
					matchedResponses++;
				} else {
					LOG.info("responseEvent data with parentKey {} NOT matched = {}.", getChangeRecordKey(responseEvent), responseEvent);
				}
			}
		}
		
		return parentKeyAndResponseEvents;
	}

	private String getChangeRecordKey(CDCPartyChangeRecord originEvent) {
		return originEvent.getSequence() + "-" + originEvent.getEventNumber() + "-" + originEvent.getTimestamp() + "-" + originEvent.getEventType();
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

	public static void main(String[] args) throws Exception {
		ScyllaParams scyllaParams = TopologyConfigFactory.getTopologyConfig().getScyllaConfig().getScyllaParams();
		CDCPartyRequestSender rs = new CDCPartyRequestSender(ScyllaManager.getInstance(scyllaParams.getNodeList()).getSession(scyllaParams.getKeyspace()));
		rs.requestManyCreateToTopologyAndWaitResponse(true, Integer.valueOf(args[0]));
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
