package com.orwellg.yggdrasil.party.create.topology;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.codec.binary.Base64;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lable.oss.uniqueid.GeneratorException;

import com.google.gson.Gson;
import com.orwellg.umbrella.avro.types.event.EntityIdentifierType;
import com.orwellg.umbrella.avro.types.event.Event;
import com.orwellg.umbrella.avro.types.event.EventType;
import com.orwellg.umbrella.avro.types.event.ProcessIdentifierType;
import com.orwellg.umbrella.avro.types.party.PartyPersonalDetailsType;
import com.orwellg.umbrella.avro.types.party.PartyType;
import com.orwellg.umbrella.avro.types.party.personal.PPEmploymentDetailType;
import com.orwellg.umbrella.avro.types.party.personal.PPEmploymentDetails;
import com.orwellg.umbrella.commons.repositories.mariadb.impl.PartyDAO;
import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfig;
import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfigFactory;
import com.orwellg.umbrella.commons.types.party.Party;
import com.orwellg.umbrella.commons.types.utils.avro.RawMessageUtils;
import com.orwellg.umbrella.commons.utils.constants.Constants;
import com.orwellg.umbrella.commons.utils.enums.PartyEvents;
import com.orwellg.umbrella.commons.utils.uniqueid.UniqueIDGenerator;
import com.orwellg.yggdrasil.party.config.TopologyConfigWithLdapFactory;
import com.orwellg.yggdrasil.party.dao.MariaDbManager;

public class CreatePartyRequestSender {

	protected PartyDAO partyDao;
	protected UniqueIDGenerator idGen;
	protected Gson gson = new Gson();
    
    public final static Logger LOG = LogManager.getLogger(CreatePartyRequestSender.class);

    public CreatePartyRequestSender(Connection con) {
		partyDao = new PartyDAO(con);
		idGen = new UniqueIDGenerator();
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
		
		// Generate many elements to be created
		List<PartyType> elements = generateElementsToCreate(numElements);
		// Generate many events
		String parentKey = this.getClass().getSimpleName();
		String eventName = PartyEvents.CREATE_PARTY.getEventName();
		List<Event> events = generateRequestEvents(parentKey, eventName, elements);
		
		// Send events
		sendRequestEventsToKafka(events, producer, requestTopic);
		
		boolean failed = false;
		// Wait all responses
		int maxRetries = 120;
		int interval = 1000;
		String eventToExpect;
		if (failIfWrong) {
			eventToExpect = PartyEvents.CREATE_PARTY_COMPLETE.getEventName();
			Map<String, Event> matchedResponseEvents = waitAndConsumeAllResponses(events, maxRetries, interval, consumer,
					eventToExpect);
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
		} else {
			eventToExpect = null;
		}
	
	
		// Check all elements inserted in DB
		List<PartyType> processedElements = waitAndGetInDbAllElements(events, maxRetries, interval);
		if (processedElements.size() != events.size()) {
			failed = true;
		}
		if (!failed) {
			LOG.info("All elements found inserted in DB: {}", events.size());
		}
	}

	public void sendRequestEventsToKafka(List<Event> events, Producer<String, String> producer, String topic) {
		for (Iterator<Event> iterator = events.iterator(); iterator.hasNext();) {
			Event event = iterator.next();
			String base64Event = Base64.encodeBase64String(RawMessageUtils.encode(Event.SCHEMA$, event).array());
			
			// Write CreateParty event to "party.action.event.1" kafka topic.
			LOG.debug("Sending event {} to topic {}...", event, topic);
			producer.send(new ProducerRecord<String, String>(topic, base64Event));
			LOG.debug("Event {} sent to topic {}.", event, topic);
		}
		LOG.info("{} events sent to topic {}.", events.size(), topic);
	}

	protected List<PartyType> generateElementsToCreate(int numElements) throws IOException, GeneratorException {
		ArrayList<PartyType> l = new ArrayList<>(numElements);
		for (int i = 0; i < numElements; i++) {
			PartyType pt = new PartyType();
			// Do not set ID here, then the topology will create and return it:
			pt.setId(null);
			pt.setFirstName("FirstName");
			PartyPersonalDetailsType persDet = new PartyPersonalDetailsType();
			PPEmploymentDetailType empDet = new PPEmploymentDetailType();
			empDet.setJobTitle("job title");
			persDet.setEmploymentDetails(new PPEmploymentDetails(empDet));
			pt.setPersonalDetails(persDet);
			
			l.add(pt);
		}
		return l;
	}

	public KafkaConsumer<String, String> makeConsumer(String bootstrapServer) {
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

	public Producer<String, String> makeProducer(String bootstrapServer) {
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

	protected List<Event> generateRequestEvents(String parentKey, String eventName, List<PartyType> elements) {
		// Generate Events with eventData
		List<Event> events = new ArrayList<>();
		for (Iterator<PartyType> iterator = elements.iterator(); iterator.hasNext();) {
			PartyType t = iterator.next();
			String serializedType = gson.toJson(t);
			String processId = "" + ThreadLocalRandom.current().nextLong(1000000);
			String uuid = t.getId().getId();
			String eventKeyId = "EVENT-" + uuid;
			LOG.debug("eventKeyId = {}", eventKeyId);
			Event event = generateEvent(parentKey, processId, eventKeyId, eventName, serializedType);
			events.add(event);
		}
		LOG.info("{} events generated with eventName = {}.", events.size(), eventName);
		return events;
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
	protected Event generateEvent(String parentKey, String processId, String eventKeyId, String eventName, String serializedData) {

		String logPreffix = String.format("[Key: %s][ProcessId: %s]: ", parentKey, processId);

		LOG.info("{} Generating event {} for Topology", logPreffix, eventName);

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

	public void sendToKafka(String bootstrapServer, List<Event> events) throws SQLException {
		String topic = TopologyConfigWithLdapFactory.getTopologyConfig().getKafkaSubscriberSpoutConfig().getTopic().getName().get(0);
		Producer<String, String> producer = makeProducer(bootstrapServer);
		for (Iterator<Event> iterator = events.iterator(); iterator.hasNext();) {
			Event event = iterator.next();
			String base64Event = Base64.encodeBase64String(RawMessageUtils.encode(Event.SCHEMA$, event).array());
			
			// Write CreateParty event to "party.action.event.1" kafka topic.
			LOG.debug("Sending event {} to topic {}...", event, topic);
			producer.send(new ProducerRecord<String, String>(topic, base64Event));
			LOG.debug("Event {} sent to topic {}.", event, topic);
		}
		LOG.info("{} events sent to topic {}.", events.size(), topic);
		producer.close();
	}

	public Map<String, Event> waitAndConsumeAllResponses(List<Event> originEvents, int maxRetries, int interval,
			KafkaConsumer<String, String> consumer, String eventToExpect) {
		
		// parentKeys are originEvents keys
		Set<String> parentKeys = new HashSet<>();
		for (Iterator<Event> iterator = originEvents.iterator(); iterator.hasNext();) {
			Event originEvent = (Event) iterator.next();
			parentKeys.add(originEvent.getEvent().getKey());
		}
		
		// parentKey, event
		Map<String, Event> parentKeyAndResponseEvents = new HashMap<>();
		
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
				Event responseEvent = RawMessageUtils.decode(Event.SCHEMA$, Base64.decodeBase64(record.value()));
				LOG.info("responseEvent data with parentKey {} found = {}", responseEvent.getEvent().getParentKey(), responseEvent);
				// Check if eventKeyId of the request matches the parent of the response
				boolean parentKeyMatch = parentKeys.contains(responseEvent.getEvent().getParentKey());
				// Check if eventName matches only if eventToExpect is not null
				boolean eventNameMatch;
				if (eventToExpect != null) {
					eventNameMatch = responseEvent.getEvent().getName().equals(eventToExpect);
				} else {
					eventNameMatch = true;
				}
				if (parentKeyMatch && eventNameMatch) {
					parentKeyAndResponseEvents.put(responseEvent.getEvent().getParentKey(), responseEvent);
					LOG.info("responseEvent data with parentKey {} matched with responseEvent = {}.", 
							responseEvent.getEvent().getParentKey(),
							responseEvent.getEvent().getName());
					matchedResponses++;
				} else {
					LOG.info("responseEvent data with parentKey {} NOT matched = {}.", responseEvent.getEvent().getParentKey(), responseEvent);
				}
			}
		}
		
		return parentKeyAndResponseEvents;
	}

	protected Party waitAndGetById(int maxRetries, int interval, String elementID)
			throws SQLException {
		int retries;
		retries = 0;
		Party p = null;
		while (p == null && retries < maxRetries) {
			p = partyDao.getById(elementID);

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
		CreatePartyRequestSender rs = new CreatePartyRequestSender(MariaDbManager.getInstance().getConnection());
		rs.requestManyCreateToTopologyAndWaitResponse(true, Integer.valueOf(args[0]));
	}

	public List<PartyType> waitAndGetInDbAllElements(List<Event> originEvents, int maxRetries, int interval) throws SQLException {
		// parentKeys are originEvents keys
		List<String> ids = new ArrayList<>();
		for (Iterator<Event> iterator = originEvents.iterator(); iterator.hasNext();) {
			Event originEvent = (Event) iterator.next();
			PartyType e = gson.fromJson(originEvent.getEvent().getData(), PartyType.class);
			ids.add(e.getId().getId());
		}
		
		// Get from db until all found or maxRetries
		int retries = 0;
		List<PartyType> elementsFound = new ArrayList<>();
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
					elementsFound.add(e.getParty());
				}
			}
		}
		
		return elementsFound;
	}
}
